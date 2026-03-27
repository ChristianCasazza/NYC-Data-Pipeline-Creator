# opendata_framework/dagster/resources/io/polars_parquet_io_manager.py
from collections.abc import Iterator

import polars as pl
from dagster import (
    AssetKey,
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
)
from upath import UPath


class PolarsParquetIOManager(ConfigurableIOManager):
    """
    A unified IO Manager for Polars that handles Parquet storage across
    local and cloud backends.

    Features:
    - Universal Pathlib (S3/R2/Local transparently).
    - Schema Evolution (Diagonal Concat on read).
    - Streaming Writes (Iterators).
    - Hive Partitioning Support.
    """

    base_path: str
    extension: str = ".parquet"

    @property
    def _base_path(self) -> UPath:
        return UPath(self.base_path)

    def _resolve_path(self, asset_name: str, partition: str | None = None) -> UPath:
        """
        Resolves the target file path.

        Logic:
        - Unpartitioned: base / asset / asset.parquet
        - Partitioned (Standard): base / asset / year=XXXX / asset_XXXX.parquet
        - Partitioned (Hive): base / asset / year=XXXX / month=MM / asset_XXXX-MM.parquet
        """
        if partition:
            # Construct a filename that includes the asset name for readability
            filename = f"{asset_name}_{partition}{self.extension}"

            # Hive-style partitioning logic (year=YYYY/month=MM)
            if "-" in partition:
                parts = partition.split("-")
                # YYYY-MM
                if len(parts) >= 2 and len(parts[0]) == 4 and len(parts[1]) == 2:
                    return (
                        self._base_path
                        / asset_name
                        / f"year={parts[0]}"
                        / f"month={parts[1]}"
                        / filename
                    )

            # Yearly logic
            if len(partition) == 4 and partition.isdigit():
                return self._base_path / asset_name / f"year={partition}" / filename

            # Default flat partitioning (fallback)
            return self._base_path / asset_name / partition / filename

        # Standard unpartitioned path
        return self._base_path / asset_name / f"{asset_name}{self.extension}"

    def get_path_for_asset(self, asset_key: AssetKey, partition_key: str | None = None) -> UPath:
        """
        Public API: Returns the specific file path for an asset/partition.
        Crucial for 'path.exists()' checks in assets.
        """
        asset_name = asset_key.path[-1]
        if partition_key:
            return self._resolve_path(asset_name, partition_key)

        # If no partition, return the standard single-file path
        # Note: If the asset is actually a directory of files (sharded), checking this
        # specific path might fail, but it's the standard entry point.
        return self._resolve_path(asset_name, None)

    def get_glob_pattern(self, asset_key: AssetKey, *, recursive: bool = True) -> str:
        """
        Generates a glob pattern used for DuckDB parquet_scan or bulk file discovery.
        Always targets the asset root directory.
        """
        root = self._base_path / asset_key.path[-1]
        if recursive:
            return str(root / "**" / f"*{self.extension}")
        return str(root / f"*{self.extension}")

    def handle_output(
        self,
        context: OutputContext,
        obj: pl.DataFrame | pl.LazyFrame | Iterator[pl.DataFrame],
    ) -> None:
        """
        Writes Polars objects to Parquet storage.
        """
        asset_name = context.asset_key.path[-1]
        p_key = context.partition_key if context.has_partition_key else None

        # 1. Handle Streaming Iterator (Batched Writes)
        if isinstance(obj, Iterator):
            # We resolve the standard path, then use its PARENT directory for batches
            standard_path = self._resolve_path(asset_name, p_key)
            target_dir = standard_path.parent
            target_dir.mkdir(parents=True, exist_ok=True)

            rows = 0
            for i, batch_df in enumerate(obj):
                if batch_df.is_empty():
                    continue

                # Filename: {asset}_{partition}_batch_{i}.parquet
                part_suffix = f"_{p_key}" if p_key else ""
                fname = f"{asset_name}{part_suffix}_batch_{i}{self.extension}"

                with (target_dir / fname).open("wb") as f:
                    batch_df.write_parquet(f)
                rows += batch_df.height

            context.add_output_metadata({
                "rows": rows,
                "path": MetadataValue.path(str(target_dir)),
                "write_mode": "streaming_batches",
            })
            return

        # 2. Handle Single DataFrame (Standard Write)
        path = self._resolve_path(asset_name, p_key)

        # Ensure parent exists
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

        context.log.info(f"Writing {type(obj).__name__} to {path}")

        if isinstance(obj, pl.LazyFrame):
            obj.sink_parquet(str(path))
        elif isinstance(obj, pl.DataFrame):
            with path.open("wb") as f:
                obj.write_parquet(f)
        else:
            raise TypeError(f"Expected Polars DataFrame, LazyFrame, or Iterator. Got {type(obj)}")

        context.add_output_metadata({
            "path": MetadataValue.path(str(path)),
            "write_mode": "single_file",
        })

    def load_input(self, context: InputContext) -> pl.DataFrame | pl.LazyFrame:
        """
        Loads Parquet data. Handles schema evolution via diagonal concatenation.
        """
        asset_name = context.asset_key.path[-1]
        is_lazy = (
            context.upstream_output.definition_metadata.get("lazy", True)
            if context.upstream_output
            else True
        )

        paths: list[str] = []

        # 1. Resolve Paths
        if context.has_asset_partitions:
            for pk in context.asset_partition_keys:
                p = self._resolve_path(asset_name, pk)

                if p.exists():
                    paths.append(str(p))
                else:
                    # Fallback: Check if it was written as a directory of batches
                    # e.g. .../year=2025/ (Directory) instead of .../year=2025/asset.parquet (File)
                    p_dir = p.parent
                    if p_dir.exists() and list(p_dir.glob(f"*{self.extension}")):
                        # If the directory exists and contains parquet files, add the glob
                        paths.append(str(p_dir / f"*{self.extension}"))
                    else:
                        context.log.warning(f"Partition file missing: {p}")
        else:
            # Unpartitioned
            p = self._resolve_path(asset_name, None)
            if p.exists():
                paths.append(str(p))
            else:
                # Fallback: Glob root if specific file missing
                root = self._base_path / asset_name
                if root.exists():
                    paths.append(str(root / f"**/*{self.extension}"))

        if not paths:
            raise FileNotFoundError(
                f"No parquet files found for asset: {asset_name}. "
                f"Checked base path: {self._base_path / asset_name}"
            )

        context.log.info(f"Loading {len(paths)} paths for {asset_name} (Lazy={is_lazy})")

        # 2. Load with Schema Evolution Support
        # We create a scan for EACH path and concat diagonally.
        # This allows column A to exist in file 1 but not file 2.
        scans = [pl.scan_parquet(p, hive_partitioning=True) for p in paths]
        combined = pl.concat(scans, how="diagonal")

        return combined if is_lazy else combined.collect()

