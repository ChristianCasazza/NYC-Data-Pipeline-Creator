# opendata_framework/dagster/resources/io/landing_io_manager.py
import gzip
import polars as pl
from typing import Any
from collections.abc import Iterator, Generator
from dagster import ConfigurableIOManager, OutputContext, InputContext, MetadataValue
from upath import UPath

class LandingIOManager(ConfigurableIOManager):
    """
    IO Manager for the 'Landing' layer (Sharded CSVs).
    
    1. Writes: Accepts a Generator of (batch_index, bytes_iterator).
       Writes {asset}_{partition}_batch_{i}.csv.gz
    
    2. Reads: Globs all batch files for the partition and reads them into one Polars DF.
    """
    base_path: str

    @property
    def _base_path(self) -> UPath:
        return UPath(self.base_path)

    def _resolve_dir(self, asset_name: str, partition_key: str | None) -> UPath:
        """
        Resolves the directory where files should be stored.
        """
        root = self._base_path / asset_name
        
        if not partition_key:
            return root
        
        # Hive-style partitioning logic
        if "-" in partition_key:
            parts = partition_key.split("-")
            if len(parts) >= 2 and len(parts[0]) == 4 and parts[0].isdigit() and parts[1].isdigit():
                return root / f"year={parts[0]}" / f"month={parts[1]}"
        
        if len(partition_key) == 4 and partition_key.isdigit():
            return root / f"year={partition_key}"

        return root / partition_key

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """
        Writes data to GZIP-compressed CSV shards.
        Expects: Generator[Tuple[int, Iterator[bytes]]]
        """
        partition_key = context.partition_key if context.has_partition_key else None
        asset_name = context.asset_key.path[-1]
        target_dir = self._resolve_dir(asset_name, partition_key)
        
        target_dir.mkdir(parents=True, exist_ok=True)

        # Clear stale shards from prior ingestions to prevent duplicates
        # when page size changes (e.g., 50K→500K reduces shard count).
        stale = list(target_dir.glob("*.csv.gz"))
        if stale:
            for f in stale:
                f.unlink()
            context.log.info(
                f"Cleared {len(stale)} existing CSV shards from {target_dir}"
            )

        # We enforce that the input must be our specific generator format
        if not isinstance(obj, (Generator, Iterator)):
             raise ValueError("LandingIOManager now expects a Generator of (batch_index, stream).")

        total_batches = 0
        total_size = 0
        
        for batch_index, stream in obj:
            # Construct filename: {asset}_{partition}_batch_{i}.csv.gz
            # If unpartitioned, just {asset}_batch_{i}.csv.gz
            p_suffix = f"_{partition_key}" if partition_key else ""
            filename = f"{asset_name}{p_suffix}_batch_{batch_index}.csv.gz"
            path = target_dir / filename
            
            context.log.info(f"Writing batch {batch_index} to {path}")
            
            # Stream write to disk
            with path.open("wb") as f_raw:
                with gzip.open(f_raw, "wb") as f_gz:
                    for chunk in stream:
                        f_gz.write(chunk)
            
            total_batches += 1
            total_size += path.stat().st_size

        metadata: dict[str, Any] = {
            "path": MetadataValue.path(str(target_dir)),
            "batches": total_batches,
            "format": "csv.gz",
            "total_size_mb": round(total_size / (1024 * 1024), 2),
        }
        if stale:
            metadata["cleared_stale_shards"] = len(stale)
        context.add_output_metadata(metadata)

    def get_dir_for_asset(self, asset_name: str, partition_key: str | None = None) -> UPath:
        """Public API: Returns the directory where shards are stored for an asset/partition."""
        return self._resolve_dir(asset_name, partition_key)

    def load_input(self, context: InputContext) -> pl.LazyFrame:
        """
        Globs and reads all CSV shards for the request as a LazyFrame.

        Handles partition mappings (e.g., monthly landing → yearly clean) by
        iterating over all mapped upstream partition keys and collecting files
        from each resolved directory.

        Returns a lazy scan so downstream assets can build a full query plan
        (schema contract + enrichment) and execute in a single streaming pass.
        """
        asset_name = context.asset_key.path[-1]

        # Collect files across all mapped partition keys
        all_files: list[str] = []

        if context.has_asset_partitions:
            for pk in context.asset_partition_keys:
                target_dir = self._resolve_dir(asset_name, pk)
                if not target_dir.exists():
                    continue
                files = list(target_dir.glob("*.csv.gz"))
                all_files.extend(str(f) for f in files)
        else:
            target_dir = self._resolve_dir(asset_name, None)
            if target_dir.exists():
                files = list(target_dir.glob("*.csv.gz"))
                all_files.extend(str(f) for f in files)

        if not all_files:
            dirs_checked = (
                [str(self._resolve_dir(asset_name, pk)) for pk in context.asset_partition_keys]
                if context.has_asset_partitions
                else [str(self._resolve_dir(asset_name, None))]
            )
            context.log.warning(
                f"No .csv.gz files found for {asset_name}. "
                f"Checked {len(dirs_checked)} directories: {dirs_checked[:5]}"
            )
            return pl.LazyFrame()

        context.log.info(f"Loading {len(all_files)} CSV shards for {asset_name} (lazy)")

        try:
            return pl.scan_csv(
                all_files,
                infer_schema_length=0,
                ignore_errors=True
            )
        except Exception as e:
            raise RuntimeError(f"Failed to scan CSVs for {asset_name}: {e}") from e