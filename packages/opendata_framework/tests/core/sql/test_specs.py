from opendata_framework.core.sql.specs import SqlAssetSpec


class TestSqlAssetSpec:
    def test_creation(self):
        spec = SqlAssetSpec(
            name="my_asset",
            sql="SELECT 1",
            tags={"domain": "transit"},
            declared_deps=["upstream_a"],
            extra_deps=["upstream_b"],
            meta={"description": "test"},
        )
        assert spec.name == "my_asset"
        assert spec.sql == "SELECT 1"
        assert spec.tags == {"domain": "transit"}
        assert spec.declared_deps == ["upstream_a"]
        assert spec.extra_deps == ["upstream_b"]
        assert spec.meta == {"description": "test"}

    def test_frozen(self):
        spec = SqlAssetSpec(
            name="test",
            sql="SELECT 1",
            tags={},
            declared_deps=[],
            extra_deps=[],
            meta={},
        )
        with __import__("pytest").raises(AttributeError):
            spec.name = "changed"

    def test_default_empty_collections(self):
        spec = SqlAssetSpec(
            name="test",
            sql="SELECT 1",
            tags={},
            declared_deps=[],
            extra_deps=[],
            meta={},
        )
        assert len(spec.tags) == 0
        assert len(spec.declared_deps) == 0
