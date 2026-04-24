from data_consumers._auth import QueryStationAuth
from data_consumers.remote_duckdb_wrapper import RemoteDuckDBWrapper


class TestQueryStationAuth:
    def test_missing_api_key_raises(self):
        import os

        original = os.environ.get("QUERYSTATION_API_KEY")
        if "QUERYSTATION_API_KEY" in os.environ:
            del os.environ["QUERYSTATION_API_KEY"]
        try:
            with __import__("pytest").raises(ValueError, match="No API key"):
                QueryStationAuth(api_key="")
        finally:
            if original is not None:
                os.environ["QUERYSTATION_API_KEY"] = original

    def test_explicit_api_key(self):
        auth = QueryStationAuth(api_key="sk_test_123", auth_url="https://example.com")
        assert auth._api_key == "sk_test_123"
        assert auth._auth_url == "https://example.com"

    def test_default_auth_url(self):
        auth = QueryStationAuth(api_key="sk_test_123")
        assert auth._auth_url == "https://auth-dev.querystation.app"

    def test_force_refresh(self):
        auth = QueryStationAuth(api_key="sk_test_123")
        auth._expires_at = 9999999999.0
        auth._token = "old_token"
        auth.force_refresh()
        assert auth._expires_at == 0.0


class TestParseTableRef:
    def test_three_part(self):
        catalog, schema, name = RemoteDuckDBWrapper._parse_table_ref(
            "lake.nyc_ops.service_requests"
        )
        assert catalog == "lake"
        assert schema == "nyc_ops"
        assert name == "service_requests"

    def test_two_part(self):
        catalog, schema, name = RemoteDuckDBWrapper._parse_table_ref("public.users")
        assert catalog == "lake"
        assert schema == "public"
        assert name == "users"

    def test_one_part(self):
        catalog, schema, name = RemoteDuckDBWrapper._parse_table_ref("my_table")
        assert catalog == "lake"
        assert schema == "main"
        assert name == "my_table"
