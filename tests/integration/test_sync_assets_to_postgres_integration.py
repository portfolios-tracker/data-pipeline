"""Integration tests for the deprecated sync_assets_to_postgres no-op DAG."""

import pytest

@pytest.mark.integration
def test_sync_assets_is_no_op_after_supabase_rollout():
    """The bridge DAG should return 0 and avoid external dependencies."""
    from dags.sync_assets_to_postgres import sync_assets

    assert sync_assets() == 0
