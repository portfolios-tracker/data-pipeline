import ast
from pathlib import Path

import pytest

BLOCKED_TOP_LEVEL_IMPORT_PREFIXES = (
    "dags.etl_modules.notifications",
    "dags.etl_modules.fetcher",
    "dags.etl_modules.vci_provider",
    "dags.etl_modules.orchestrators",
    "dags.etl_modules.extractors",
)

TARGET_DAGS = (
    "market_data_ratios_weekly.py",
    "market_news_morning.py",
    "ingest_company_intelligence.py",
    "market_data_events_daily.py",
)


def _top_level_import_targets(file_path: Path) -> list[str]:
    module = ast.parse(file_path.read_text(encoding="utf-8"))
    targets: list[str] = []
    for node in module.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                targets.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                targets.append(node.module)
    return targets


@pytest.mark.unit
@pytest.mark.parametrize("dag_file", TARGET_DAGS)
def test_dag_avoids_heavy_top_level_etl_imports(dag_file: str):
    dags_dir = Path(__file__).resolve().parents[2] / "dags"
    targets = _top_level_import_targets(dags_dir / dag_file)
    blocked = [
        target
        for target in targets
        if target.startswith(BLOCKED_TOP_LEVEL_IMPORT_PREFIXES)
    ]
    assert blocked == []
