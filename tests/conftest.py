from datetime import date

import pytest

from pipeline.config import Settings

DAY = date(2026, 9, 1)


@pytest.fixture
def settings(tmp_path):
    return Settings(
        lake=tmp_path / "lake",
        warehouse=tmp_path / "warehouse" / "t.duckdb",
        reports=tmp_path / "reports",
    ).resolved()
