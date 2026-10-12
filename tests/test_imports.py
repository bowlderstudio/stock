import importlib

import pytest

MODULES = [
    "stockdata.config",
    "stockdata.db",
    "stockdata.flows",
    "stockdata.models",
    "stockdata.quality",
    "stockdata.trading_calendar",
    "stockdata.sources.euronext",
    "stockdata.sources.yahoo",
    "stockdata.pipelines.corporate_actions",
    "stockdata.pipelines.instruments",
    "stockdata.pipelines.prices",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name):
    importlib.import_module(name)
