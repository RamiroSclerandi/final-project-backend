"""Shared test setup.

Keeps a developer's local `.env` out of the test run. `Settings` reads that
file as a fallback, so a test that removes a variable from the environment
would still see the file's value and quietly pass only on machines without an
`.env`. Every test builds its own environment explicitly instead.
"""

import pytest

from ingest.config import Settings


@pytest.fixture(autouse=True)
def ignore_local_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every `Settings()` in the suite read the environment only."""
    monkeypatch.setitem(Settings.model_config, "env_file", None)
