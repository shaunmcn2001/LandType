import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))
from app.main import app


@pytest.mark.integration
def test_vector_smoke():
    c = TestClient(app)
    r = c.get("/vector", params={"lotplan": "13SP181800"})
    assert r.status_code in (200, 404)
    assert r.headers["content-type"].startswith("application/json")
    data = r.json()
    assert "easements" in data
    assert isinstance(data["easements"], dict)
    assert "features" in data["easements"]
    assert isinstance(data["easements"]["features"], list)
