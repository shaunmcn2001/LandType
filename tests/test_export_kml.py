import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))
from app.main import app


@pytest.mark.integration
def test_export_kml_smoke():
    c = TestClient(app)
    r = c.get("/export_kml", params={"lotplan": "13SP181800"})
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        assert r.headers["content-type"].startswith("application/vnd.google-earth.kml")
