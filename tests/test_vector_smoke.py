import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from shapely.geometry import Polygon, mapping

sys.path.append(str(Path(__file__).resolve().parents[1]))
import app.main as main  # noqa: E402
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


@pytest.mark.integration
def test_vector_includes_water_layers(monkeypatch):
    polygon = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    parcel_fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(polygon),
                "properties": {"lotplan": "1TEST"},
            }
        ],
    }

    monkeypatch.setattr(main, "fetch_parcel_geojson", lambda lp: parcel_fc)
    monkeypatch.setattr(main, "to_shapely_union", lambda fc: polygon)
    monkeypatch.setattr(main, "bbox_3857", lambda geom: (0, 0, 1, 1))

    def fake_prepare(parcel, thematic):
        feats = (thematic or {}).get("features", [])
        out = []
        for feat in feats:
            props = feat.get("properties") or {}
            code = props.get("code", "LT1")
            name = props.get("name", "Test Feature")
            out.append((polygon, code, name, 1.0))
        return out

    monkeypatch.setattr(main, "prepare_clipped_shapes", fake_prepare)

    monkeypatch.setattr(
        main,
        "fetch_landtypes_intersecting_envelope",
        lambda env: {"type": "FeatureCollection", "features": []},
    )

    bore_fc = {
        "type": "FeatureCollection",
        "features": [],
    }
    monkeypatch.setattr(main, "fetch_bores_intersecting_envelope", lambda env: bore_fc)

    monkeypatch.setattr(
        main,
        "fetch_easements_intersecting_envelope",
        lambda env: {"type": "FeatureCollection", "features": []},
    )

    water_fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(polygon),
                "properties": {"code": "W-1", "name": "Water Test"},
            }
        ],
    }

    monkeypatch.setattr(
        main,
        "fetch_water_layers_intersecting_envelope",
        lambda env: [
            {
                "layer_id": 25,
                "layer_title": "Water Layer",
                "source_layer_name": "Water Layer",
                "geometry_type": "esriGeometryPolygon",
                "feature_collection": water_fc,
            }
        ],
    )

    client = TestClient(app)
    response = client.get("/vector", params={"lotplan": "1TEST"})
    assert response.status_code == 200
    data = response.json()
    water = data.get("water", {})
    layers = water.get("layers", [])
    assert layers, "expected water layers in vector response"
    layer_entry = layers[0]
    assert layer_entry["layer_title"] == "Water Layer"
    features = layer_entry.get("features", {}).get("features", [])
    assert features and features[0]["properties"]["name"] == "Water Test"
