import io
import sys
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from shapely.geometry import Point, Polygon, mapping

sys.path.append(str(Path(__file__).resolve().parents[1]))
import app.main as main  # noqa: E402
from app.config import (  # noqa: E402
    BORE_NUMBER_FIELD,
    BORE_STATUS_CODE_FIELD,
    BORE_TYPE_CODE_FIELD,
)


@pytest.mark.integration
def test_export_kmz_includes_bore_icons(monkeypatch):
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

    def fake_prepare_clipped_shapes(parcel, features):
        feats = (features or {}).get("features", [])
        if feats:
            prepared = []
            for feat in feats:
                props = feat.get("properties") or {}
                code = props.get("code", "LT1")
                name = props.get("name", "Test Land Type")
                prepared.append((polygon, code, name, 1.0))
            return prepared
        return [(polygon, "LT1", "Test Land Type", 1.0)]

    monkeypatch.setattr(main, "prepare_clipped_shapes", fake_prepare_clipped_shapes)
    monkeypatch.setattr(
        main,
        "fetch_landtypes_intersecting_envelope",
        lambda env: {"type": "FeatureCollection", "features": []},
    )

    bore_point = Point(0.5, 0.5)
    bore_fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(bore_point),
                "properties": {
                    BORE_NUMBER_FIELD: "RN123",
                    BORE_STATUS_CODE_FIELD: "EX",
                    BORE_TYPE_CODE_FIELD: "AB",
                },
            }
        ],
    }

    monkeypatch.setattr(main, "fetch_bores_intersecting_envelope", lambda env: bore_fc)

    easement_polygon = Polygon([(0.2, 0.2), (0.2, 0.8), (0.8, 0.8), (0.8, 0.2)])
    easement_fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(easement_polygon),
                "properties": {
                    "lotplan": "1TEST",
                    "parcel_typ": "EASEMENT",  # uses config default field names
                    "feat_name": "Access Easement",
                    "tenure": "Freehold",
                    "lot_area": 123.0,
                },
            }
        ],
    }

    monkeypatch.setattr(
        main,
        "fetch_easements_intersecting_envelope",
        lambda env: easement_fc,
    )

    water_polygon = Polygon([(0.1, 0.1), (0.1, 0.9), (0.9, 0.9), (0.9, 0.1)])
    water_fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(water_polygon),
                "properties": {"code": "W-1", "name": "Test Water Feature"},
            }
        ],
    }

    monkeypatch.setattr(
        main,
        "fetch_water_layers_intersecting_envelope",
        lambda env: [
            {
                "layer_id": 25,
                "layer_title": "Test Water Layer",
                "source_layer_name": "Test Water Layer",
                "geometry_type": "esriGeometryPolygon",
                "feature_collection": water_fc,
            }
        ],
    )

    client = TestClient(main.app)
    response = client.get("/export_kmz", params={"lotplan": "1TEST", "veg_url": ""})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.google-earth.kmz")
    disposition = response.headers.get("content-disposition", "")
    assert "Property Report - 1TEST.kmz" in disposition
    assert "filename*=UTF-8''Property%20Report%20%E2%80%93%201TEST.kmz" in disposition

    with zipfile.ZipFile(io.BytesIO(response.content)) as kmz:
        names = kmz.namelist()
        assert "doc.kml" in names
        icon_entries = [name for name in names if name.startswith("icons/")]
        assert icon_entries, "expected bore icon assets in KMZ archive"
        for name in icon_entries:
            data = kmz.read(name)
            assert data, f"KMZ asset {name} is empty"

        doc_text = kmz.read("doc.kml").decode("utf-8")
        assert "Property Report – 1TEST" in doc_text
        assert "<Document><name>Property Report – 1TEST</name>" in doc_text
        assert "<Folder><name>Land Types</name>" in doc_text
        assert "<Folder><name>Vegetation</name>" in doc_text
        easement_marker = "<Folder><name>Easements</name>"
        assert easement_marker in doc_text
        easement_section = doc_text.split(easement_marker, 1)[1]
        assert "<Polygon" in easement_section or "<MultiGeometry" in easement_section
        assert "Parcel Type: EASEMENT" in doc_text
        assert "Tenure: Freehold" in doc_text
        assert "Alias: -" in doc_text
        assert "Area: " in doc_text
        assert "Access Easement (Lot/Plan: 1TEST" in doc_text
        assert "<Folder><name>Water</name>" in doc_text
        assert "<Folder><name>Water</name><Folder><name>Groundwater Bores</name>" in doc_text
        assert "<Folder><name>Test Water Layer</name>" in doc_text
        assert "Test Water Feature" in doc_text
