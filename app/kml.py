# app/kml.py
"""
Minimal, dependency-light KML/KMZ builder that’s compatible with the app.

Public API (compatible with your main.py):
  - build_kml(clipped, color_fn, folder_name=None, **kwargs) -> str (KML text)
  - write_kmz(kml_text, out_path) -> None

`clipped` is a list of tuples: (geom4326, code, name, area_ha)
  - geom4326 is a shapely geometry in EPSG:4326 (lon/lat)
  - code: land type code (str)
  - name: land type name (str)
  - area_ha: float hectares
`color_fn(code) -> (r, g, b)` is provided by colors.py
"""
from __future__ import annotations
from typing import Callable, Iterable, Tuple, Optional
from zipfile import ZipFile, ZIP_DEFLATED
from io import BytesIO
import html

try:
    from shapely.geometry import Polygon, MultiPolygon
except Exception:
    # If Shapely is unavailable, raise a clear error when used
    Polygon = MultiPolygon = None  # type: ignore

def _kml_color_abgr_with_alpha(rgb: Tuple[int,int,int], alpha: int = 160) -> str:
    """
    Google Earth KML color is aabbggrr (not rrggbb).
    alpha 0..255, default ~0.63 opacity.
    """
    r, g, b = [max(0, min(255, int(v))) for v in rgb]
    a = max(0, min(255, int(alpha)))
    return f"{a:02x}{b:02x}{g:02x}{r:02x}"

def _coords_to_kml_ring(coords) -> str:
    # coords: iterable of (x, y)
    # KML needs lon,lat[,alt] with the first == last
    pts = list(coords)
    if len(pts) == 0:
        return ""
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    return " ".join(f"{float(x):.8f},{float(y):.8f},0" for x, y in pts)

def _geom_to_kml_polygons(geom) -> Iterable[str]:
    """
    Yields <Polygon> ... </Polygon> strings for Polygon or MultiPolygon.
    """
    if Polygon is None or MultiPolygon is None:
        raise RuntimeError("Shapely is required for KML polygon conversion")

    geoms = []
    if isinstance(geom, Polygon):
        geoms = [geom]
    elif isinstance(geom, MultiPolygon):
        geoms = list(geom.geoms)
    else:
        # try to get polygon exteriors if possible
        try:
            if geom.geom_type == "Polygon":
                geoms = [geom]
            elif geom.geom_type == "MultiPolygon":
                geoms = list(geom.geoms)
        except Exception:
            pass

    for poly in geoms:
        ext = _coords_to_kml_ring(poly.exterior.coords)
        inners = []
        for ring in poly.interiors:
            inners.append(_coords_to_kml_ring(ring.coords))
        inner_xml = "".join(f"<innerBoundaryIs><LinearRing><coordinates>{ring}</coordinates></LinearRing></innerBoundaryIs>" for ring in inners if ring)
        yield f"<Polygon><outerBoundaryIs><LinearRing><coordinates>{ext}</coordinates></LinearRing></outerBoundaryIs>{inner_xml}</Polygon>"

def build_kml(clipped, color_fn: Callable[[str], Tuple[int,int,int]], folder_name: Optional[str] = None, **kwargs) -> str:
    """
    Build a minimal KML string with per-code styles and clickable attributes.
    Unknown kwargs are ignored to remain signature-compatible.
    """
    folder_label = html.escape(folder_name or "QLD Land Types")

    # Collect unique styles per code
    styles = {}
    for _geom, code, name, _area in clipped:
        if code in styles: continue
        rgb = color_fn(code)
        styles[code] = _kml_color_abgr_with_alpha(rgb, alpha=160)

    style_xml = []
    for code, kml_color in styles.items():
        style_xml.append(
            f"<Style id=\"s_{html.escape(code)}\">"
            f"<LineStyle><color>ff000000</color><width>1.2</width></LineStyle>"
            f"<PolyStyle><color>{kml_color}</color><fill>1</fill><outline>1</outline></PolyStyle>"
            f"</Style>"
        )

    placemarks = []
    for geom, code, name, area_ha in clipped:
        esc_name = html.escape(name or code or "Unknown")
        desc = f"<![CDATA[<b>{esc_name}</b><br/>Code: <code>{html.escape(code)}</code><br/>Area: {float(area_ha):.2f} ha]]>"
        # MultiPolygon → MultiGeometry, else single Polygon
        try:
            polys = list(_geom_to_kml_polygons(geom))
        except Exception:
            polys = []
        if not polys:
            continue
        if len(polys) == 1:
            geom_xml = polys[0]
        else:
            geom_xml = "<MultiGeometry>" + "".join(polys) + "</MultiGeometry>"

        placemarks.append(
            f"<Placemark>"
            f"<name>{esc_name} ({html.escape(code)})</name>"
            f"<description>{desc}</description>"
            f"<styleUrl>#s_{html.escape(code)}</styleUrl>"
            f"{geom_xml}"
            f"</Placemark>"
        )

    kml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<kml xmlns=\"http://www.opengis.net/kml/2.2\">"
        "<Document>"
        f"<name>{folder_label}</name>"
        + "".join(style_xml) +
        f"<Folder><name>{folder_label}</name>"
        + "".join(placemarks) +
        "</Folder>"
        "</Document>"
        "</kml>"
    )
    return kml

def write_kmz(kml_text: str, out_path: str) -> None:
    """
    Write a KMZ (zip containing doc.kml).
    """
    kml_bytes = kml_text.encode("utf-8")
    with ZipFile(out_path, "w", compression=ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_bytes)