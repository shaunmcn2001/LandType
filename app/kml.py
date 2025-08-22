import zipfile
import tempfile
from typing import List, Tuple
from shapely.geometry import Polygon, MultiPolygon, mapping

# KML colors are aabbggrr (alpha, blue, green, red), hex. We have (r,g,b).
def _kml_color_from_rgb(rgb, alpha=255):
    r, g, b = rgb
    return f"{alpha:02x}{b:02x}{g:02x}{r:02x}"

def _coords_kml(geom4326):
    """
    Return coordinate strings for KML (lon,lat,0). Supports Polygon/MultiPolygon.
    """
    def ring_to_str(ring):
        return " ".join(f"{x:.6f},{y:.6f},0" for (x, y) in ring)

    g = geom4326
    if isinstance(g, Polygon):
        rings = [ring_to_str(list(g.exterior.coords))]
        rings += [ring_to_str(list(i.coords)) for i in g.interiors]
        return [rings]
    elif isinstance(g, MultiPolygon):
        all_polys = []
        for poly in g.geoms:
            rings = [ring_to_str(list(poly.exterior.coords))]
            rings += [ring_to_str(list(i.coords)) for i in poly.interiors]
            all_polys.append(rings)
        return all_polys
    else:
        # Fallback via mapping (shouldn't happen since we only pass polys)
        gm = mapping(g)
        out = []
        if gm["type"] == "Polygon":
            rings = [ring_to_str(r) for r in gm["coordinates"]]
            out.append(rings)
        elif gm["type"] == "MultiPolygon":
            for poly in gm["coordinates"]:
                rings = [ring_to_str(r) for r in poly]
                out.append(rings)
        return out

def build_kml(clipped: List[Tuple[object, str, str, float]], color_fn) -> str:
    """
    Build a KML document string with a Placemark per clipped polygon.
    clipped: list of (geom4326, code, name, area_ha)
    color_fn: function(code)->(r,g,b)
    """
    # Styles per code
    styles = {}
    for _g, code, _name, _ha in clipped:
        if code not in styles:
            rgb = color_fn(code)
            styles[code] = _kml_color_from_rgb(rgb, alpha=170)  # ~66% opacity

    # KML header
    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    parts.append("<Document>")
    parts.append("<name>QLD Land Types</name>")

    # Style defs
    for code, kml_color in styles.items():
        parts.append(f"<Style id=\"s_{code}\">"
                     f"<LineStyle><color>{kml_color}</color><width>1.2</width></LineStyle>"
                     f"<PolyStyle><color>{kml_color}</color><fill>1</fill><outline>1</outline></PolyStyle>"
                     f"</Style>")

    # Placemarks
    for geom, code, name, area_ha in clipped:
        coords_sets = _coords_kml(geom)
        desc = (f"<![CDATA[<b>{name}</b><br/>"
                f"Code: <code>{code}</code><br/>"
                f"Area: {area_ha:.2f} ha]]>")
        parts.append("<Placemark>")
        parts.append(f"<name>{code} – {name}</name>")
        parts.append(f"<styleUrl>#s_{code}</styleUrl>")
        parts.append(f"<description>{desc}</description>")
        # Write Multipolygon as Multiple <Polygon> in a MultiGeometry
        if len(coords_sets) > 1:
            parts.append("<MultiGeometry>")
        for rings in coords_sets:
            parts.append("<Polygon><outerBoundaryIs><LinearRing><coordinates>")
            parts.append(rings[0])
            parts.append("</coordinates></LinearRing></outerBoundaryIs>")
            for inner in rings[1:]:
                parts.append("<innerBoundaryIs><LinearRing><coordinates>")
                parts.append(inner)
                parts.append("</coordinates></LinearRing></innerBoundaryIs>")
            parts.append("</Polygon>")
        if len(coords_sets) > 1:
            parts.append("</MultiGeometry>")
        parts.append("</Placemark>")

    parts.append("</Document></kml>")
    return "\n".join(parts)

def write_kmz(kml_str: str, out_path: str):
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_str)
