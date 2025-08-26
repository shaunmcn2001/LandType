# app/kml.py
from __future__ import annotations
from typing import Callable, Iterable, Tuple, Optional
from zipfile import ZipFile, ZIP_DEFLATED
import html

try:
    from shapely.geometry import Polygon, MultiPolygon
except Exception:
    Polygon = MultiPolygon = None  # type: ignore

def _kml_color_abgr_with_alpha(rgb: Tuple[int,int,int], alpha: int = 160) -> str:
    r, g, b = [max(0, min(255, int(v))) for v in rgb]
    a = max(0, min(255, int(alpha)))
    return f"{a:02x}{b:02x}{g:02x}{r:02x}"

def _coords_to_kml_ring(coords) -> str:
    pts = list(coords)
    if len(pts) == 0:
        return ""
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    return " ".join(f"{float(x):.8f},{float(y):.8f},0" for x, y in pts)

def _geom_to_kml_polygons(geom) -> Iterable[str]:
    if Polygon is None or MultiPolygon is None:
        raise RuntimeError("Shapely is required for KML polygon conversion")
    geoms = []
    if isinstance(geom, Polygon):
        geoms = [geom]
    elif isinstance(geom, MultiPolygon):
        geoms = list(geom.geoms)
    else:
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
        inner_xml = "".join(
            f"<innerBoundaryIs><LinearRing><coordinates>{ring}</coordinates></LinearRing></innerBoundaryIs>"
            for ring in inners if ring
        )
        yield f"<Polygon><outerBoundaryIs><LinearRing><coordinates>{ext}</coordinates></LinearRing></outerBoundaryIs>{inner_xml}</Polygon>"

def build_kml(clipped, color_fn: Callable[[str], Tuple[int,int,int]], folder_name: Optional[str] = None, **kwargs) -> str:
    folder_label = html.escape(folder_name or "Export")
    styles = {}
    for _geom, code, name, _area in clipped:
        if code in styles: continue
        rgb = color_fn(code)
        styles[code] = _kml_color_abgr_with_alpha(rgb, alpha=180)

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
        try:
            polys = list(_geom_to_kml_polygons(geom))
        except Exception:
            polys = []
        if not polys:
            continue
        geom_xml = polys[0] if len(polys) == 1 else "<MultiGeometry>" + "".join(polys) + "</MultiGeometry>"
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

def build_kml_folders(groups: Iterable[Tuple[Iterable, Callable[[str], Tuple[int,int,int]], str]], doc_name: Optional[str] = None) -> str:
    """Build a KML document with multiple folders.

    `groups` is an iterable of `(clipped, color_fn, folder_name)` tuples, where
    `clipped` is as expected by :func:`build_kml`.
    """
    doc_label = html.escape(doc_name or "Export")
    styles = {}
    # Collect styles across all groups
    for clipped, color_fn, _fname in groups:
        for _geom, code, _name, _area in clipped:
            if code in styles:
                continue
            rgb = color_fn(code)
            styles[code] = _kml_color_abgr_with_alpha(rgb, alpha=180)

    style_xml = []
    for code, kml_color in styles.items():
        style_xml.append(
            f"<Style id=\"s_{html.escape(code)}\">"
            f"<LineStyle><color>ff000000</color><width>1.2</width></LineStyle>"
            f"<PolyStyle><color>{kml_color}</color><fill>1</fill><outline>1</outline></PolyStyle>"
            f"</Style>"
        )

    folder_xml = []
    for clipped, _color_fn, fname in groups:
        folder_label = html.escape(fname or "Layer")
        placemarks = []
        for geom, code, name, area_ha in clipped:
            esc_name = html.escape(name or code or "Unknown")
            desc = f"<![CDATA[<b>{esc_name}</b><br/>Code: <code>{html.escape(code)}</code><br/>Area: {float(area_ha):.2f} ha]]>"
            try:
                polys = list(_geom_to_kml_polygons(geom))
            except Exception:
                polys = []
            if not polys:
                continue
            geom_xml = polys[0] if len(polys) == 1 else "<MultiGeometry>" + "".join(polys) + "</MultiGeometry>"
            placemarks.append(
                f"<Placemark>"
                f"<name>{esc_name} ({html.escape(code)})</name>"
                f"<description>{desc}</description>"
                f"<styleUrl>#s_{html.escape(code)}</styleUrl>"
                f"{geom_xml}"
                f"</Placemark>"
            )
        folder_xml.append(f"<Folder><name>{folder_label}</name>" + "".join(placemarks) + "</Folder>")

    kml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<kml xmlns=\"http://www.opengis.net/kml/2.2\">"
        "<Document>"
        f"<name>{doc_label}</name>"
        + "".join(style_xml)
        + "".join(folder_xml)
        + "</Document>"
        "</kml>"
    )
    return kml

def build_kml_nested_folders(nested_groups: Iterable[Tuple[str, Iterable[Tuple[Iterable, Callable[[str], Tuple[int,int,int]], str]]]], doc_name: Optional[str] = None) -> str:
    """Build a KML document with nested folder structure.

    `nested_groups` is an iterable of `(parent_folder_name, subgroups)` tuples, where
    `subgroups` is as expected by :func:`build_kml_folders`.
    """
    doc_label = html.escape(doc_name or "Export")
    styles = {}
    
    # Collect styles across all nested groups
    for _parent_name, subgroups in nested_groups:
        for clipped, color_fn, _fname in subgroups:
            for _geom, code, _name, _area in clipped:
                if code in styles:
                    continue
                rgb = color_fn(code)
                styles[code] = _kml_color_abgr_with_alpha(rgb, alpha=180)

    style_xml = []
    for code, kml_color in styles.items():
        style_xml.append(
            f"<Style id=\"s_{html.escape(code)}\">"
            f"<LineStyle><color>ff000000</color><width>1.2</width></LineStyle>"
            f"<PolyStyle><color>{kml_color}</color><fill>1</fill><outline>1</outline></PolyStyle>"
            f"</Style>"
        )

    parent_folder_xml = []
    for parent_name, subgroups in nested_groups:
        parent_label = html.escape(parent_name or "Folder")
        
        # Create subfolders within this parent
        subfolder_xml = []
        for clipped, _color_fn, subfolder_name in subgroups:
            subfolder_label = html.escape(subfolder_name or "Layer")
            placemarks = []
            for geom, code, name, area_ha in clipped:
                esc_name = html.escape(name or code or "Unknown")
                desc = f"<![CDATA[<b>{esc_name}</b><br/>Code: <code>{html.escape(code)}</code><br/>Area: {float(area_ha):.2f} ha]]>"
                try:
                    polys = list(_geom_to_kml_polygons(geom))
                except Exception:
                    polys = []
                if not polys:
                    continue
                geom_xml = polys[0] if len(polys) == 1 else "<MultiGeometry>" + "".join(polys) + "</MultiGeometry>"
                placemarks.append(
                    f"<Placemark>"
                    f"<name>{esc_name} ({html.escape(code)})</name>"
                    f"<description>{desc}</description>"
                    f"<styleUrl>#s_{html.escape(code)}</styleUrl>"
                    f"{geom_xml}"
                    f"</Placemark>"
                )
            subfolder_xml.append(f"<Folder><name>{subfolder_label}</name>" + "".join(placemarks) + "</Folder>")
        
        parent_folder_xml.append(f"<Folder><name>{parent_label}</name>" + "".join(subfolder_xml) + "</Folder>")

    kml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<kml xmlns=\"http://www.opengis.net/kml/2.2\">"
        "<Document>"
        f"<name>{doc_label}</name>"
        + "".join(style_xml)
        + "".join(parent_folder_xml)
        + "</Document>"
        "</kml>"
    )
    return kml

def write_kmz(kml_text: str, out_path: str) -> None:
    kml_bytes = kml_text.encode("utf-8")
    with ZipFile(out_path, "w", compression=ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_bytes)
