# app/kml.py
from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Optional, Sequence, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

try:
    from shapely.geometry import MultiPolygon, Polygon
except Exception:
    Polygon = MultiPolygon = None  # type: ignore

def _kml_color_abgr_with_alpha(rgb: Tuple[int,int,int], alpha: int = 160) -> str:
    r, g, b = [max(0, min(255, int(v))) for v in rgb]
    a = max(0, min(255, int(alpha)))
    return f"{a:02x}{b:02x}{g:02x}{r:02x}"


@dataclass(frozen=True)
class PointPlacemark:
    """Lightweight container describing a point placemark."""

    name: str
    description_html: str = ""
    lon: float = 0.0
    lat: float = 0.0
    style_id: Optional[str] = None
    icon_href: Optional[str] = None
    scale: float = 1.0


def _point_style_xml(style_id: str, icon_href: str, scale: float = 1.0) -> str:
    sid = html.escape(style_id)
    href = html.escape(icon_href)
    scale_val = max(0.1, float(scale)) if scale else 1.0
    return (
        f"<Style id=\"{sid}\">"
        f"<IconStyle><scale>{scale_val:.2f}</scale><Icon><href>{href}</href></Icon></IconStyle>"
        f"</Style>"
    )


def _point_placemark_xml(point: PointPlacemark) -> str:
    name = html.escape(point.name or "Point")
    desc_html = point.description_html or ""
    desc_xml = f"<description><![CDATA[{desc_html}]]></description>" if desc_html else ""
    style_xml = (
        f"<styleUrl>#{html.escape(point.style_id)}</styleUrl>" if point.style_id else ""
    )
    coords = f"{float(point.lon):.8f},{float(point.lat):.8f},0"
    return (
        f"<Placemark>"
        f"<name>{name}</name>"
        f"{desc_xml}"
        f"{style_xml}"
        f"<Point><coordinates>{coords}</coordinates></Point>"
        f"</Placemark>"
    )

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

def _collect_point_styles(points: Sequence[PointPlacemark]) -> Mapping[str, Tuple[str, float]]:
    styles: dict[str, Tuple[str, float]] = {}
    for point in points:
        if not point.style_id or not point.icon_href:
            continue
        if point.style_id in styles:
            continue
        styles[point.style_id] = (point.icon_href, point.scale or 1.0)
    return styles


def build_kml(
    clipped,
    color_fn: Callable[[str], Tuple[int, int, int]],
    folder_name: Optional[str] = None,
    *,
    point_placemarks: Optional[Iterable[PointPlacemark]] = None,
    point_folder_name: Optional[str] = None,
    **kwargs,
) -> str:
    folder_label = html.escape(folder_name or "Export")
    point_list = list(point_placemarks or [])
    point_styles = _collect_point_styles(point_list)

    styles: dict[str, str] = {}
    for _geom, code, name, _area in clipped:
        if code in styles:
            continue
        rgb = color_fn(code)
        styles[code] = _kml_color_abgr_with_alpha(rgb, alpha=180)

    style_xml: list[str] = []
    for code, kml_color in styles.items():
        style_xml.append(
            f"<Style id=\"s_{html.escape(code)}\">"
            f"<LineStyle><color>ff000000</color><width>1.2</width></LineStyle>"
            f"<PolyStyle><color>{kml_color}</color><fill>1</fill><outline>1</outline></PolyStyle>"
            f"</Style>"
        )

    for style_id, (icon_href, scale) in point_styles.items():
        style_xml.append(_point_style_xml(style_id, icon_href, scale=scale))

    placemarks: list[str] = []
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

    polygon_folder_xml = (
        f"<Folder><name>{folder_label}</name>" + "".join(placemarks) + "</Folder>"
    )

    point_folder_xml = ""
    if point_list:
        point_label = html.escape(point_folder_name or "Point Features")
        point_pm_xml = "".join(_point_placemark_xml(p) for p in point_list)
        point_folder_xml = f"<Folder><name>{point_label}</name>{point_pm_xml}</Folder>"

    kml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<kml xmlns=\"http://www.opengis.net/kml/2.2\">"
        "<Document>"
        f"<name>{folder_label}</name>"
        + "".join(style_xml)
        + polygon_folder_xml
        + point_folder_xml
        + "</Document>"
        "</kml>"
    )
    return kml

def _unpack_group(group):
    if isinstance(group, tuple) or isinstance(group, list):
        if len(group) >= 4:
            clipped, color_fn, folder_name, point_data = group[:4]
        else:
            clipped, color_fn, folder_name = group
            point_data = None
    else:
        clipped, color_fn, folder_name = group
        point_data = None
    points = list(point_data or [])
    return clipped, color_fn, folder_name, points


def build_kml_folders(
    groups: Iterable[Tuple[Iterable, Callable[[str], Tuple[int, int, int]], str]],
    doc_name: Optional[str] = None,
) -> str:
    """Build a KML document with multiple folders.

    `groups` is an iterable of `(clipped, color_fn, folder_name)` tuples, where
    `clipped` is as expected by :func:`build_kml`.
    """
    doc_label = html.escape(doc_name or "Export")
    styles: dict[str, str] = {}
    point_styles: dict[str, Tuple[str, float]] = {}

    unpacked_groups = []
    for group in groups:
        clipped, color_fn, folder_title, points = _unpack_group(group)
        unpacked_groups.append((clipped, color_fn, folder_title, points))
        for _geom, code, _name, _area in clipped:
            if code in styles:
                continue
            rgb = color_fn(code)
            styles[code] = _kml_color_abgr_with_alpha(rgb, alpha=180)
        for point in points:
            if not point.style_id or not point.icon_href:
                continue
            if point.style_id in point_styles:
                continue
            point_styles[point.style_id] = (point.icon_href, point.scale or 1.0)

    style_xml = []
    for code, kml_color in styles.items():
        style_xml.append(
            f"<Style id=\"s_{html.escape(code)}\">"
            f"<LineStyle><color>ff000000</color><width>1.2</width></LineStyle>"
            f"<PolyStyle><color>{kml_color}</color><fill>1</fill><outline>1</outline></PolyStyle>"
            f"</Style>"
        )

    for style_id, (icon_href, scale) in point_styles.items():
        style_xml.append(_point_style_xml(style_id, icon_href, scale=scale))

    folder_xml = []
    for clipped, _color_fn, fname, points in unpacked_groups:
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
        for point in points:
            placemarks.append(_point_placemark_xml(point))
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

def build_kml_nested_folders(
    nested_groups: Iterable[Tuple[str, Iterable[Tuple[Iterable, Callable[[str], Tuple[int, int, int]], str]]]],
    doc_name: Optional[str] = None,
) -> str:
    """Build a KML document with nested folder structure.

    `nested_groups` is an iterable of `(parent_folder_name, subgroups)` tuples, where
    `subgroups` is as expected by :func:`build_kml_folders`.
    """
    doc_label = html.escape(doc_name or "Export")
    styles: dict[str, str] = {}
    point_styles: dict[str, Tuple[str, float]] = {}
    unpacked_nested = []

    # Collect styles across all nested groups
    for parent_name, subgroups in nested_groups:
        unpacked_subgroups = []
        for group in subgroups:
            clipped, color_fn, fname, points = _unpack_group(group)
            unpacked_subgroups.append((clipped, color_fn, fname, points))
            for _geom, code, _name, _area in clipped:
                if code in styles:
                    continue
                rgb = color_fn(code)
                styles[code] = _kml_color_abgr_with_alpha(rgb, alpha=180)
            for point in points:
                if not point.style_id or not point.icon_href:
                    continue
                if point.style_id in point_styles:
                    continue
                point_styles[point.style_id] = (point.icon_href, point.scale or 1.0)
        unpacked_nested.append((parent_name, unpacked_subgroups))

    style_xml = []
    for code, kml_color in styles.items():
        style_xml.append(
            f"<Style id=\"s_{html.escape(code)}\">"
            f"<LineStyle><color>ff000000</color><width>1.2</width></LineStyle>"
            f"<PolyStyle><color>{kml_color}</color><fill>1</fill><outline>1</outline></PolyStyle>"
            f"</Style>"
        )

    for style_id, (icon_href, scale) in point_styles.items():
        style_xml.append(_point_style_xml(style_id, icon_href, scale=scale))

    parent_folder_xml = []
    for parent_name, subgroups in unpacked_nested:
        parent_label = html.escape(parent_name or "Folder")

        # Create subfolders within this parent
        subfolder_xml = []
        for clipped, _color_fn, subfolder_name, points in subgroups:
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
            for point in points:
                placemarks.append(_point_placemark_xml(point))
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

def write_kmz(kml_text: str, out_path: str, assets: Optional[Mapping[str, bytes]] = None) -> None:
    kml_bytes = kml_text.encode("utf-8")
    with ZipFile(out_path, "w", compression=ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_bytes)
        if assets:
            for name, data in assets.items():
                if not name or data is None:
                    continue
                zf.writestr(name, data)
