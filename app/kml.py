# app/kml.py
from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, Tuple, cast
from zipfile import ZIP_DEFLATED, ZipFile

try:
    from shapely.geometry import (
        GeometryCollection,
        LineString,
        MultiLineString,
        MultiPoint,
        MultiPolygon,
        Point,
        Polygon,
    )
except Exception:
    GeometryCollection = (
        LineString
    ) = (
        MultiLineString
    ) = MultiPoint = MultiPolygon = Point = Polygon = cast(Any, None)

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


def _coords_to_kml_path(coords) -> str:
    pts = list(coords)
    if len(pts) == 0:
        return ""
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


def _geom_to_kml_geometry(geom) -> str:
    if geom is None:
        return ""
    if Polygon is None:
        raise RuntimeError("Shapely is required for KML geometry conversion")

    try:
        geom_type = geom.geom_type
    except Exception:
        geom_type = None

    if geom_type in ("Polygon", "MultiPolygon"):
        try:
            polys = list(_geom_to_kml_polygons(geom))
        except Exception:
            polys = []
        if not polys:
            return ""
        if len(polys) == 1:
            return polys[0]
        return "<MultiGeometry>" + "".join(polys) + "</MultiGeometry>"

    if geom_type == "LineString":
        try:
            coords = geom.coords
        except Exception:
            coords = []
        path = _coords_to_kml_path(coords)
        if not path:
            return ""
        return f"<LineString><tessellate>1</tessellate><coordinates>{path}</coordinates></LineString>"

    if geom_type == "MultiLineString":
        try:
            geoms = list(geom.geoms)
        except Exception:
            geoms = []
        parts = []
        for part in geoms:
            xml = _geom_to_kml_geometry(part)
            if xml:
                parts.append(xml)
        if not parts:
            return ""
        if len(parts) == 1:
            return parts[0]
        return "<MultiGeometry>" + "".join(parts) + "</MultiGeometry>"

    if geom_type == "Point":
        try:
            coords = f"{float(geom.x):.8f},{float(geom.y):.8f},0"
        except Exception:
            return ""
        return f"<Point><coordinates>{coords}</coordinates></Point>"

    if geom_type == "MultiPoint":
        try:
            geoms = list(geom.geoms)
        except Exception:
            geoms = []
        points = []
        for part in geoms:
            if part is None or getattr(part, "is_empty", False):
                continue
            try:
                coords = f"{float(part.x):.8f},{float(part.y):.8f},0"
            except Exception:
                continue
            points.append(f"<Point><coordinates>{coords}</coordinates></Point>")
        if not points:
            return ""
        if len(points) == 1:
            return points[0]
        return "<MultiGeometry>" + "".join(points) + "</MultiGeometry>"

    if geom_type == "GeometryCollection":
        try:
            geoms = list(geom.geoms)
        except Exception:
            geoms = []
        pieces = []
        for part in geoms:
            xml = _geom_to_kml_geometry(part)
            if xml:
                pieces.append(xml)
        if not pieces:
            return ""
        if len(pieces) == 1:
            return pieces[0]
        return "<MultiGeometry>" + "".join(pieces) + "</MultiGeometry>"

    return ""

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
        desc_parts = [f"<b>{esc_name}</b>", f"Code: <code>{html.escape(code)}</code>"]
        try:
            area_val = float(area_ha)
        except (TypeError, ValueError):
            area_val = None
        if area_val and area_val > 0:
            desc_parts.append(f"Area: {area_val:.2f} ha")
        desc = f"<![CDATA[{'<br/>'.join(desc_parts)}]]>"
        geom_xml = _geom_to_kml_geometry(geom)
        if not geom_xml:
            continue
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

def _unpack_group(
    group: Any,
) -> tuple[
    list[Any],
    Optional[Callable[[str], Tuple[int, int, int]]],
    Optional[str],
    list[PointPlacemark],
    list[Any],
]:
    clipped: list[Any] = []
    color_fn: Optional[Callable[[str], Tuple[int, int, int]]] = None
    folder_name: Optional[str] = None
    point_data: Optional[Iterable[PointPlacemark]] = None
    children: Optional[Iterable[Any]] = None

    if isinstance(group, (tuple, list)):
        if len(group) >= 5:
            clipped, color_fn, folder_name, point_data, children = group[:5]
        elif len(group) == 4:
            clipped, color_fn, folder_name, point_data = group
        elif len(group) == 3:
            clipped, color_fn, folder_name = group
        elif len(group) == 2:
            clipped, color_fn = group
        elif len(group) == 1:
            clipped = group[0]
    else:
        clipped = group

    points = list(point_data or [])
    nested = list(children or [])
    return clipped, color_fn, folder_name, points, nested


def build_kml_folders(
    groups: Iterable[Any],
    doc_name: Optional[str] = None,
) -> str:
    """Build a KML document with multiple folders."""

    nested_groups = []
    for group in groups:
        clipped, color_fn, folder_title, points, children = _unpack_group(group)
        folder_label = folder_title or "Layer"
        subgroups = []
        if clipped or points:
            subgroups.append((clipped, color_fn, None, points))
        subgroups.extend(children or [])
        nested_groups.append((folder_label, subgroups))

    return build_kml_nested_folders(nested_groups, doc_name=doc_name)


def build_kml_nested_folders(
    nested_groups: Iterable[Tuple[str, Iterable[Any]]],
    doc_name: Optional[str] = None,
) -> str:
    """Build a KML document with nested folder structure.

    `nested_groups` is an iterable of `(parent_folder_name, subgroups)` tuples, where
    `subgroups` is as expected by :func:`build_kml_folders`.
    """
    doc_label = html.escape(doc_name or "Export")
    styles: dict[str, str] = {}
    point_styles: dict[str, Tuple[str, float]] = {}

    def process_groups(groups):
        processed = []
        for group in groups:
            clipped, color_fn, folder_name, points, children = _unpack_group(group)
            processed_children = process_groups(children)
            processed.append((clipped, color_fn, folder_name, points, processed_children))

            if color_fn:
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

        return processed

    processed_nested = []
    for parent_name, subgroups in nested_groups:
        processed_nested.append((parent_name, process_groups(subgroups)))

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

    def render_groups(groups):
        direct_content: list[str] = []
        folder_content: list[str] = []
        for clipped, _color_fn, folder_name, points, children in groups:
            placemarks = []
            for geom, code, name, area_ha in clipped:
                esc_name = html.escape(name or code or "Unknown")
                desc_parts = [f"<b>{esc_name}</b>", f"Code: <code>{html.escape(code)}</code>"]
                try:
                    area_val = float(area_ha)
                except (TypeError, ValueError):
                    area_val = None
                if area_val and area_val > 0:
                    desc_parts.append(f"Area: {area_val:.2f} ha")
                desc = f"<![CDATA[{'<br/>'.join(desc_parts)}]]>"
                geom_xml = _geom_to_kml_geometry(geom)
                if not geom_xml:
                    continue
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

            child_xml = render_groups(children)
            content = "".join(placemarks) + child_xml
            if folder_name:
                folder_label = html.escape(folder_name)
                folder_content.append(f"<Folder><name>{folder_label}</name>{content}</Folder>")
            else:
                direct_content.append(content)

        return "".join(direct_content + folder_content)

    parent_folder_xml = []
    for parent_name, subgroups in processed_nested:
        parent_label = html.escape(parent_name or "Folder")
        parent_folder_xml.append(
            f"<Folder><name>{parent_label}</name>" + render_groups(subgroups) + "</Folder>"
        )

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
