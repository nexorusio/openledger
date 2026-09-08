"""Bounded external media rendering for self-contained investigation reports."""

from __future__ import annotations

import hashlib
import io
import ipaddress
import math
import os
import socket
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import certifi
import urllib3

MAX_PHOTO_BYTES = 4_000_000
MAX_TILE_BYTES = 1_500_000
MAX_PORTRAIT_PIXELS = 20_000_000
MAX_MEDIA_FETCH_SECONDS = 15.0
MAP_CACHE_SECONDS = 7 * 24 * 60 * 60
MAP_ZOOM = 11
MAP_WIDTH = 920
MAP_HEIGHT = 360
MAX_MAP_TILES = 15
OSM_TILE_TEMPLATE = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
MAP_ATTRIBUTION = "Map data: OpenStreetMap contributors"
REPORT_USER_AGENT = (
    "OpenLedger-Investigation-Report/1.0 (+https://openledger.nexorus.io)"
)
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_ALLOWED_IMAGE_TYPES = frozenset({"image/gif", "image/jpeg", "image/png", "image/webp"})


def _validated_public_addresses(hostname: str, port: int) -> tuple[str, ...]:
    """Resolve a host and reject the complete answer if any address is non-public."""
    try:
        results = socket.getaddrinfo(
            hostname,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except (OSError, UnicodeError, ValueError) as error:
        raise ValueError("Media hostname could not be resolved") from error
    addresses = []
    for result in results:
        try:
            address = ipaddress.ip_address(str(result[4][0]).split("%", 1)[0])
        except ValueError as error:
            raise ValueError("Media hostname returned an invalid address") from error
        mapped = getattr(address, "ipv4_mapped", None)
        if mapped is not None:
            address = mapped
        if (
            not address.is_global
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
            or address.is_loopback
            or address.is_link_local
            or address.is_private
        ):
            raise ValueError("Media hostname resolved to a non-public address")
        canonical = str(address)
        if canonical not in addresses:
            addresses.append(canonical)
    if not addresses:
        raise ValueError("Media hostname returned no public address")
    return tuple(addresses[:4])


def _validated_media_url(value: str) -> tuple[str, str, int]:
    candidate = str(value or "").strip()
    if (
        not candidate
        or len(candidate) > 2000
        or "\\" in candidate
        or any(ord(character) < 32 for character in candidate)
    ):
        raise ValueError("Invalid media URL")
    try:
        parsed = urlparse(candidate)
        port = parsed.port or (443 if parsed.scheme.casefold() == "https" else 80)
    except ValueError as error:
        raise ValueError("Invalid media URL") from error
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    try:
        hostname = hostname.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise ValueError("Invalid media hostname") from error
    scheme = parsed.scheme.casefold()
    if (
        scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or (scheme == "http" and port != 80)
        or (scheme == "https" and port != 443)
    ):
        raise ValueError("Invalid media URL")
    if hostname == "localhost" or hostname.endswith(
        (".localhost", ".local", ".internal", ".lan")
    ):
        raise ValueError("Invalid media hostname")
    return candidate, hostname, port


def _pinned_pool(hostname: str, address: str, port: int, scheme: str):
    host_header = f"[{hostname}]" if ":" in hostname else hostname
    headers = {
        "Accept": "image/avif,image/webp,image/png,image/jpeg,image/gif;q=0.8",
        "Host": host_header,
        "User-Agent": REPORT_USER_AGENT,
    }
    timeout = urllib3.Timeout(connect=3.0, read=5.0)
    if scheme == "https":
        pool = urllib3.HTTPSConnectionPool(
            address,
            port=port,
            timeout=timeout,
            maxsize=1,
            retries=False,
            cert_reqs="CERT_REQUIRED",
            ca_certs=certifi.where(),
            assert_hostname=hostname,
            server_hostname=hostname,
        )
    else:
        pool = urllib3.HTTPConnectionPool(
            address,
            port=port,
            timeout=timeout,
            maxsize=1,
            retries=False,
        )
    return pool, headers


def fetch_public_image(url: str, *, maximum_bytes: int = MAX_PHOTO_BYTES) -> bytes:
    """Fetch one image through a DNS-pinned, redirect-bounded public connection."""
    current_url = str(url or "")
    deadline = time.monotonic() + MAX_MEDIA_FETCH_SECONDS

    def require_time_remaining() -> None:
        if time.monotonic() >= deadline:
            raise ValueError("Media response exceeded its transfer deadline")

    for redirect_count in range(3):
        require_time_remaining()
        current_url, hostname, port = _validated_media_url(current_url)
        parsed = urlparse(current_url)
        addresses = _validated_public_addresses(hostname, port)
        require_time_remaining()
        pool, headers = _pinned_pool(
            hostname,
            addresses[0],
            port,
            parsed.scheme.casefold(),
        )
        path = urlunparse(("", "", parsed.path or "/", parsed.params, parsed.query, ""))
        response = None
        try:
            response = pool.urlopen(
                "GET",
                path,
                headers=headers,
                redirect=False,
                preload_content=False,
                retries=False,
            )
            require_time_remaining()
            if response.status in _REDIRECT_STATUSES:
                location = str(response.headers.get("location") or "").strip()
                if not location or redirect_count >= 2:
                    raise ValueError("Media redirect limit reached")
                current_url = urljoin(current_url, location)
                continue
            if response.status != 200:
                raise ValueError("Media server returned an unexpected status")
            content_type = str(response.headers.get("content-type") or "")
            content_type = content_type.split(";", 1)[0].strip().casefold()
            if content_type not in _ALLOWED_IMAGE_TYPES:
                raise ValueError("Media server did not return a supported image")
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    parsed_length = int(content_length)
                except ValueError as error:
                    raise ValueError("Invalid media content length") from error
                if parsed_length < 0:
                    raise ValueError("Invalid media content length")
                if parsed_length > maximum_bytes:
                    raise ValueError("Media response is too large")
            body = bytearray()
            read_chunk = getattr(response, "read1", response.read)
            while True:
                require_time_remaining()
                # read1 performs at most one underlying socket read. Combined with
                # the monotonic checks, a peer cannot keep this call alive by
                # trickling bytes quickly enough to reset the socket read timeout.
                chunk = read_chunk(min(65_536, maximum_bytes + 1 - len(body)))
                require_time_remaining()
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > maximum_bytes:
                    raise ValueError("Media response is too large")
            if not body:
                raise ValueError("Media response was empty")
            return bytes(body)
        finally:
            if response is not None:
                response.release_conn()
            pool.close()
    raise ValueError("Media redirect limit reached")


def prepare_portrait(image_bytes: bytes, *, size: int = 560) -> bytes:
    """Validate and crop an image into the report's square portrait."""
    from PIL import Image, ImageDraw, ImageOps

    with Image.open(io.BytesIO(image_bytes)) as source:
        source.verify()
    with Image.open(io.BytesIO(image_bytes)) as source:
        if source.width * source.height > MAX_PORTRAIT_PIXELS:
            raise ValueError("Portrait dimensions are too large")
        source = ImageOps.exif_transpose(source).convert("RGB")
        portrait = ImageOps.fit(
            source,
            (size, size),
            method=Image.Resampling.LANCZOS,
            centering=(0.5, 0.38),
        )
        mask = Image.new("L", (size, size), 0)
        ImageDraw.Draw(mask).rounded_rectangle(
            (0, 0, size - 1, size - 1),
            radius=size // 14,
            fill=255,
        )
        output_image = Image.new("RGBA", (size, size), (255, 255, 255, 0))
        output_image.paste(portrait, (0, 0), mask)
        output = io.BytesIO()
        output_image.save(output, format="PNG", optimize=True)
        return output.getvalue()


def load_approved_portrait(url: str) -> Optional[bytes]:
    """Return a render-ready approved portrait, or a safe empty fallback."""
    try:
        return prepare_portrait(fetch_public_image(url))
    except Exception:
        return None


def _tile_cache_root() -> Path:
    mounted_reports = Path("/tmp/maigret_reports")
    if mounted_reports.is_dir():
        return mounted_reports / ".map-tile-cache"
    return Path("/tmp/openledger-report-map-tiles")


def _cached_tile(
    zoom: int,
    tile_x: int,
    tile_y: int,
    *,
    fetcher: Callable[..., bytes] = fetch_public_image,
) -> bytes:
    cache_root = _tile_cache_root()
    cache_key = hashlib.sha256(f"osm:{zoom}:{tile_x}:{tile_y}".encode()).hexdigest()
    cache_file = cache_root / f"{cache_key}.png"
    try:
        if (
            cache_file.is_file()
            and time.time() - cache_file.stat().st_mtime < MAP_CACHE_SECONDS
        ):
            return cache_file.read_bytes()
    except OSError:
        pass
    url = OSM_TILE_TEMPLATE.format(z=zoom, x=tile_x, y=tile_y)
    tile_bytes = fetcher(url, maximum_bytes=MAX_TILE_BYTES)
    temporary = None
    try:
        cache_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{cache_key}.",
            suffix=".tmp",
            dir=cache_root,
        )
        temporary = Path(temporary_name)
        with os.fdopen(file_descriptor, "wb") as temporary_file:
            temporary_file.write(tile_bytes)
        temporary.replace(cache_file)
    except OSError:
        pass
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
    return tile_bytes


def _mercator_pixel(
    latitude: float, longitude: float, zoom: int
) -> tuple[float, float]:
    latitude = min(max(latitude, -85.05112878), 85.05112878)
    world_pixels = 256 * (2**zoom)
    x_pixel = (longitude + 180.0) / 360.0 * world_pixels
    latitude_radians = math.radians(latitude)
    y_pixel = (
        (1.0 - math.asinh(math.tan(latitude_radians)) / math.pi) / 2.0 * world_pixels
    )
    return x_pixel, y_pixel


def render_location_map(latitude: float, longitude: float) -> Optional[bytes]:
    """Render a bounded city-scale OSM map with a visible approved-center pin."""
    from PIL import Image, ImageDraw, ImageFont

    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except (TypeError, ValueError):
        return None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None
    center_x, center_y = _mercator_pixel(latitude, longitude, MAP_ZOOM)
    left = int(round(center_x - MAP_WIDTH / 2))
    top = int(round(center_y - MAP_HEIGHT / 2))
    first_tile_x = math.floor(left / 256)
    last_tile_x = math.floor((left + MAP_WIDTH - 1) / 256)
    first_tile_y = math.floor(top / 256)
    last_tile_y = math.floor((top + MAP_HEIGHT - 1) / 256)
    tile_count = 2**MAP_ZOOM
    requested_tile_count = (last_tile_x - first_tile_x + 1) * (
        last_tile_y - first_tile_y + 1
    )
    if requested_tile_count > MAX_MAP_TILES:
        return None
    canvas = Image.new("RGB", (MAP_WIDTH, MAP_HEIGHT), "#E8EEF2")
    try:
        for tile_y in range(first_tile_y, last_tile_y + 1):
            if tile_y < 0 or tile_y >= tile_count:
                continue
            for tile_x in range(first_tile_x, last_tile_x + 1):
                wrapped_x = tile_x % tile_count
                tile_bytes = _cached_tile(MAP_ZOOM, wrapped_x, tile_y)
                with Image.open(io.BytesIO(tile_bytes)) as tile:
                    tile = tile.convert("RGB")
                    canvas.paste(
                        tile,
                        (tile_x * 256 - left, tile_y * 256 - top),
                    )
    except Exception:
        return None
    draw = ImageDraw.Draw(canvas, "RGBA")
    pin_x, pin_y = MAP_WIDTH // 2, MAP_HEIGHT // 2
    draw.ellipse(
        (pin_x - 18, pin_y - 18, pin_x + 18, pin_y + 18),
        fill=(12, 27, 42, 65),
    )
    draw.ellipse(
        (pin_x - 10, pin_y - 10, pin_x + 10, pin_y + 10),
        fill=(18, 184, 176, 255),
        outline=(255, 255, 255, 255),
        width=4,
    )
    attribution = MAP_ATTRIBUTION
    try:
        font = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            18,
        )
    except OSError:
        font = ImageFont.load_default()
    text_box = draw.textbbox((0, 0), attribution, font=font)
    text_width = text_box[2] - text_box[0]
    text_height = text_box[3] - text_box[1]
    draw.rounded_rectangle(
        (
            MAP_WIDTH - text_width - 20,
            MAP_HEIGHT - text_height - 16,
            MAP_WIDTH - 6,
            MAP_HEIGHT - 5,
        ),
        radius=4,
        fill=(255, 255, 255, 225),
    )
    draw.text(
        (MAP_WIDTH - text_width - 13, MAP_HEIGHT - text_height - 12),
        attribution,
        fill=(23, 37, 51, 255),
        font=font,
    )
    output = io.BytesIO()
    canvas.save(output, format="PNG", optimize=True)
    return output.getvalue()
