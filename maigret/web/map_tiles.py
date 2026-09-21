"""Shared browser-map tile configuration.

Persona routes must agree on this value so legacy and P2 screens never split
between direct public tile requests and the protected same-origin path.
"""

import os


OSM_TILE_UPSTREAM = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
BROWSER_TILE_ROUTE = "/map-tiles/{z}/{x}/{y}.png"


def browser_map_tile_url():
    """Return an explicit provider or the safe same-origin default."""
    configured = os.getenv("OPENLEDGER_MAP_TILE_URL", "").strip()
    # Older Compose releases exported the public OSM URL by default. Preserve
    # custom providers, but upgrade that legacy value to the proxy automatically.
    if configured and configured != OSM_TILE_UPSTREAM:
        return configured
    return BROWSER_TILE_ROUTE
