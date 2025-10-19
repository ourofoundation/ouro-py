# Utility functions for using Plotly html files in Ouro


from __future__ import annotations

import os
from typing import Optional, Tuple

MODEBAR_CSS = """
<style>
/* Move Plotly toolbar to the top-left */
.plotly-graph-div .modebar {
  left: 0 !important;
  right: auto !important;
}
</style>
"""


THEME_JS = """
<script>
(function(){
  function applyTheme(theme){
    try {
      var dark = (theme === 'dark');
      var update = {
        template: dark ? 'plotly_dark' : 'plotly_white',
        paper_bgcolor: dark ? '#262626' : '#f8f8f8',
        plot_bgcolor: dark ? '#262626' : '#f8f8f8',
        font: {color: dark ? '#e5e5e5' : '#262626'},
        xaxis: {gridcolor: dark ? '#333333' : '#dddddd', zerolinecolor: dark ? '#666666' : '#999999'},
        yaxis: {gridcolor: dark ? '#333333' : '#dddddd', zerolinecolor: dark ? '#666666' : '#999999'}
      };
      var gd = document.getElementById('plot') || document.querySelector('div.plotly-graph-div');
      if (gd && window.Plotly && window.Plotly.relayout) {
        window.Plotly.relayout(gd, update);
      }
      document.documentElement.style.background = update.paper_bgcolor;
      document.body.style.background = update.paper_bgcolor;
    } catch (e) {}
  }
  window.addEventListener('message', function(event){
    var data = event && event.data ? event.data : {};
    if (data && data.type === 'ouro-theme') {
      applyTheme(data.theme);
    }
  }, false);
  try {
    var prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    applyTheme(prefersDark ? 'dark' : 'light');
  } catch (e) {}
})();
</script>
"""


def _normalize_dir_url(url: str) -> str:
    return url.rstrip("/")


def _plotly_dir_from_base(base_url: str) -> str:
    base = _normalize_dir_url(base_url)
    # If the base already ends with /plotly, don't append again
    if base.endswith("/plotly"):
        return base
    return f"{base}/plotly"


def _resolve_assets_dir_url(override_dir_url: Optional[str] = None) -> Optional[str]:
    """Resolve directory URL that contains plotly-modebar.css and plotly-theme.js.

    Precedence:
    1) override_dir_url (used as-is)
    2) OURO_PLOTLY_ASSETS_URL (used as-is)
    3) OURO_PUBLIC_ASSETS_URL (append /plotly)
    4) OURO_FRONTEND_URL (append /plotly)
    """
    if override_dir_url:
        return _normalize_dir_url(override_dir_url)

    exact = os.getenv("OURO_PLOTLY_ASSETS_URL")
    if exact:
        return _normalize_dir_url(exact)

    app_base = os.getenv("OURO_PUBLIC_ASSETS_URL") or os.getenv("OURO_FRONTEND_URL")
    if app_base:
        return _plotly_dir_from_base(app_base)

    return None


def build_plotly_asset_tags(
    assets_dir_url: Optional[str] = None,
) -> Tuple[str, str]:
    """Return (css_tag, script_tag) for Plotly UI/theme.

    If a directory URL is available, return external link/script tags that point to
    the app-hosted static assets. Otherwise, fall back to inline CSS/JS.
    """
    resolved = _resolve_assets_dir_url(assets_dir_url)
    if resolved:
        css = f'<link rel="stylesheet" href="{resolved}/plotly-modebar.css" />'
        js = f'<script src="{resolved}/plotly-theme.js"></script>'
        return css, js
    return MODEBAR_CSS, THEME_JS


def inject_assets_into_html(
    html: str,
    assets_dir_url: Optional[str] = None,
) -> str:
    """Inject CSS/JS tags into a Plotly HTML document.

    Prefers placing CSS before </head> and JS before </body>. Falls back to
    appending if the markers are missing.
    """
    css_tag, js_tag = build_plotly_asset_tags(assets_dir_url)

    # Insert CSS in <head> when possible
    if "</head>" in html:
        html = html.replace("</head>", f"{css_tag}\n</head>")
    else:
        html += css_tag

    # Insert JS before </body> when possible
    if "</body>" in html:
        html = html.replace("</body>", f"{js_tag}\n</body>")
    else:
        html += js_tag

    return html
