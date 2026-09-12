"""Shared rendering for stored and newly returned chat messages."""

import re
from html import escape
from urllib.parse import urlsplit

from markupsafe import Markup

# Bounded, disjoint delimiters keep malformed input from causing unbounded
# backtracking. This intentionally supports a small chat-formatting subset.
_INLINE = re.compile(
    r"(?P<code>`[^`\n]{1,4000}`)"
    r"|(?P<strong>\*\*[^*\n]{1,4000}\*\*)"
    r"|(?P<em>(?<!\*)\*[^*\n]{1,4000}\*(?!\*))"
    r"|(?P<link>(?<!!)\[(?P<label>[^\[\]\n]{1,500})\]"
    r"\((?P<url>[^()\s]{1,2000})\))"
)
_LIST = re.compile(r"^\s{0,3}(?:(?P<bullet>[-+*])|\d{1,9}[.)])\s+(.+)$")


def _safe_link(value: str) -> bool:
    if "\\" in value or any(ord(char) < 32 for char in value):
        return False
    try:
        parsed = urlsplit(value)
        return bool(
            parsed.scheme in {"http", "https"}
            and parsed.hostname
            and parsed.username is None
            and parsed.password is None
        )
    except ValueError:
        return False


def render_chat_content(value: str) -> Markup:
    """Render escaped paragraphs, lists, emphasis, code and HTTP(S) links."""
    output = []
    paragraph = []
    code = []
    in_code = False
    list_tag = None

    def flush_paragraph():
        if paragraph:
            output.append(
                "<p>" + "<br>".join(_inline(line) for line in paragraph) + "</p>"
            )
            paragraph.clear()

    def close_list():
        nonlocal list_tag
        if list_tag:
            output.append(f"</{list_tag}>")
            list_tag = None

    for line in str(value or "")[:50_000].splitlines():
        if line.startswith("```"):
            flush_paragraph()
            close_list()
            if in_code:
                output.append("<pre><code>" + escape("\n".join(code)) + "</code></pre>")
                code.clear()
            in_code = not in_code
        elif in_code:
            code.append(line)
        elif not line.strip():
            flush_paragraph()
            close_list()
        elif match := _LIST.match(line):
            flush_paragraph()
            tag = "ul" if match.group("bullet") else "ol"
            if tag != list_tag:
                close_list()
                output.append(f"<{tag}>")
                list_tag = tag
            output.append("<li>" + _inline(match.group(2)) + "</li>")
        else:
            close_list()
            heading = re.match(r"^#{1,6}\s+(.+)$", line)
            if heading:
                flush_paragraph()
                output.append("<h3>" + _inline(heading.group(1)) + "</h3>")
            else:
                paragraph.append(line)
    flush_paragraph()
    close_list()
    if in_code:
        output.append("<pre><code>" + escape("\n".join(code)) + "</code></pre>")
    return Markup("\n".join(output))


def _inline(value: str) -> str:
    output = []
    position = 0
    for match in _INLINE.finditer(value):
        output.append(escape(value[position : match.start()]))
        if match.group("code"):
            output.append("<code>" + escape(match.group()[1:-1]) + "</code>")
        elif match.group("strong"):
            output.append("<strong>" + escape(match.group()[2:-2]) + "</strong>")
        elif match.group("em"):
            output.append("<em>" + escape(match.group()[1:-1]) + "</em>")
        elif _safe_link(match.group("url")):
            output.append(
                '<a href="'
                + escape(match.group("url"), quote=True)
                + '" target="_blank" rel="noopener noreferrer">'
                + escape(match.group("label"))
                + "</a>"
            )
        else:
            output.append(escape(match.group()))
        position = match.end()
    output.append(escape(value[position:]))
    return "".join(output)
