from html.parser import HTMLParser

import pytest

from maigret.web.chat_presentation import render_chat_content


class Elements(HTMLParser):
    def __init__(self, value):
        super().__init__()
        self.elements = []
        self.feed(value)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))


def test_chat_formats_profile_explanation_and_citation():
    html = str(
        render_chat_content(
            "The **supplied profile** remains pending.\n\n"
            "- Direct access to `www.linkedin.com` failed.\n"
            "- An [indexed profile](https://id.linkedin.com/in/alice-example?ref=search) was cited.\n\n"
            "This is *not* identity confirmation."
        )
    )
    assert "<strong>supplied profile</strong>" in html
    assert "<code>www.linkedin.com</code>" in html
    assert html.count("<li>") == 2
    assert "<em>not</em>" in html
    links = [attrs for tag, attrs in Elements(html).elements if tag == "a"]
    assert links == [
        {
            "href": "https://id.linkedin.com/in/alice-example?ref=search",
            "target": "_blank",
            "rel": "noopener noreferrer",
        }
    ]


@pytest.mark.parametrize(
    "text",
    [
        "<script>alert(1)</script><img src=x onerror=alert(1)>",
        '<svg><a xlink:href="javascript:alert(1)">click</a></svg>',
        "[click](javascript:alert%281%29)",
        "[click](jAvAsCrIpT:alert%281%29)",
        "[click](javascript&#58;alert%281%29)",
        "[click](data:text/html,attack)",
        "[click](//attacker.example/path)",
        "[click](file:///etc/passwd)",
        "[click](https://user:password@example.com/path)",
        "![image](https://attacker.example/tracker)",
        '**<iframe src="https://attacker.example"></iframe>**',
        "`<img src=x onerror=alert(1)>`",
        "```html\n<script>alert(1)</script>\n```",
    ],
)
def test_chat_cannot_inject_html_links_or_external_resources(text):
    elements = Elements(str(render_chat_content(text))).elements
    assert all(tag in {"p", "br", "strong", "em", "code", "pre"} for tag, _ in elements)
    assert all(not attrs for _, attrs in elements)


def test_chat_link_attributes_are_escaped_and_code_stays_literal():
    html = str(
        render_chat_content(
            '[source](https://example.com/"onclick="attack)\n\n'
            "```\n**literal** [link](https://example.com)\n```"
        )
    )
    links = [attrs for tag, attrs in Elements(html).elements if tag == "a"]
    assert len(links) == 1
    assert set(links[0]) == {"href", "target", "rel"}
    assert "<pre><code>**literal** [link](https://example.com)</code></pre>" in html


def test_chat_preserves_username_underscores_and_caps_input():
    assert str(render_chat_content("alice_example")) == "<p>alice_example</p>"
    html = str(render_chat_content("[" * 50_000 + "not-retained"))
    assert "not-retained" not in html
    assert html.count("[") == 50_000
