from maigret.web.persona_images import ordered_profile_image_urls


def test_operator_selected_profile_image_precedes_the_stable_fallback():
    assert ordered_profile_image_urls(
        [
            {'url': 'https://images.example.test/a.jpg'},
            {
                'url': 'https://images.example.test/z.jpg',
                'selected': True,
                'selected_at': '2026-09-22T12:00:00+00:00',
            },
        ]
    ) == [
        'https://images.example.test/z.jpg',
        'https://images.example.test/a.jpg',
    ]


def test_profile_images_use_url_order_when_no_operator_choice_exists():
    assert ordered_profile_image_urls(
        [
            {'url': 'https://images.example.test/z.jpg'},
            {'url': 'https://images.example.test/a.jpg'},
        ]
    ) == [
        'https://images.example.test/a.jpg',
        'https://images.example.test/z.jpg',
    ]
