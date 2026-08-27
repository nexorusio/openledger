from maigret.ai import _parse_responses_analysis


def test_responses_analysis_preserves_deduplicated_safe_citations():
    payload = {
        'output': [
            {'type': 'web_search_call', 'status': 'completed'},
            {
                'type': 'message',
                'content': [
                    {
                        'type': 'output_text',
                        'text': 'The evidence supports the identified subject.',
                        'annotations': [
                            {
                                'type': 'url_citation',
                                'url': 'https://example.com/profile',
                                'title': 'Official profile',
                            },
                            {
                                'type': 'url_citation',
                                'url': 'https://example.com/profile',
                                'title': 'Duplicate',
                            },
                            {
                                'type': 'url_citation',
                                'url': 'javascript:alert(1)',
                                'title': 'Unsafe',
                            },
                        ],
                    }
                ],
            },
        ]
    }

    result = _parse_responses_analysis(payload)

    assert result['analysis'].startswith('The evidence supports')
    assert result['sources'] == [
        {'title': 'Official profile', 'url': 'https://example.com/profile'}
    ]
