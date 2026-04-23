"""
Minimal sanity-check test - verifies pytest discovery and fixture loading work.
Delete once real tests are in place.
"""


def test_pytest_works():
    """Trivial test - if this passes, pytest is installed and runs."""
    assert 1 + 1 == 2


def test_fixture_loads(sample_api_response):
    """Verifies that tests/fixtures/sample_response.json is loadable."""
    assert sample_api_response["city_name"] == "Warsaw"
    assert "current" in sample_api_response
    assert "hourly" in sample_api_response
    assert "daily" in sample_api_response
