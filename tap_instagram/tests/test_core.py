"""Tests standard tap features using the built-in SDK tests library."""

# import datetime
import os

from singer_sdk.helpers._util import read_json_file
from singer_sdk.testing import get_standard_tap_tests

from tap_instagram.tap import TapInstagram

CONFIG_PATH = ".secrets/config.json"

if os.getenv("CI"):  # true when running a GitHub Actions workflow
    SAMPLE_CONFIG = {
        "access_token": os.getenv("TAP_INSTAGRAM_ACCESS_TOKEN"),
        "ig_user_ids": [
            int(os.getenv("TAP_INSTAGRAM_USER_ID"))
        ],  # TODO: Accept arrays here
    }
else:
    SAMPLE_CONFIG = read_json_file(CONFIG_PATH)


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapInstagram, config=SAMPLE_CONFIG)
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
