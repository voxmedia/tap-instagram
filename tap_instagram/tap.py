"""Instagram tap class."""

from typing import Dict, List

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_instagram.streams import (
    MediaChildrenStream,
    MediaInsightsStream,
    MediaStream,
    StoriesStream,
    StoryInsightsStream,
    UserInsights28DayStream,
    UserInsightsAudienceStream,
    UserInsightsDailyStream,
    UserInsightsFollowersStream,
    UserInsightsOnlineFollowersStream,
    UserInsightsWeeklyStream,
    UsersStream,
)

STREAM_TYPES = [
    MediaChildrenStream,
    MediaInsightsStream,
    MediaStream,
    StoriesStream,
    StoryInsightsStream,
    UserInsights28DayStream,
    UserInsightsAudienceStream,
    UserInsightsDailyStream,
    UserInsightsFollowersStream,
    UserInsightsOnlineFollowersStream,
    UserInsightsWeeklyStream,
    UsersStream,
]

BASE_URL = "https://graph.facebook.com/{ig_user_id}"

session = requests.Session()


class TapInstagram(Tap):
    """Instagram tap class."""

    name = "tap-instagram"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            description="A user access token",
        ),
        th.Property(
            "ig_user_ids",
            th.ArrayType(th.IntegerType),
            required=True,
            description="User IDs of the Instagram accounts to replicate",
        ),
        th.Property(
            "media_insights_lookback_days",
            th.IntegerType,
            default=60,
            description="The tap fetches media insights for Media objects posted in the last `insights_lookback_days` "
            "days - defaults to 14 days if not provided",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "metrics_log_level",
            th.StringType,
            description="A user access token",
        ),
    ).to_dict()

    @property
    def access_tokens(self) -> Dict[str, str]:
        return {
            user_id: self._exchange_token(user_id)
            for user_id in self.config.get("ig_user_ids")
        }

    def _exchange_token(self, user_id: str):
        url = BASE_URL.format(ig_user_id=user_id)
        data = {
            "fields": "access_token,name",
            "access_token": self.config.get("access_token"),
        }
        self.logger.info(f"Exchanging access token for user: {user_id}")
        response = session.get(url=url, params=data)
        response.raise_for_status()
        self.logger.info(f"Successfully exchanged token for user: {user_id}")
        return response.json().get("access_token")

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
