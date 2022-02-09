# Singer Tap for Instagram

[![test](https://github.com/prratek/tap-instagram/actions/workflows/ci_workflow.yml/badge.svg)](https://github.com/prratek/tap-instagram/actions/workflows/ci_workflow.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyPI Version](https://img.shields.io/pypi/v/tap-instagram?style=flat)](https://pypi.org/project/tap-instagram/)
[![License](https://img.shields.io/pypi/l/tap-instagram)](LICENSE.md)
[![Python](https://img.shields.io/pypi/pyversions/tap-instagram)](https://pypi.org/project/tap-instagram/)

`tap-instagram` is a Singer tap for the [Instagram Graph API](https://developers.facebook.com/docs/instagram-api) built 
with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-instagram
```

## Supported Streams

The Instagram tap replicates the following data:
* [Users](https://developers.facebook.com/docs/instagram-api/reference/ig-user)
* [Media](https://developers.facebook.com/docs/instagram-api/reference/ig-user/media)
* [Stories](https://developers.facebook.com/docs/instagram-api/reference/ig-user/stories)
* [User Insights](https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights)
* [Media Insights](https://developers.facebook.com/docs/instagram-api/reference/ig-media/insights)
* [Story Insights](https://developers.facebook.com/docs/instagram-api/reference/ig-media/insights)

Those sources are spread across a few additional streams since they support varying query parameters, time periods, 
amounts of historical data, etc. The following section outlines some important information, but defer to the API docs 
linked above for more detail.

* **Users:** IG User objects representing the Instagram Business or Creator Accounts from the `ig_user_ids` config 
parameter.
  * **Replication Method:** Full Table
* **Media:** IG Media objects representing media published by a given IG User.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
  * **Limitations:** Return a max of 10k of the most recently created media objects for that user. Does not include
  stories, which are in the "stories" stream.
* **Stories:** IG Media objects representing stories published by a given IG User in the last 24 hours.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
  * **Limitations:** Stories are only available for 24hrs, do not include Live Video stories or reshared stories.
* **Media Children:** IG Media objects corresponding to images or videos in an album.
  * **Replication Method:** Full Table
  * **Parent Stream:** Media
* **User Insights 28 Day:** User Insights stream containing impressions and reach for a 28 day period.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
* **User Insights Audience:** User Insights stream containing audience metrics audience_city, audience_country, 
audience_gender_age, audience_locale for a lifetime period.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
* **User Insights Daily:** User Insights stream containing all daily metrics except follower_count, which has less 
historical data - email_contacts, get_directions_clicks, impressions, phone_call_clicks, profile_views, reach, 
text_message_clicks, website_clicks.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
* **User Insights Followers:** User Insights stream containing follower_count on a daily time period.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
  * **Limitations:** Only returns data for the last 30 days.
* **User Insights Online Followers:** User Insights stream containing online_followers on a lifetime time period.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
  * **Limitations:** Only returns data for the last 30 days.
* **User Insights Weekly:** User Insights stream containing impressions and reach for a weekly period.
  * **Replication Method:** Full Table
  * **Parent Stream:** Users
* **Media Insights:** Media Insights stream returning the supported metrics for photo, video, and album Media objects.
  * **Replication Method:** Full Table
  * **Parent Stream:** Media
  * **Limitation:** Does not return insights for media published before account was changed from personal to business.
* **Story Insights:** Media Insights stream returning the supported metrics for story Media objects.
  * **Replication Method:** Full Table
  * **Parent Stream:** Stories
  * **Limitation:** Does not return insights for stories published before account was changed from personal to business.

## Configuration

**The tap accepts the following config options:**

- **`ig_user_ids: List[str]` (required)**: List of user IDs of Instagram Business Accounts or Instagram Creator Accounts. One way to 
find the user ID is by navigating to
> [Facebook Business Manager](https://business.facebook.com) > Accounts > Instagram accounts > [Your Account]

and you will see the user ID in the URL - https://business.facebook.com/instagram-account-v2s/{user_id}?business_id={business_id}

- **`access_token: str` (required)**: A long-lived **user access token**, which can be obtained by following 
[these instructions](https://developers.facebook.com/docs/pages/access-tokens). Ensure the access token has the 
following permissions:
  - `instagram_basic`
  - `instagram_manage_insights`
  - `pages_show_list`
  - `pages_read_engagement`

  **NOTE: You will need to create a Facebook App if you do not have one already to be able to generate an access token**

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-instagram --about
```


## Usage

You can easily run `tap-instagram` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-instagram --version
tap-instagram --help
tap-instagram --config CONFIG --discover > ./catalog.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_instagram/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-instagram` CLI interface directly using `poetry run`:

```bash
poetry run tap-instagram --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-instagram
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-instagram --version
# OR run a test `elt` pipeline:
meltano elt tap-instagram target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
