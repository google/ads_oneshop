# Ads OneShop

## Overview

Ads OneShop helps merchants identify opportunities to improve product data and
feature adoption, benchmarking against top retailers to maximise performance.

By implementing these recommendations, merchants can improve offer quality,
reduce disapprovals, increase advanced feature adoption and drive better auction
performance.

### Solutions Included

The Ads OneShop project contains two solutions:

*   Advanced Commerce Insights Tool (ACIT) - A monitoring dashboard for product lifecycle
*   Merchant Excellence for Partners (MEX4P) - A best-practices dashboard for Google Merchant Center (GMC) accounts


## Setup

### Prerequisites

#### Google Ads

*   [**Developer Token**](https://developers.google.com/google-ads/api/docs/get-started/dev-token)

    *   Standard Access is best, but Basic will work for prototyping/testing.

    *   As with the OAuth consent screen, it's easiest to get approval if you
        submit the request for "internal use only" (you are your own client),
        that is, if you don't plan to open up your app directly (i.e., sign in
        page) to people outside your company.


> [!IMPORTANT]
>    Legal entities are limited to one developer token per
>    company/email domain. If you already have a token somewhere, you must
>    use that token. You must not solicit a developer token from any other
>    companies (i.e., you are an agency)

> [!TIP]
>    It does not matter what MCC the Developer Token comes from;
>    all it's used for is identifying  which company wrote the calling code.

#### Merchant Center

*   At least standard access to the Merchant Center account, but Admin is preferable, as it would allow more data to be ingested.
*   Google Ads & Google Merchant Center accounts must be linked together.

### Core Pipeline Deployment

Click here to open the [tutorial in Google Cloud Shell](https://console.cloud.google.com/?cloudshell=true&cloudshell_git_repo=https://github.com/google/ads_oneshop&cloudshell_tutorial=walkthrough.md).

> [!NOTE]
> If the tutorial was closed by accident (e.g. you had to refresh the page) and you need to open it again, first run the command below to open the right directory:
>
> `cd ~/cloudshell_open/ads_oneshop/`
>
> Then, to open the tutorial again, run:
>
> `cloudshell_open --tutorial "walkthrough.md"`

> [!TIP]
> #### Implementation walkthrough  video
> We recorded a video where we go through the implementation of Merchant Excellence for Partners to help guide you through that process. You can take a look at the two takes below:
> * **[Short version](https://www.youtube.com/watch?v=8Fb-X7IJxcI)**: a version of the implementation that goes through the main parts of the process, editing out long explanations about each step and fast forwards the long waits for the scripts to run.
> * **[Long version](https://www.youtube.com/watch?v=ji8RaVgBcUI)**: a version that goes through each part in detail, explaning each step of the process and also fast forwards the long waits for the scripts to run.

### ACIT Dashboard Deployment

*   To be able to access the template below, join the
    [Ads OneShop public Google group](https://groups.google.com/g/ads-oneshop).
    *   The group is **open**, no need to request access.
    *   Just press 'Join Group' when **logged in with a Google Account**.
*   Make a Copy of the
    [template](https://lookerstudio.google.com/c/u/0/reporting/666f766e-6b80-48fb-94ca-aa8efe1113a0/page/RLaHD).
*   Update data sources to use BQ dataset.

### MEX4P Dashboard Deployment

> [!IMPORTANT]
> *    **You need to get access to the group on the next step before opening the template**.
*   To gain access to the template, ensure you've joined the
    [public group](https://groups.google.com/g/ads-oneshop) as pointed out
    previously.
*   After making sure you joined the group, copy the
    [template](https://lookerstudio.google.com/c/u/0/reporting/8b2138b7-6fd2-4c99-9910-a5f5b109015e/page/2RkaD).
*   When making a copy of the template, you'll see a popup asking you to map the "Original Data Source" to a "New Data Source", **don't** select your newly created tables just now. Instead, just click "Copy Report".
*   In the new dashboard that you just created, go through the [steps to update the data sources](https://cloud.google.com/looker/docs/studio/edit-a-data-source-article) and select your newly created tables.
