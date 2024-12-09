# Deploying Ads OneShop

<!-- TODO: Update numbering here and below for style guide -->

<walkthrough-metadata>
  <meta name="title" content="Deploying Ads OneShop" />
  <meta name="description" content="A step by step guide on configuring cloud and deploying the dashboard." />
</walkthrough-metadata>


## Introduction

In this walkthrough, you'll generate OAuth credentials in preparation for the deployment of Ads OneShop.

<walkthrough-tutorial-difficulty difficulty="3"></walkthrough-tutorial-difficulty>
<walkthrough-tutorial-duration duration="45"></walkthrough-tutorial-duration>


## Prerequisites

To run OneShop, you will need:

 - A Google Cloud Platform project
 - A Google Ads Developer Token

Please see the README for instructions for obtaining a Google Ads developer
token if you do not have one already.

<walkthrough-project-setup billing="true"></walkthrough-project-setup>


## Enable Google Cloud APIs

Enable the required APIs for this project.

<walkthrough-enable-apis apis="serviceusage.googleapis.com,iam.googleapis.com,googleads.googleapis.com,shoppingcontent.googleapis.com">
</walkthrough-enable-apis>


## Configure OAuth Consent Screen

Set up OAuth consent.

1.  Go to the **APIs & Services > OAuth consent screen** page in the Cloud
    Console. You can use the button below to find the section.

    <walkthrough-path-nav path="/apis/credentials/consent">OAuth consent screen</walkthrough-path-nav>

1.  Choose **Internal** as the user type for your application,
    If this application is only going to be used by your organization. If you
    are not part of a GCP organization, you will only have the "External"
    option.

1.  Click
    <walkthrough-spotlight-pointer cssSelector="button[type='submit']">**Create**</walkthrough-spotlight-pointer>
    to continue.

1.  Under *App information*, enter the **Application name** you want to display.
    You can call it "Ads OneShop."

1.  For the **Support email** dropdown menu, select the email address you want
    to display as a public contact. This email address must be your email
    address, or a Google Group you own.

1.  Under **Developer contact information**, enter a valid email address.

Click
<walkthrough-spotlight-pointer cssSelector=".cfc-stepper-step-continue-button">
  **Save and continue**
</walkthrough-spotlight-pointer>.


## Sensitive Scopes on Consent Screen (Optional)

NOTE: This section is only required if your application is **not** internal-only.

1.  Click
    <walkthrough-spotlight-pointer locator="semantic({button 'Add or remove scopes'})">
      Add or remove scopes
    </walkthrough-spotlight-pointer>

1.  Now in
    <walkthrough-spotlight-pointer locator="semantic({combobox 'Filter'})">
      Enter property name or value
    </walkthrough-spotlight-pointer> search for the Google Ads API, check the
    box for the first option to choose it.

1.  Do the same for Content API for Shopping. Choose the first (content) scope.

1.  Click
    <walkthrough-spotlight-pointer locator="text('Update')">
      Update,
    </walkthrough-spotlight-pointer>
    and then
    <walkthrough-spotlight-pointer cssSelector=".cfc-stepper-step-continue-button">
      **Save and continue**
    </walkthrough-spotlight-pointer>.


NOTE: If your app is external, you will have to add yourself to the list of
external users.


## Creating OAuth Credentials

Create the credentials that are needed for generating a refresh token.

1.  On the APIs & Services page, click the
    <walkthrough-spotlight-pointer cssSelector="#cfctest-section-nav-item-metropolis_api_credentials">**Credentials**</walkthrough-spotlight-pointer>
    tab.

1.  On the
    <walkthrough-spotlight-pointer cssSelector="[id$=action-bar-create-button]" validationPath="/apis/credentials">**Create
    credentials**</walkthrough-spotlight-pointer> drop-down list, select **OAuth
    client ID**.

1.  Under
    <walkthrough-spotlight-pointer cssSelector="[formcontrolname='typeControl']">**Application
    type**</walkthrough-spotlight-pointer>, select **Web application**.

1.  Add a
    <walkthrough-spotlight-pointer cssSelector="[formcontrolname='displayName']">**Name**</walkthrough-spotlight-pointer>
    for your OAuth client ID, like "OAuth Playground."

1.  Click
    <walkthrough-spotlight-pointer locator="semantic({group 'Authorized redirect URIs'} {button 'Add URI'})">Authorized redirect URI</walkthrough-spotlight-pointer>
    and copy the following:

    ```
    https://developers.google.com/oauthplayground
    ```

1.  Click **Create**. Your OAuth client ID and client secret are generated and
    displayed in a popup.

    Within that pop-up, dlick the "Download JSON" button, and save the
    credentials to your computer. Alternatively, you can click the icon on the
    same credentials page where they were created.


## Prepare application secrets

Now we will switch over to the editor, and save application secrets.

The following files will be saved to your personal Cloud Shell storage, where
only you can access them. Later on, we'll  make the secrets accessible to the
application.

First, switch to the workspace:

```sh
cloudshell open-workspace ~/cloudshell_open/ads_oneshop
```

Wait for the workspace to load.

Then,
<walkthrough-editor-spotlight spotlightId="menu-terminal-new-terminal">
  open a terminal
</walkthrough-editor-spotlight> and run the following command:

<!-- Files must exist for the editor to open them -->
```sh
gcloud config set project <walkthrough-project-id>
touch -a client_secrets.json refresh_token.txt google_ads_developer_token.txt env.sh
```

Open
<walkthrough-editor-open-file filePath="client_secrets.json">
  client_secrets.json
</walkthrough-editor-open-file>
and paste in the contents of the OAuth secrets file you downloaded earlier.
Save the file.

Run the following command and click the link to authenticate with the OAuth
playground:

```sh
python acit/auth/oauth.py client_secrets.json adwords content
```

Click the blue **Authorize APIs** button on the left-hand pane.
![Authorize APIs](https://services.google.com/fh/files/misc/authorize_apis.png)

If you are prompted to authorize access, please choose your Google account that
has access to Google Ads and Merchant Center, and approve.

Now, click the new blue button **Exchange authorization code for tokens**
![Exchange authorization code for tokens](https://services.google.com/fh/files/misc/exchange_authorization_code_for_token.png)

Next, At the bottom of that screen you'll see your refresh token on the last
line.  Copy the value of the "refresh_token" field only. *Do not copy the
quotation marks*
![refresh_token](https://services.google.com/fh/files/misc/refresh_token.png)

(A larger version of this image may be found [here](https://services.google.com/fh/files/misc/refresh_token.png).)

Open
<walkthrough-editor-open-file filePath="refresh_token.txt">
  refresh_token.txt
</walkthrough-editor-open-file>
and paste the refresh token into it. Save the file.

Finally, open
<walkthrough-editor-open-file filePath="google_ads_developer_token.txt">
  google_ads_developer_token.txt
</walkthrough-editor-open-file>
and paste your Google Ads Developer Token there. Save the file.


## Set up cloud environment

Set up the infrastructure.

<!-- TODO: fix region parameterization -->

Run the following commands to provision the infrastructure:

```sh
export GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
terraform -chdir=infra/ init
terraform -chdir=infra/ apply -auto-approve -var "project_id=${GOOGLE_CLOUD_PROJECT}" -var "region=us-central1"
```


## Build the container images

Run the following command to build the container images:

```sh
export CLOUD_BUILD_SERVICE_ACCOUNT="$(terraform -chdir=infra/ output -json cloud_build_sa | jq -r)"
export DATAFLOW_REGION="$(terraform -chdir=infra/ output -json region | jq -r)"
export CLOUD_BUILD_LOGS_URL="$(terraform -chdir=infra/ output -json cloud_build_logs_url | jq -r)"
export IMAGES_REPO="$(terraform -chdir=infra/ output -json images_repo | jq -r)"
./build_images.sh
```


## Store application secrets

Run the following command to install the application locally:

```sh
python -m venv .venv
source ./.venv/bin/activate
python -m pip install -U pip wheel build
python -m pip install -e .
```

Next, run the command to store the application secrets:

```sh
python -m acit.register_app_secrets
```


## Deploy the job


Open
<walkthrough-editor-open-file filePath="env.sh">
  env.sh
</walkthrough-editor-open-file>
and paste in content similar to

```
# Deployment config
# Google Ads top-level login customer IDs, comma-delimited, no hyphens.
# If you'd like to specify a child account, you can also do
#   `login_customer_id:child_mcc_or_leaf_account`.
export CUSTOMER_IDS="1234567890,0987654321"
# Merchant Center account IDs, comma-delimited, may be parents or leaves.
export MERCHANT_IDS="12345,67890"
# Update to "true" if you are an admin on the Merchant Center account.
export ADMIN="false"
# The location of the outpu BigQuery dataset
export DATASET_LOCATION="US"
# The name for the output BigQuery dataset
export DATASET_NAME="oneshop"

# Optionally, if you'd like to run Merchant Excellence, uncomment the following line:
# NOTE: This will fail if benchmarks have not been uploaded.
# export RUN_MERCHANT_EXCELLENCE=true
```

Update the file contents to match your environment.
NOTE: You must update Google Ads Customer IDs and Merchant IDs at a minimum.

Save the file, and then run the following command to create the job:

```sh
source env.sh
export DATAFLOW_REGION="$(terraform -chdir=infra/ output -json region | jq -r)"
export IMAGES_REPO="$(terraform -chdir=infra/ output -json images_repo | jq -r)"
./deploy_job.sh
```


## Running and scheduling

To run, run the command

```sh
./run_job.sh
```

It may take some time for the job to complete. Check the Cloud Run jobs UI and
Dataflow UI to see progress.

If everything ran correctly, you can schedule the job with:

```sh
./schedule_job.sh
```


## Conclusion

Congratulations. You've set up Ads OneShop!

<walkthrough-conclusion-trophy></walkthrough-conclusion-trophy>

<walkthrough-inline-feedback></walkthrough-inline-feedback>
