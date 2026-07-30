---
title: "Google Cloud Storage"
description: "Rclone docs for Google Cloud Storage"
versionIntroduced: "v1.02"
---

# Google Cloud Storage

Paths are specified as `remote:bucket` (or `remote:` for the `lsd`
command.)  You may put subdirectories in too, e.g. `remote:bucket/path/to/dir`.

## Configuration

The initial setup for google cloud storage involves getting a token from Google
Cloud Storage which you need to do in your browser.  `rclone config` walks you
through it.

Here is an example of how to make a remote called `remote`.  First run:

```console
rclone config
```

This will guide you through an interactive setup process:

```text
n) New remote
d) Delete remote
q) Quit config
e/n/d/q> n
name> remote
Type of storage to configure.
Choose a number from below, or type in your own value
[snip]
XX / Google Cloud Storage (this is not Google Drive)
   \ "google cloud storage"
[snip]
Storage> google cloud storage
Google Application Client Id - leave blank normally.
client_id>
Google Application Client Secret - leave blank normally.
client_secret>
Project number optional - needed only for list/create/delete buckets - see your developer console.
project_number> 12345678
Service Account Credentials JSON file path - needed only if you want use SA instead of interactive login.
service_account_file>
Access Control List for new objects.
Choose a number from below, or type in your own value
 1 / Object owner gets OWNER access, and all Authenticated Users get READER access.
   \ "authenticatedRead"
 2 / Object owner gets OWNER access, and project team owners get OWNER access.
   \ "bucketOwnerFullControl"
 3 / Object owner gets OWNER access, and project team owners get READER access.
   \ "bucketOwnerRead"
 4 / Object owner gets OWNER access [default if left blank].
   \ "private"
 5 / Object owner gets OWNER access, and project team members get access according to their roles.
   \ "projectPrivate"
 6 / Object owner gets OWNER access, and all Users get READER access.
   \ "publicRead"
object_acl> 4
Access Control List for new buckets.
Choose a number from below, or type in your own value
 1 / Project team owners get OWNER access, and all Authenticated Users get READER access.
   \ "authenticatedRead"
 2 / Project team owners get OWNER access [default if left blank].
   \ "private"
 3 / Project team members get access according to their roles.
   \ "projectPrivate"
 4 / Project team owners get OWNER access, and all Users get READER access.
   \ "publicRead"
 5 / Project team owners get OWNER access, and all Users get WRITER access.
   \ "publicReadWrite"
bucket_acl> 2
Location for the newly created buckets.
Choose a number from below, or type in your own value
 1 / Empty for default location (US).
   \ ""
 2 / Multi-regional location for Asia.
   \ "asia"
 3 / Multi-regional location for Europe.
   \ "eu"
 4 / Multi-regional location for United States.
   \ "us"
 5 / Taiwan.
   \ "asia-east1"
 6 / Tokyo.
   \ "asia-northeast1"
 7 / Singapore.
   \ "asia-southeast1"
 8 / Sydney.
   \ "australia-southeast1"
 9 / Belgium.
   \ "europe-west1"
10 / London.
   \ "europe-west2"
11 / Iowa.
   \ "us-central1"
12 / South Carolina.
   \ "us-east1"
13 / Northern Virginia.
   \ "us-east4"
14 / Ohio.
   \ "us-east5"
15 / Oregon.
   \ "us-west1"
location> 12
The storage class to use when storing objects in Google Cloud Storage.
Choose a number from below, or type in your own value
 1 / Default
   \ ""
 2 / Multi-regional storage class
   \ "MULTI_REGIONAL"
 3 / Regional storage class
   \ "REGIONAL"
 4 / Nearline storage class
   \ "NEARLINE"
 5 / Coldline storage class
   \ "COLDLINE"
 6 / Durable reduced availability storage class
   \ "DURABLE_REDUCED_AVAILABILITY"
storage_class> 5
Remote config
Use web browser to automatically authenticate rclone with remote?
 * Say Y if the machine running rclone has a web browser you can use
 * Say N if running rclone on a (remote) machine without web browser access
If not sure try Y. If Y failed, try N.
y) Yes
n) No
y/n> y
If your browser doesn't open automatically go to the following link: http://127.0.0.1:53682/auth
Log in and authorize rclone for access
Waiting for code...
Got code
Configuration complete.
Options:
- type: google cloud storage
- client_id:
- client_secret:
- token: {"AccessToken":"xxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","RefreshToken":"x/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_xxxxxxxxx","Expiry":"2014-07-17T20:49:14.929208288+01:00","Extra":null}
- project_number: 12345678
- object_acl: private
- bucket_acl: private
Keep this "remote" remote?
y) Yes this is OK
e) Edit this remote
d) Delete this remote
y/e/d> y
```

See the [remote setup docs](/remote_setup/) for how to set it up on a
machine without an internet-connected web browser available.

Note that rclone runs a webserver on your local machine to collect the
token as returned from Google if using web browser to automatically
authenticate. This only
runs from the moment it opens your browser to the moment you get back
the verification code.  This is on `http://127.0.0.1:53682/` and this
it may require you to unblock it temporarily if you are running a host
firewall, or use manual mode.

This remote is called `remote` and can now be used like this

See all the buckets in your project

```console
rclone lsd remote:
```

Make a new bucket

```console
rclone mkdir remote:bucket
```

List the contents of a bucket

```console
rclone ls remote:bucket
```

Sync `/home/local/directory` to the remote bucket, deleting any excess
files in the bucket.

```console
rclone sync --interactive /home/local/directory remote:bucket
```

### Service Account support

You can set up rclone with Google Cloud Storage in an unattended mode,
i.e. not tied to a specific end-user Google account. This is useful
when you want to synchronise files onto machines that don't have
actively logged-in users, for example build machines.

To get credentials for Google Cloud Platform
[IAM Service Accounts](https://cloud.google.com/iam/docs/service-accounts),
please head to the
[Service Account](https://console.cloud.google.com/permissions/serviceaccounts)
section of the Google Developer Console. Service Accounts behave just
like normal `User` permissions in
[Google Cloud Storage ACLs](https://cloud.google.com/storage/docs/access-control),
so you can limit their access (e.g. make them read only). After
creating an account, a JSON file containing the Service Account's
credentials will be downloaded onto your machines. These credentials
are what rclone will use for authentication.

To use a Service Account instead of OAuth2 token flow, enter the path
to your Service Account credentials at the `service_account_file`
prompt and rclone won't use the browser based authentication
flow. If you'd rather stuff the contents of the credentials file into
the rclone config file, you can set `service_account_credentials` with
the actual contents of the file instead, or set the equivalent
environment variable.

### Service Account Authentication with Access Tokens

Another option for service account authentication is to use access tokens via
*gcloud impersonate-service-account*. Access tokens protect security by avoiding
the use of the JSON key file, which can be breached. They also bypass oauth
login flow, which is simpler on remote VMs that lack a web browser.

If you already have a working service account, skip to step 3.

#### 1. Create a service account using

```console
gcloud iam service-accounts create gcs-read-only
```

You can re-use an existing service account as well (like the one created above)

#### 2. Attach a Viewer (read-only) or User (read-write) role to the service account

```console
$ PROJECT_ID=my-project
$ gcloud --verbose iam service-accounts add-iam-policy-binding \
   gcs-read-only@${PROJECT_ID}.iam.gserviceaccount.com  \
   --member=serviceAccount:gcs-read-only@${PROJECT_ID}.iam.gserviceaccount.com \
   --role=roles/storage.objectViewer
```

Use the Google Cloud console to identify a limited role. Some relevant
pre-defined roles:

- *roles/storage.objectUser* -- read-write access but no admin privileges
- *roles/storage.objectViewer* -- read-only access to objects
- *roles/storage.admin*  -- create buckets & administrative roles

#### 3. Get a temporary access key for the service account

```console
$ gcloud auth application-default print-access-token  \
   --impersonate-service-account \
      gcs-read-only@${PROJECT_ID}.iam.gserviceaccount.com  

ya29.c.c0ASRK0GbAFEewXD [truncated]
```

#### 4. Update `access_token` setting

hit `CTRL-C` when you see *waiting for code*.  This will save the config without
doing oauth flow

```console
rclone config update ${REMOTE_NAME} access_token ya29.c.c0Axxxx
```

#### 5. Run rclone as usual

```console
rclone ls dev-gcs:${MY_BUCKET}/
```

### More Info on Service Accounts

- [Official GCS Docs](https://cloud.google.com/compute/docs/access/service-accounts)
- [Guide on Service Accounts using Key Files (less secure, but similar concepts)](https://forum.rclone.org/t/access-using-google-service-account/24822/2)

### Anonymous Access

For downloads of objects that permit public access you can configure rclone
to use anonymous access by setting `anonymous` to `true`.
With unauthorized access you can't write or create files but only read or list
those buckets and objects that have public read access.

### Application Default Credentials

If no other source of credentials is provided, rclone will fall back
to
[Application Default Credentials](https://cloud.google.com/video-intelligence/docs/common/auth#authenticating_with_application_default_credentials)
this is useful both when you already have configured authentication
for your developer account, or in production when running on a google
compute host. Note that if running in docker, you may need to run
additional commands on your google compute machine -
[see this page](https://cloud.google.com/container-registry/docs/advanced-authentication#gcloud_as_a_docker_credential_helper).

Note that in the case application default credentials are used, there
is no need to explicitly configure a project number.

### --fast-list

This remote supports `--fast-list` which allows you to use fewer
transactions in exchange for more memory. See the [rclone
docs](/docs/#fast-list) for more details.

### Listing from a BigQuery inventory table

For very large buckets the Cloud Storage list API can be slow and costly.
If you have a [GCS Storage Insights inventory
report](https://cloud.google.com/storage/docs/insights/inventory-reports)
loaded into BigQuery, set `bigquery_table` to serve all listings (both
single-level browsing and recursive `--fast-list`) from that table instead
of the list API. Object **downloads** still go through Cloud Storage.

    [gcs]
    type = google cloud storage
    service_account_file = /path/to/sa.json
    bigquery_table = my-project.inventory.gcs_objects
    bigquery_cache_db = /var/lib/rclone/gcs-cache.bolt

The table must have columns `bucket`, `name`, `size`, `md5Hash` and
`updated`. Results reflect the most recent inventory report, which lags
live bucket state, so an object written since the last report won't appear
until the cache is repopulated from a newer one.

`bigquery_cache_db` is required and points at a local bbolt file. Listings
are always served from it, never from BigQuery: one recursive query over
the whole bucket populates the cache, and every list - single-level
browsing, recursive `--fast-list`, and object stats - is then answered
locally. This keeps a tree walk to a single BigQuery job instead of one per
directory.

The cache is repopulated in three ways:

- **In the background**, once it is older than `bigquery_cache_max_age`
  (default 48h). The list that notices the age keeps serving the existing
  snapshot and returns immediately while one repopulate runs behind it, so
  a refresh never stalls a mount. Only a completely empty cache blocks,
  because there is nothing to serve yet.
- **On demand**, with `rclone backend refresh remote:bucket` - see
  [backend commands](#backend-commands) below. This is the one to run from
  cron after the inventory report updates; it fails with a non-zero exit if
  the query errors, times out, or returns no rows, and keeps serving the
  previous snapshot in all three cases.
- **On first use**, when no snapshot exists yet.

Notes:

- A populate is bounded by BigQuery's REST result API
  (`getQueryResults`), which returns large result sets slowly - budget for
  it taking substantially longer than the query itself. `bigquery_timeout`
  (default 10m) caps it. This cost is paid once per refresh, not per
  listing.
- `bigquery_cache_db` must be a local path owned by a single rclone process:
  bbolt takes an exclusive lock and memory-maps the file, so it can be
  neither shared between machines nor opened by a second rclone while a
  mount is running. Use the rc API to refresh a running mount's cache.
- This needs a BigQuery read scope in addition to the storage scope. With a
  service account or `env_auth` it is requested automatically; with
  interactive oauth you must `rclone config reconnect` to re-consent.
- The query runs in (and is billed to) the project from
  `bigquery_billing_project`, or a fully-qualified `project.dataset.table`,
  or the service account's `project_id`.
- This is most useful behind the `overlay` backend, listing a GCS origin
  while reading object data from another remote.

### Custom upload headers

You can set custom upload headers with the `--header-upload`
flag. Google Cloud Storage supports the headers as described in the
[working with metadata documentation](https://cloud.google.com/storage/docs/gsutil/addlhelp/WorkingWithObjectMetadata)

- Cache-Control
- Content-Disposition
- Content-Encoding
- Content-Language
- Content-Type
- X-Goog-Storage-Class
- X-Goog-Meta-

Eg `--header-upload "Content-Type text/potato"`

Note that the last of these is for setting custom metadata in the form
`--header-upload "x-goog-meta-key: value"`

### Modification times

Google Cloud Storage stores md5sum natively.
Google's [gsutil](https://cloud.google.com/storage/docs/gsutil) tool stores
modification time with one-second precision as `goog-reserved-file-mtime` in
file metadata.

To ensure compatibility with gsutil, rclone stores modification time in 2
separate metadata entries. `mtime` uses RFC3339 format with one-nanosecond
precision. `goog-reserved-file-mtime` uses Unix timestamp format with one-second
precision. To get modification time from object metadata, rclone reads the
metadata in the following order: `mtime`, `goog-reserved-file-mtime`, object
updated time.

Note that rclone's default modify window is 1ns.
Files uploaded by gsutil only contain timestamps with one-second precision.
If you use rclone to sync files previously uploaded by gsutil,
rclone will attempt to update modification time for all these files.
To avoid these possibly unnecessary updates, use `--modify-window 1s`.

### Restricted filename characters

| Character | Value | Replacement |
| --------- |:-----:|:-----------:|
| NUL       | 0x00  | ␀           |
| LF        | 0x0A  | ␊           |
| CR        | 0x0D  | ␍           |
| /         | 0x2F  | ／          |

Invalid UTF-8 bytes will also be [replaced](/overview/#invalid-utf8),
as they can't be used in JSON strings.

<!-- autogenerated options start - DO NOT EDIT - instead edit fs.RegInfo in backend/googlecloudstorage/googlecloudstorage.go and run make backenddocs to verify --> <!-- markdownlint-disable-line line-length -->
### Standard options

Here are the Standard options specific to google cloud storage (Google Cloud Storage (this is not Google Drive)).

#### --gcs-client-id

OAuth Client Id.

Leave blank normally.

Properties:

- Config:      client_id
- Env Var:     RCLONE_GCS_CLIENT_ID
- Type:        string
- Required:    false

#### --gcs-client-secret

OAuth Client Secret.

Leave blank normally.

Properties:

- Config:      client_secret
- Env Var:     RCLONE_GCS_CLIENT_SECRET
- Type:        string
- Required:    false

#### --gcs-project-number

Project number.

Optional - needed only for list/create/delete buckets - see your developer console.

Properties:

- Config:      project_number
- Env Var:     RCLONE_GCS_PROJECT_NUMBER
- Type:        string
- Required:    false

#### --gcs-user-project

User project.

Optional - needed only for requester pays.

Properties:

- Config:      user_project
- Env Var:     RCLONE_GCS_USER_PROJECT
- Type:        string
- Required:    false

#### --gcs-service-account-file

Service Account Credentials JSON file path.

Leave blank normally.
Needed only if you want use SA instead of interactive login.

Leading `~` will be expanded in the file name as will environment variables such as `${RCLONE_CONFIG_DIR}`.

Properties:

- Config:      service_account_file
- Env Var:     RCLONE_GCS_SERVICE_ACCOUNT_FILE
- Type:        string
- Required:    false

#### --gcs-service-account-credentials

Service Account Credentials JSON blob.

Leave blank normally.
Needed only if you want use SA instead of interactive login.

Properties:

- Config:      service_account_credentials
- Env Var:     RCLONE_GCS_SERVICE_ACCOUNT_CREDENTIALS
- Type:        string
- Required:    false

#### --gcs-anonymous

Access public buckets and objects without credentials.

Set to 'true' if you just want to download files and don't configure credentials.

Properties:

- Config:      anonymous
- Env Var:     RCLONE_GCS_ANONYMOUS
- Type:        bool
- Default:     false

#### --gcs-object-acl

Access Control List for new objects.

Properties:

- Config:      object_acl
- Env Var:     RCLONE_GCS_OBJECT_ACL
- Type:        string
- Required:    false
- Examples:
  - "authenticatedRead"
    - Object owner gets OWNER access.
    - All Authenticated Users get READER access.
  - "bucketOwnerFullControl"
    - Object owner gets OWNER access.
    - Project team owners get OWNER access.
  - "bucketOwnerRead"
    - Object owner gets OWNER access.
    - Project team owners get READER access.
  - "private"
    - Object owner gets OWNER access.
    - Default if left blank.
  - "projectPrivate"
    - Object owner gets OWNER access.
    - Project team members get access according to their roles.
  - "publicRead"
    - Object owner gets OWNER access.
    - All Users get READER access.

#### --gcs-bucket-acl

Access Control List for new buckets.

Properties:

- Config:      bucket_acl
- Env Var:     RCLONE_GCS_BUCKET_ACL
- Type:        string
- Required:    false
- Examples:
  - "authenticatedRead"
    - Project team owners get OWNER access.
    - All Authenticated Users get READER access.
  - "private"
    - Project team owners get OWNER access.
    - Default if left blank.
  - "projectPrivate"
    - Project team members get access according to their roles.
  - "publicRead"
    - Project team owners get OWNER access.
    - All Users get READER access.
  - "publicReadWrite"
    - Project team owners get OWNER access.
    - All Users get WRITER access.

#### --gcs-bucket-policy-only

Access checks should use bucket-level IAM policies.

If you want to upload objects to a bucket with Bucket Policy Only set
then you will need to set this.

When it is set, rclone:

- ignores ACLs set on buckets
- ignores ACLs set on objects
- creates buckets with Bucket Policy Only set

Docs: https://cloud.google.com/storage/docs/bucket-policy-only


Properties:

- Config:      bucket_policy_only
- Env Var:     RCLONE_GCS_BUCKET_POLICY_ONLY
- Type:        bool
- Default:     false

#### --gcs-location

Location for the newly created buckets.

Properties:

- Config:      location
- Env Var:     RCLONE_GCS_LOCATION
- Type:        string
- Required:    false
- Examples:
  - ""
    - Empty for default location (US)
  - "asia"
    - Multi-regional location for Asia
  - "eu"
    - Multi-regional location for Europe
  - "us"
    - Multi-regional location for United States
  - "asia-east1"
    - Taiwan
  - "asia-east2"
    - Hong Kong
  - "asia-northeast1"
    - Tokyo
  - "asia-northeast2"
    - Osaka
  - "asia-northeast3"
    - Seoul
  - "asia-south1"
    - Mumbai
  - "asia-south2"
    - Delhi
  - "asia-southeast1"
    - Singapore
  - "asia-southeast2"
    - Jakarta
  - "australia-southeast1"
    - Sydney
  - "australia-southeast2"
    - Melbourne
  - "europe-north1"
    - Finland
  - "europe-west1"
    - Belgium
  - "europe-west2"
    - London
  - "europe-west3"
    - Frankfurt
  - "europe-west4"
    - Netherlands
  - "europe-west6"
    - Zürich
  - "europe-central2"
    - Warsaw
  - "us-central1"
    - Iowa
  - "us-east1"
    - South Carolina
  - "us-east4"
    - Northern Virginia
  - "us-east5"
    - Ohio
  - "us-west1"
    - Oregon
  - "us-west2"
    - California
  - "us-west3"
    - Salt Lake City
  - "us-west4"
    - Las Vegas
  - "northamerica-northeast1"
    - Montréal
  - "northamerica-northeast2"
    - Toronto
  - "southamerica-east1"
    - São Paulo
  - "southamerica-west1"
    - Santiago
  - "asia1"
    - Dual region: asia-northeast1 and asia-northeast2.
  - "eur4"
    - Dual region: europe-north1 and europe-west4.
  - "nam4"
    - Dual region: us-central1 and us-east1.

#### --gcs-storage-class

The storage class to use when storing objects in Google Cloud Storage.

Properties:

- Config:      storage_class
- Env Var:     RCLONE_GCS_STORAGE_CLASS
- Type:        string
- Required:    false
- Examples:
  - ""
    - Default
  - "MULTI_REGIONAL"
    - Multi-regional storage class
  - "REGIONAL"
    - Regional storage class
  - "NEARLINE"
    - Nearline storage class
  - "COLDLINE"
    - Coldline storage class
  - "ARCHIVE"
    - Archive storage class
  - "DURABLE_REDUCED_AVAILABILITY"
    - Durable reduced availability storage class

#### --gcs-env-auth

Get GCP IAM credentials from runtime (environment variables or instance meta data if no env vars).

Only applies if service_account_file and service_account_credentials is blank.

Properties:

- Config:      env_auth
- Env Var:     RCLONE_GCS_ENV_AUTH
- Type:        bool
- Default:     false
- Examples:
  - "false"
    - Enter credentials in the next step.
  - "true"
    - Get GCP IAM credentials from the environment (env vars or IAM).

### Advanced options

Here are the Advanced options specific to google cloud storage (Google Cloud Storage (this is not Google Drive)).

#### --gcs-token

OAuth Access Token as a JSON blob.

Properties:

- Config:      token
- Env Var:     RCLONE_GCS_TOKEN
- Type:        string
- Required:    false

#### --gcs-auth-url

Auth server URL.

Leave blank to use the provider defaults.

Properties:

- Config:      auth_url
- Env Var:     RCLONE_GCS_AUTH_URL
- Type:        string
- Required:    false

#### --gcs-token-url

Token server url.

Leave blank to use the provider defaults.

Properties:

- Config:      token_url
- Env Var:     RCLONE_GCS_TOKEN_URL
- Type:        string
- Required:    false

#### --gcs-client-credentials

Use client credentials OAuth flow.

This will use the OAUTH2 client Credentials Flow as described in RFC 6749.

Note that this option is NOT supported by all backends.

Properties:

- Config:      client_credentials
- Env Var:     RCLONE_GCS_CLIENT_CREDENTIALS
- Type:        bool
- Default:     false

#### --gcs-access-token

Short-lived access token.

Leave blank normally.
Needed only if you want use short-lived access token instead of interactive login.

Properties:

- Config:      access_token
- Env Var:     RCLONE_GCS_ACCESS_TOKEN
- Type:        string
- Required:    false

#### --gcs-directory-markers

Upload an empty object with a trailing slash when a new directory is created

Empty folders are unsupported for bucket based remotes, this option creates an empty
object ending with "/", to persist the folder.


Properties:

- Config:      directory_markers
- Env Var:     RCLONE_GCS_DIRECTORY_MARKERS
- Type:        bool
- Default:     false

#### --gcs-no-check-bucket

If set, don't attempt to check the bucket exists or create it.

This can be useful when trying to minimise the number of transactions
rclone does if you know the bucket exists already.


Properties:

- Config:      no_check_bucket
- Env Var:     RCLONE_GCS_NO_CHECK_BUCKET
- Type:        bool
- Default:     false

#### --gcs-decompress

If set this will decompress gzip encoded objects.

It is possible to upload objects to GCS with "Content-Encoding: gzip"
set. Normally rclone will download these files as compressed objects.

If this flag is set then rclone will decompress these files with
"Content-Encoding: gzip" as they are received. This means that rclone
can't check the size and hash but the file contents will be decompressed.


Properties:

- Config:      decompress
- Env Var:     RCLONE_GCS_DECOMPRESS
- Type:        bool
- Default:     false

#### --gcs-endpoint

Custom endpoint for the storage API. Leave blank to use the provider default.

When using a custom endpoint that includes a subpath (e.g. example.org/custom/endpoint),
the subpath will be ignored during upload operations due to a limitation in the
underlying Google API Go client library.
Download and listing operations will work correctly with the full endpoint path.
If you require subpath support for uploads, avoid using subpaths in your custom
endpoint configuration.

Properties:

- Config:      endpoint
- Env Var:     RCLONE_GCS_ENDPOINT
- Type:        string
- Required:    false
- Examples:
  - "storage.example.org"
    - Specify a custom endpoint
  - "storage.example.org:4443"
    - Specifying a custom endpoint with port
  - "storage.example.org:4443/gcs/api"
    - Specifying a subpath, see the note, uploads won't use the custom path!

#### --gcs-encoding

The encoding for the backend.

See the [encoding section in the overview](/overview/#encoding) for more info.

Properties:

- Config:      encoding
- Env Var:     RCLONE_GCS_ENCODING
- Type:        Encoding
- Default:     Slash,CrLf,InvalidUtf8,Dot

#### --gcs-bigquery-table

BigQuery table holding a GCS Storage Insights inventory report.

If set, object listings are served from a local bbolt cache populated from this
table instead of the Cloud Storage list API - useful for very large buckets. This
requires bigquery_cache_db to be set. Give a fully-qualified `project.dataset.table`
(or `dataset.table` with bigquery_billing_project set). The table must have
columns: bucket, name, size, md5Hash, updated. Object downloads still go through
Cloud Storage.

This needs a BigQuery read scope in addition to the storage scope; with oauth
(not service account/env auth) you must reconnect to re-consent.

Properties:

- Config:      bigquery_table
- Env Var:     RCLONE_GCS_BIGQUERY_TABLE
- Type:        string
- Required:    false

#### --gcs-bigquery-billing-project

Project that runs and is billed for bigquery_table queries.

Leave blank to infer it from a fully-qualified bigquery_table, or from the
service account credentials' project_id.

Properties:

- Config:      bigquery_billing_project
- Env Var:     RCLONE_GCS_BIGQUERY_BILLING_PROJECT
- Type:        string
- Required:    false

#### --gcs-bigquery-cache-db

Local bbolt file caching bigquery_table listings (required with bigquery_table).

Required whenever bigquery_table is set. Point it at a writable local file path -
e.g. /var/lib/rclone/gcs-cache.bolt. Every list is served from this file with no
BigQuery traffic; the cache is repopulated in one BigQuery query - in the
background once it ages past bigquery_cache_max_age (lists keep serving the old
snapshot meanwhile), synchronously only when it is empty, or on demand via
"rclone backend refresh remote:bucket" (against a running mount, which holds the
file lock: "rclone rc backend/command command=refresh fs=remote:bucket").

Must be a local path owned by a single rclone process (bbolt takes an exclusive
lock and memory-maps the file) - never share it between multiple machines.

Properties:

- Config:      bigquery_cache_db
- Env Var:     RCLONE_GCS_BIGQUERY_CACHE_DB
- Type:        string
- Required:    false

#### --gcs-bigquery-cache-max-age

How stale a bigquery_cache_db entry may be before it is refreshed.

A list served from a cache older than this triggers one full repopulate in the
background (still a single BigQuery query, never one-per-directory) while the
old snapshot keeps being served. This drives periodic refresh on its own when
nothing external refreshes the cache, and acts as a safety net when something
does (e.g. a cron running the "refresh" backend command after the inventory
updates). Set to 0 to serve from the cache no matter how old it is.

Properties:

- Config:      bigquery_cache_max_age
- Env Var:     RCLONE_GCS_BIGQUERY_CACHE_MAX_AGE
- Type:        Duration
- Default:     2d

#### --gcs-bigquery-timeout

Timeout for one bigquery_table populate query (job plus result pagination).

A populate exceeding this fails; an existing cache generation keeps being served
and the populate retries after a cooldown. Set to 0 to disable.

Properties:

- Config:      bigquery_timeout
- Env Var:     RCLONE_GCS_BIGQUERY_TIMEOUT
- Type:        Duration
- Default:     10m0s

#### --gcs-description

Description of the remote.

Properties:

- Config:      description
- Env Var:     RCLONE_GCS_DESCRIPTION
- Type:        string
- Required:    false

## Backend commands

Here are the commands specific to the google cloud storage backend.

Run them with:

```console
rclone backend COMMAND remote:
```

The help below will explain what arguments each command takes.

See the [backend](/commands/rclone_backend/) command for more
info on how to pass options and arguments.

These can be run on a running backend using the rc command
[backend/command](/rc/#backend-command).

### refresh

Force a repopulate of the bigquery_table listing cache

```console
rclone backend refresh remote: [options] [<arguments>+]
```

Runs one recursive BigQuery inventory query for the remote's bucket and
atomically swaps the bbolt listing cache to the new snapshot. Ignores
bigquery_cache_max_age and the failure cooldown. Intended for a cron job after
the Storage Insights inventory updates:

    rclone backend refresh gcs-bq:bucket

Against a running mount (which holds the cache file's exclusive lock), use the
remote control API instead. Backend commands are authenticated, so the mount
needs --rc together with either --rc-no-auth or --rc-user/--rc-pass:

    rclone rc backend/command command=refresh fs=gcs-bq:bucket

The fs argument must match the remote string the mount opened, so that rclone
reuses the running instance rather than opening a second one (which would fail
on the cache file's lock).

Fails if the query errors, times out (bigquery_timeout), or returns zero rows -
in all cases the previous snapshot is kept and served.

<!-- autogenerated options stop -->

## Limitations

`rclone about` is not supported by the Google Cloud Storage backend. Backends without
this capability cannot determine free space for an rclone mount or
use policy `mfs` (most free space) as a member of an rclone union
remote.

See [List of backends that do not support rclone about](https://rclone.org/overview/#optional-features)
and [rclone about](https://rclone.org/commands/rclone_about/).
