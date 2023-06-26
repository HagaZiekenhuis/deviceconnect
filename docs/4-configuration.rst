
=============
Configuration
=============

The following environment variables will need to be defined:

* GOOGLE_CLOUD_PROJECT

* FIRESTORE_DATASET

* BIGQUERY_DATASET

* FITBIT_OAUTH_CLIENT_ID

* FITBIT_OAUTH_CLIENT_SECRET

* FITBIT_OAUTH_REDIRECT_URL

* FILE_GOOGLE_APPLICATION_CREDENTIALS

* OPENID_AUTH_METADATA_URL

* OPENID_AUTH_CLIENT_ID

* OPENID_AUTH_CLIENT_SECRET

* SECRET_KEY

* API_KEY

* FIRST_SYNC

The following environment variables are optional:

* DEBUG

* FRONTEND_ONLY

* BACKEND_ONLY

Deploying for development
=========================

The above environement variables can be added to `.env-docker`:

.. literalinclude:: ../.env-example


.. warning:: make sure you don't check this file into the version control system.
