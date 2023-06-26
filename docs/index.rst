
.. include:: ../README.rst

Features
========

This project contains the open source implementation of `Device Connect for Fitbit <https://cloud.google.com/device-connect>`_ packaged solution, and includes the following capabilities:

* Provides end-user enrollment, consent management and Fitbit device linking,

* Provides a data connecter that ingests data from the Fitbit Web-APIs and pushes to Cloud BigQuery, where data is stored in a pseudonymised manner

* Provides looker dashboards for visualizing participants data in specific or in aggregate.

This is an implementation of the Device Connect reference architecture, simplified a bit to make it easy to customize and to deploy.

Additional features provided by this implementation:

* Support for connecting to patient management systems using OpenID Connect. The deployment instructions use `Google OIDC <https://developers.google.com/identity/protocols/oauth2/openid-connect>`_ but any OIDC can be configured.

* Daily data ingestion by default, but can be easily customized. However, this is not intended for real-time use cases.

* Stable storage for oauth2 tokens provided by Cloud FireStore, with automatic token renewal.



Implementation notes
====================

* The code is implemented in Python using the Flask framework and
   packaged as a docker container.

* The containerized application can be run manually or in a container
   runtime environment such as GKE or Cloud Run.

* The code depends on a variety of GCP services, such as BigQuery,
  Cloud Scheduler, Cloud Storage among others.  See requirements.

* FireStore for stable storage of auth tokens


.. toctree::
   :hidden:
   :glob:

   *
