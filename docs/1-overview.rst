

========
Overview
========

`Fitbit Onboarding` provides a web application that allows users to self register and
manage consent for providing wearable data. Initially this includes a single data
provider -- Fitbit -- but in time will be expanded to include other wearables or services. The
data is regularly ingested and stored in BigQuery where it can be analyzed with a variety
of analytic tools.

Why?
====

With wearables growing usage and their increasing medical capabilities, many
medical providers and clinics want to be able to easily request access to, and
subsequently access wearable data from their patients. This data can be used to
help guide the medical practioner with additional data that augments the data
obtained at the time of the visit.

Fitbit is a leading wearable with a number of different devices and increasingly
sophisticated sensors. While Fitbit Health Solutions and their partners provide
out-of-the-box solutions for providers and payers, this open source solution
leverages Google Cloud Platform services to show how such an infrastucture can
be built and leveraged for novel use cases.

Specifically this software allows you to do the following:

+ enroll patients using a user portal
+ allow patients to manage their consent for Fitbit and other wearable device data
+ behind the scenes the software will ingest the data from the data sources and store
  it in BigQuery, where it can be used to build dashboards or ML models.
