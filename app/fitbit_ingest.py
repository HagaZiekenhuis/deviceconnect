# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""routes for fitbit data ingestion into bigquery

provides several routes used to query fitbit web apis
and process and ingest data into bigquery tables.  Typically
this would be provided only for administrative access or
run on a schedule.

Routes:

    /ingest: test route to test if the blueprint is correctly registered

    /update_tokens: will refresh all fitbit tokens to ensure they are valid
        when being used.

    /fitbit_chunk_1: Device information

    /fitbit_body_weight: body and weight data

    /fitbit_nutrition_scope: nutrition data

    /fitbit_heart_rate_scope: heart rate information

    /fitbit_intraday_scope: includes intraday heartrate and steps

    /fitbit_sleep_scope:  sleep data

Dependencies:

    - fitbit application configuration is required to access the
        fitbit web apis.  see documentation on configuration

    - user refresh tokens are required to refresh the access token.
        this is stored in the backend firestore tables.  See
        documentation for details.

Configuration:

    * `GOOGLE_CLOUD_PROJECT`: gcp project where bigquery is available.
    * `GOOGLE_APPLICATION_CREDENTIALS`: points to a service account json.
    * `BIGQUERY_DATASET`: dataset to use to store user data.

Notes:

    all the data is ingested into BigQuery tables.

    there is currently no protection for these routes.

"""

import os
import timeit
from datetime import datetime, timedelta, date
from google.api_core.exceptions import NotFound
import logging

import pandas as pd
import pandas_gbq
from pandas_gbq.exceptions import GenericGBQException
from flask import Blueprint, request
from flask_dance.contrib.fitbit import fitbit
from skimpy import clean_columns

from .fitbit_auth import fitbit_bp
from .security import token_required


log = logging.getLogger(__name__)
last_resp = 0


bp = Blueprint("fitbit_ingest_bp", __name__)

bigquery_datasetname = os.environ.get("BIGQUERY_DATASET", "fitbit")


def _tablename(table: str) -> str:
    return bigquery_datasetname + "." + table


@bp.route("/ingest", methods=["GET", "POST"])
@token_required
def ingest():
    """Get all data from endpoints"""
    fitbit_bp.storage.user = None
    log.info('Received request')
    try:
        last_exit_code = fitbit_chunk_1()
    except (Exception):
        fitbit_bp.storage.user = None
    """
    if last_exit_code != 429:
        last_exit_code = fitbit_body_weight()
    else:
        return '', 429
    if last_exit_code != 429:
        last_exit_code = fitbit_nutrition_scope()
    else:
        return '', 429
    """
    if last_exit_code != 429:
        try:
            last_exit_code = fitbit_heart_rate_scope()
        except (Exception):
            fitbit_bp.storage.user = None
    else:
        fitbit_bp.storage.user = None
        return '', 429
    if last_exit_code != 429:
        try:
            last_exit_code = fitbit_activity_scope()
        except (Exception):
            fitbit_bp.storage.user = None
    else:
        fitbit_bp.storage.user = None
        return '', 429
    if last_exit_code != 429:
        try:
            last_exit_code = fitbit_intraday_scope()
        except (Exception):
            fitbit_bp.storage.user = None
    else:
        fitbit_bp.storage.user = None
        return '', 429
    if last_exit_code != 429:
        try:
            last_exit_code = fitbit_sleep_scope()
        except (Exception):
            fitbit_bp.storage.user = None
    else:
        fitbit_bp.storage.user = None
        return '', 429
    fitbit_bp.storage.user = None
    return '', 200


def _normalize_response(df, column_list, fitbit_id, pulled_date=None, date_column=None, status_code=None):
    for col in column_list:
        if col not in df.columns:
            df[col] = None
    df = df.reindex(columns=column_list)
    if df.empty and pulled_date is not None and status_code == 200:
        # Write row with only date to make sure the application knows it has already been synced
        df = df.append({date_column: pulled_date}, ignore_index=True)
    df.insert(0, "id", fitbit_id)
    df.insert(1, "pull_date", datetime.now().strftime("%Y-%m-%d"))
    df = clean_columns(df)
    return df


def _last_date_in_bq(tablename: str, user_id: str,
                     datetime_column: str, date_type: bool = False):
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if date_type:
        sql = "SELECT `%s` AS `last_sync` " \
            "FROM `%s` " \
            "WHERE `id` = '%s' " \
            "ORDER BY `last_sync` DESC " \
            "LIMIT 1" % (datetime_column, tablename, user_id)
    else:
        sql = "SELECT EXTRACT(DATE FROM `%s`) AS `last_sync` " \
            "FROM `%s` " \
            "WHERE `id` = '%s' " \
            "ORDER BY `last_sync` DESC " \
            "LIMIT 1" % (datetime_column, tablename, user_id)
    try:
        df = pandas_gbq.read_gbq(sql, project_id=project_id, progress_bar_type=None)
        try:
            last_sync = df['last_sync'].tolist()[0] + timedelta(days=1)
        except IndexError:
            # No earlier data found, possibly the first sync
            last_sync = datetime.strptime(os.environ.get('FIRST_SYNC'), "%Y-%m-%d")
    except (NotFound, GenericGBQException) as error:
        # Table might not yet exist
        logging.debug(error)
        last_sync = datetime.strptime(os.environ.get('FIRST_SYNC'), "%Y-%m-%d")
    if isinstance(last_sync, date):
        last_sync = datetime.combine(last_sync, datetime.min.time())
    return last_sync


#
# Chunk 1: Device
#
@bp.route("/fitbit_chunk_1")
@token_required
def fitbit_chunk_1():

    start = timeit.default_timer()
    global last_resp
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    user_list = fitbit_bp.storage.all_users()
    try:
        if request.headers["user"] in user_list:
            user_list = [request.headers["user"]]
    except (Exception):
        pass

    log.debug("fitbit_chunk_1:")

    pd.set_option("display.max_columns", 500)

    device_list = []

    for user in user_list:
        log.debug("user: %s", user)

        fitbit_bp.storage.user = user

        if fitbit_bp.session.token:
            del fitbit_bp.session.token

        try:
            # CONNECT TO DEVICE ENDPOINT #
            resp = fitbit.get("/1/user/-/devices.json")
            last_resp = resp.status_code

            log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

            device_df = pd.json_normalize(resp.json())
            try:
                device_df = device_df.drop(
                    ["features", "id", "mac", "type"], axis=1
                )
            except (Exception):
                pass

            device_columns = [
                "battery",
                "batteryLevel",
                "deviceVersion",
                "lastSyncTime",
            ]
            device_df = _normalize_response(
                device_df, device_columns, fitbit_bp.token['user_id']
            )
            device_df["last_sync_time"] = device_df["last_sync_time"].apply(
                lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f")
            )
            device_list.append(device_df)

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    # end loop over users

    # CONCAT DATAFRAMES INTO BULK DF #

    load_stop = timeit.default_timer()
    time_to_load = load_stop - start
    log.info("Program Executed in " + str(time_to_load))

    # LOAD DATA INTO BIGQUERY #

    log.debug("push to BQ")

    if len(device_list) > 0:

        try:

            bulk_device_df = pd.concat(device_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_device_df,
                destination_table=_tablename("device"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "battery",
                        "type": "STRING",
                        "description": "Returns the battery level of the device. Supported: High | Medium | Low | \
                            Empty",
                    },
                    {
                        "name": "battery_level",
                        "type": "INTEGER",
                        "description": "Returns the battery level percentage of the device.",
                    },
                    {
                        "name": "device_version",
                        "type": "STRING",
                        "description": "The product name of the device.",
                    },
                    {
                        "name": "last_sync_time",
                        "type": "TIMESTAMP",
                        "description": "Timestamp representing the last time the device was sync'd with the Fitbit \
                            mobile application.",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    log.info("Fitbit Chunk Loaded " + str(execution_time))

    fitbit_bp.storage.user = None

    return last_resp


#
# Body and Weight
#
@bp.route("/fitbit_body_weight")
@token_required
def fitbit_body_weight():

    start = timeit.default_timer()
    global last_resp
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    user_list = fitbit_bp.storage.all_users()
    try:
        if request.headers["user"] in user_list:
            user_list = [request.headers["user"]]
    except (Exception):
        pass

    pd.set_option("display.max_columns", 500)

    body_weight_df_list = []

    for user in user_list:

        log.debug("user: %s", user)

        fitbit_bp.storage.user = user

        if fitbit_bp.session.token:
            del fitbit_bp.session.token

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("body_weight"),
                                     fitbit_bp.token['user_id'], "date", True))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:

                resp = fitbit.get(
                    "/1/user/-/body/log/weight/date/" + pull_date.strftime("%Y-%m-%d") + ".json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                try:
                    body_weight = resp.json()["weight"]
                    body_weight_df = pd.json_normalize(body_weight)
                except KeyError:
                    assert body_weight, "weight returned no data"
                    if resp.status_code == 200:
                        body_weight_df = pd.json_normalize({})

                body_weight_columns = ["bmi", "date", "fat", "logId", "source", "time", "weight"]
                body_weight_df = _normalize_response(
                    body_weight_df, body_weight_columns, fitbit_bp.token['user_id'],
                    pull_date.strftime("%Y-%m-%d"), 'date', resp.status_code
                )
                body_weight_df['date'] = pd.to_datetime(body_weight_df['date'], format='%Y-%m-%d')
                body_weight_df['time'] = pd.to_datetime(body_weight_df['time'], format='%H:%M:%S')
                body_weight_df_list.append(body_weight_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

    # end loop over users

    log.debug("push to BQ")
    if len(body_weight_df_list) > 0:

        try:

            bulk_body_weight_df = pd.concat(body_weight_df_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_body_weight_df,
                destination_table=_tablename("body_weight"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "bmi",
                        "type": "FLOAT",
                        "description": "Calculated BMI in the format X.XX",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "description": "Log entry date in the format yyyy-MM-dd.",
                    },
                    {
                        "name": "fat",
                        "type": "FLOAT",
                        "description": "The body fat percentage.",
                    },
                    {
                        "name": "log_id",
                        "type": "INTEGER",
                        "description": "Weight Log IDs are unique to the user, but not globally unique.",
                    },
                    {
                        "name": "source",
                        "type": "STRING",
                        "description": "The source of the weight log.",
                    },
                    {
                        "name": "time",
                        "type": "TIME",
                        "description": "Time of the measurement; hours and minutes in the format HH:mm:ss."
                    },
                    {
                        "name": "weight",
                        "type": "FLOAT",
                        "description": "Weight in the format X.XX,",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    log.info("Body & Weight Scope Loaded " + str(execution_time))

    fitbit_bp.storage.user = None
    return last_resp


#
# Nutrition Data
#
@bp.route("/fitbit_nutrition_scope")
@token_required
def fitbit_nutrition_scope():

    start = timeit.default_timer()
    global last_resp
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    user_list = fitbit_bp.storage.all_users()
    try:
        if request.headers["user"] in user_list:
            user_list = [request.headers["user"]]
    except (Exception):
        pass

    pd.set_option("display.max_columns", 500)

    nutrition_summary_list = []
    nutrition_logs_list = []
    nutrition_goals_list = []

    for user in user_list:

        log.debug("user: %s", user)

        fitbit_bp.storage.user = user

        if fitbit_bp.session.token:
            del fitbit_bp.session.token

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("nutrition_summary"),
                                     fitbit_bp.token['user_id'], "date", True))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:

                resp = fitbit.get(
                    "/1/user/-/foods/log/date/" + pull_date.strftime("%Y-%m-%d") + ".json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                nutrition_summary = resp.json()["summary"]
                nutrition_summary['date'] = pull_date.strftime("%Y-%m-%d")
                nutrition_logs = resp.json()["foods"]

                nutrition_summary_df = pd.json_normalize(nutrition_summary)
                nutrition_logs_df = pd.json_normalize(nutrition_logs)

                try:
                    nutrition_logs_df = nutrition_logs_df.drop(
                        [
                            "loggedFood.creatorEncodedId",
                            "loggedFood.unit.id",
                            "loggedFood.units",
                        ],
                        axis=1,
                    )
                except (Exception):
                    pass

                nutrition_summary_columns = [
                    "date",
                    "calories",
                    "carbs",
                    "fat",
                    "fiber",
                    "protein",
                    "sodium",
                    "water",
                ]
                nutrition_logs_columns = [
                    "isFavorite",
                    "logDate",
                    "logId",
                    "loggedFood.accessLevel",
                    "loggedFood.amount",
                    "loggedFood.brand",
                    "loggedFood.calories",
                    "loggedFood.foodId",
                    "loggedFood.mealTypeId",
                    "loggedFood.name",
                    "loggedFood.unit.name",
                    "loggedFood.unit.plural",
                    "nutritionalValues.calories",
                    "nutritionalValues.carbs",
                    "nutritionalValues.fat",
                    "nutritionalValues.fiber",
                    "nutritionalValues.protein",
                    "nutritionalValues.sodium",
                    "loggedFood.locale",
                ]

                nutrition_summary_df = _normalize_response(
                    nutrition_summary_df,
                    nutrition_summary_columns,
                    fitbit_bp.token['user_id'],
                    pull_date.strftime("%Y-%m-%d"),
                    "date",
                    resp.status_code
                )
                nutrition_logs_df = _normalize_response(
                    nutrition_logs_df, nutrition_logs_columns, fitbit_bp.token['user_id'],
                    pull_date.strftime("%Y-%m-%d"), "logDate", resp.status_code
                )

                nutrition_summary_list.append(nutrition_summary_df)
                nutrition_logs_list.append(nutrition_logs_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

        try:
            resp = fitbit.get("/1/user/-/foods/log/goal.json")
            last_resp = resp.status_code

            log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

            try:
                nutrition_goal = resp.json()["goals"]
            except KeyError as e:
                if resp.status_code == 200:
                    nutrition_goal = {}
                else:
                    raise e

            nutrition_goal_df = pd.json_normalize(nutrition_goal)
            nutrition_goal_columns = ["calories"]
            nutrition_goal_df = _normalize_response(
                nutrition_goal_df, nutrition_goal_columns, fitbit_bp.token['user_id']
            )
            nutrition_goals_list.append(nutrition_goal_df)

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    # end of loop over users
    log.debug("push to BQ")

    if len(nutrition_summary_list) > 0:

        try:

            bulk_nutrition_summary_df = pd.concat(
                nutrition_summary_list, axis=0
            )

            pandas_gbq.to_gbq(
                dataframe=bulk_nutrition_summary_df,
                destination_table=_tablename("nutrition_summary"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "description": "Date on which information was logged"
                    },
                    {
                        "name": "calories",
                        "type": "FLOAT",
                        "description": "Total calories consumed.",
                    },
                    {
                        "name": "carbs",
                        "type": "FLOAT",
                        "description": "Total carbs consumed.",
                    },
                    {
                        "name": "fat",
                        "type": "FLOAT",
                        "description": "Total fats consumed.",
                    },
                    {
                        "name": "fiber",
                        "type": "FLOAT",
                        "description": "Total fibers cosnsumed.",
                    },
                    {
                        "name": "protein",
                        "type": "FLOAT",
                        "description": "Total proteins consumed.",
                    },
                    {
                        "name": "sodium",
                        "type": "FLOAT",
                        "description": "Total sodium consumed.",
                    },
                    {
                        "name": "water",
                        "type": "FLOAT",
                        "description": "Total water consumed",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    if len(nutrition_logs_list) > 0:

        try:

            bulk_nutrition_logs_df = pd.concat(nutrition_logs_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_nutrition_logs_df,
                destination_table=_tablename("nutrition_logs"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "is_favorite",
                        "type": "BOOLEAN",
                        "mode": "NULLABLE",
                        "description": "Total calories consumed.",
                    },
                    {
                        "name": "log_date",
                        "type": "DATE",
                        "mode": "NULLABLE",
                        "description": "Date of the food log.",
                    },
                    {
                        "name": "log_id",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                        "description": "Food log id.",
                    },
                    {
                        "name": "logged_food_access_level",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_amount",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_brand",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_calories",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_food_id",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_meal_type_id",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_name",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_unit_name",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_unit_plural",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "nutritional_values_calories",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "nutritional_values_carbs",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "nutritional_values_fat",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "nutritional_values_fiber",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "nutritional_values_protein",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "nutritional_values_sodium",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "logged_food_locale",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    if len(nutrition_goals_list) > 0:

        try:

            bulk_nutrition_goal_df = pd.concat(nutrition_goals_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_nutrition_goal_df,
                destination_table=_tablename("nutrition_goals"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "calories",
                        "type": "INTEGER",
                        "description": "The users set calorie goal",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    log.info("Nutrition Scope Loaded " + str(execution_time))

    fitbit_bp.storage.user = None
    return last_resp


#
# Heart Data
#
@bp.route("/fitbit_heart_rate_scope")
@token_required
def fitbit_heart_rate_scope():

    start = timeit.default_timer()
    global last_resp
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    user_list = fitbit_bp.storage.all_users()
    try:
        if request.headers["user"] in user_list:
            user_list = [request.headers["user"]]
    except (Exception):
        pass

    pd.set_option("display.max_columns", 500)

    # hr_zones_list = []
    hr_list = []

    for user in user_list:

        log.debug("user: %s", user)

        fitbit_bp.storage.user = user

        if fitbit_bp.session.token:
            del fitbit_bp.session.token

        """
        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("heart_rate_zones"),
                                     fitbit_bp.token['user_id'], "date", True))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:

                resp = fitbit.get(
                    "/1/user/-/activities/heart/date/" + pull_date.strftime("%Y-%m-%d") + "/1d/1sec.json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                hr_zones = resp.json()["activities-heart"][0]["value"]

                user_activity_zone = pd.DataFrame(
                    {
                        hr_zones["heartRateZones"][0]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_calories_out": hr_zones["heartRateZones"][0][
                            "caloriesOut"
                        ],
                        hr_zones["heartRateZones"][0]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_minutes": hr_zones["heartRateZones"][0]["minutes"],
                        hr_zones["heartRateZones"][0]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_min_hr": hr_zones["heartRateZones"][0]["min"],
                        hr_zones["heartRateZones"][0]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_max_hr": hr_zones["heartRateZones"][0]["max"],
                        hr_zones["heartRateZones"][1]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_calories_out": hr_zones["heartRateZones"][1][
                            "caloriesOut"
                        ],
                        hr_zones["heartRateZones"][1]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_minutes": hr_zones["heartRateZones"][1]["minutes"],
                        hr_zones["heartRateZones"][1]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_min_hr": hr_zones["heartRateZones"][1]["min"],
                        hr_zones["heartRateZones"][1]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_max_hr": hr_zones["heartRateZones"][1]["max"],
                        hr_zones["heartRateZones"][2]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_calories_out": hr_zones["heartRateZones"][2][
                            "caloriesOut"
                        ],
                        hr_zones["heartRateZones"][2]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_minutes": hr_zones["heartRateZones"][2]["minutes"],
                        hr_zones["heartRateZones"][2]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_min_hr": hr_zones["heartRateZones"][2]["min"],
                        hr_zones["heartRateZones"][2]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_max_hr": hr_zones["heartRateZones"][2]["max"],
                        hr_zones["heartRateZones"][3]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_calories_out": hr_zones["heartRateZones"][3][
                            "caloriesOut"
                        ],
                        hr_zones["heartRateZones"][3]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_minutes": hr_zones["heartRateZones"][3]["minutes"],
                        hr_zones["heartRateZones"][3]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_min_hr": hr_zones["heartRateZones"][3]["min"],
                        hr_zones["heartRateZones"][3]["name"]
                        .replace(" ", "_")
                        .lower()
                        + "_max_hr": hr_zones["heartRateZones"][3]["max"],
                    },
                    index=[0],
                )

                if user_activity_zone.empty and resp.status_code == 200:
                    user_activity_zone.append(pd.Series(dtype='float64'), ignore_index=True)
                user_activity_zone.insert(0, "id", fitbit_bp.token['user_id'])
                user_activity_zone.insert(1, "pull_date", datetime.now().strftime("%Y-%m-%d"))
                user_activity_zone.insert(2, "date", pull_date.strftime("%Y-%m-%d"))
                hr_zones_list.append(user_activity_zone)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break
        """

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("heart_rate"),
                                     fitbit_bp.token['user_id'], "datetime"))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:

                resp = fitbit.get(
                    "/1/user/-/activities/heart/date/" + pull_date.strftime("%Y-%m-%d") + "/1d/1sec.json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                hr_columns = ["time", "value", "datetime"]

                respj = resp.json()
                assert (
                    "activities-heart-intraday" in respj
                ), "no intraday heart rate data returned"
                heart_rate = respj["activities-heart-intraday"]["dataset"]
                heart_rate_df = pd.json_normalize(heart_rate)
                if heart_rate_df.empty and resp.status_code == 200:
                    heart_rate_df = heart_rate_df.append(pd.Series(dtype='float64'), ignore_index=True)
                    heart_rate_df["datetime"] = pd.to_datetime(
                        pull_date.strftime("%Y-%m-%d") + " " + datetime.min.time().strftime('%H:%M:%S')
                    )
                else:
                    heart_rate_df["datetime"] = pd.to_datetime(
                        pull_date.strftime("%Y-%m-%d") + " " + heart_rate_df["time"]
                    )
                heart_rate_df = _normalize_response(
                    heart_rate_df, hr_columns, fitbit_bp.token['user_id']
                )

                try:
                    heart_rate_df = heart_rate_df.drop(["time"], axis=1)
                except (Exception):
                    pass

                hr_list.append(heart_rate_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break
    # end loop over users

    # CONCAT DATAFRAMES INTO BULK DF #

    load_stop = timeit.default_timer()
    time_to_load = load_stop - start
    log.info("Heart Rate Zones " + str(time_to_load))

    # LOAD DATA INTO BIGQUERY #
    """
    if len(hr_zones_list) > 0:

        try:

            bulk_hr_zones_df = pd.concat(hr_zones_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_hr_zones_df,
                destination_table=_tablename("heart_rate_zones"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "description": "Date on which information was logged",
                    },
                    {
                        "name": "out_of_range_calories_out",
                        "type": "FLOAT",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "out_of_range_minutes",
                        "type": "INTEGER",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "out_of_range_min_hr",
                        "type": "INTEGER",
                        "description": "Minimum range for the heart rate zone.",
                    },
                    {
                        "name": "out_of_range_max_hr",
                        "type": "INTEGER",
                        "description": "Maximum range for the heart rate zone.",
                    },
                    {
                        "name": "fat_burn_calories_out",
                        "type": "FLOAT",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "fat_burn_minutes",
                        "type": "INTEGER",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "fat_burn_min_hr",
                        "type": "INTEGER",
                        "description": "Minimum range for the heart rate zone.",
                    },
                    {
                        "name": "fat_burn_max_hr",
                        "type": "INTEGER",
                        "description": "Maximum range for the heart rate zone.",
                    },
                    {
                        "name": "cardio_calories_out",
                        "type": "FLOAT",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "cardio_minutes",
                        "type": "INTEGER",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "cardio_min_hr",
                        "type": "INTEGER",
                        "description": "Minimum range for the heart rate zone.",
                    },
                    {
                        "name": "cardio_max_hr",
                        "type": "INTEGER",
                        "description": "Maximum range for the heart rate zone.",
                    },
                    {
                        "name": "peak_calories_out",
                        "type": "FLOAT",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "peak_minutes",
                        "type": "INTEGER",
                        "description": "Number calories burned with the specified heart rate zone.",
                    },
                    {
                        "name": "peak_min_hr",
                        "type": "INTEGER",
                        "description": "Minimum range for the heart rate zone.",
                    },
                    {
                        "name": "peak_max_hr",
                        "type": "INTEGER",
                        "description": "Maximum range for the heart rate zone.",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))
    """
    if len(hr_list) > 0:

        try:

            bulk_hr_intraday_df = pd.concat(hr_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_hr_intraday_df,
                destination_table=_tablename("heart_rate"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "datetime",
                        "type": "TIMESTAMP",
                    },
                    {
                        "name": "value",
                        "type": "INTEGER",
                    },
                    {"name": "date_time", "type": "TIMESTAMP"},
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    log.info("Heart Rate Scope Loaded " + str(execution_time))

    fitbit_bp.storage.user = None
    return last_resp


#
# Activity Data
#
@bp.route("/fitbit_activity_scope")
@token_required
def fitbit_activity_scope():

    start = timeit.default_timer()
    global last_resp
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

    user_list = fitbit_bp.storage.all_users()
    try:
        if request.headers["user"] in user_list:
            user_list = [request.headers["user"]]
    except (Exception):
        pass

    pd.set_option("display.max_columns", 500)

    activities_list = []
    activity_summary_list = []
    activity_goals_list = []

    for user in user_list:

        log.debug("user: %s", user)

        fitbit_bp.storage.user = user

        if fitbit_bp.session.token:
            del fitbit_bp.session.token

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("activity_summary"),
                                     fitbit_bp.token['user_id'], "date", True))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:

                resp = fitbit.get(
                    "/1/user/-/activities/date/" + pull_date.strftime("%Y-%m-%d") + ".json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                # subset response for activites, summary, and goals
                try:
                    activity_goals = resp.json()["goals"]
                    activity_goals["date"] = pull_date.strftime("%Y-%m-%d")
                    activity_goals_df = pd.json_normalize(activity_goals)
                    activity_goals_columns = [
                        "date",
                        "activeMinutes",
                        "caloriesOut",
                        "distance",
                        "floors",
                        "steps",
                    ]
                    activity_goals_df = _normalize_response(
                        activity_goals_df, activity_goals_columns, fitbit_bp.token['user_id']
                    )
                    activity_goals_list.append(activity_goals_df)
                except Exception:
                    # No data available
                    pass
                try:
                    activities = resp.json()["activities"]
                    activities_df = pd.json_normalize(activities)
                    # Define columns
                    activites_columns = [
                        "activityId",
                        "activityParentId",
                        "activityParentName",
                        "calories",
                        "description",
                        "distance",
                        "duration",
                        "hasActiveZoneMinutes",
                        "hasStartTime",
                        "isFavorite",
                        "lastModified",
                        "logId",
                        "name",
                        "startDate",
                        "startTime",
                        "steps",
                    ]
                    activities_df = _normalize_response(
                        activities_df, activites_columns, fitbit_bp.token['user_id']
                    )
                    activities_df["start_datetime"] = pd.to_datetime(
                        activities_df["start_date"] + " " + activities_df["start_time"]
                    )
                    activities_df = activities_df.drop(
                        ["start_date", "start_time", "last_modified"], axis=1
                    )
                    activities_list.append(activities_df)
                except KeyError:
                    # No data available
                    pass
                try:
                    try:
                        activity_summary = resp.json()["summary"]
                    except KeyError:
                        # No data available, provide dict with one row only (with date) to force creation for tracking
                        if resp.status_code == 200:
                            activity_summary = {"date": pull_date.strftime("%Y-%m-%d")}
                    activity_summary_df = pd.json_normalize(activity_summary)
                    try:
                        activity_summary_df = activity_summary_df.drop(
                            ["distances", "heartRateZones"], axis=1
                        )
                    except (Exception):
                        pass

                    activity_summary_columns = [
                        "date",
                        "activeScore",
                        "activityCalories",
                        "caloriesBMR",
                        "caloriesOut",
                        "elevation",
                        "fairlyActiveMinutes",
                        "floors",
                        "lightlyActiveMinutes",
                        "marginalCalories",
                        "restingHeartRate",
                        "sedentaryMinutes",
                        "steps",
                        "veryActiveMinutes",
                    ]

                    activity_summary_df = _normalize_response(
                        activity_summary_df, activity_summary_columns, fitbit_bp.token['user_id'],
                        pull_date.strftime("%Y-%m-%d"), "date", resp.status_code
                    )
                    activity_summary_df = activity_summary_df.assign(date=pull_date.strftime("%Y-%m-%d"))
                    activity_summary_list.append(activity_summary_df)
                except Exception as e:
                    log.exception("exception occured: %s", str(e))

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

    fitbit_stop = timeit.default_timer()
    fitbit_execution_time = fitbit_stop - start
    log.info("Activity Scope: " + str(fitbit_execution_time))

    if len(activities_list) > 0:

        try:

            bulk_activities_df = pd.concat(activities_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_activities_df,
                destination_table=_tablename("activity_logs"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "activity_id",
                        "type": "INTEGER",
                        "description": "The ID of the activity.",
                    },
                    {
                        "name": "activity_parent_id",
                        "type": "INTEGER",
                        "description": 'The ID of the top level ("parent") activity.',
                    },
                    {
                        "name": "activity_parent_name",
                        "type": "STRING",
                        "description": 'The name of the top level ("parent") activity.',
                    },
                    {
                        "name": "calories",
                        "type": "INTEGER",
                        "description": "Number of calories burned during the exercise.",
                    },
                    {
                        "name": "description",
                        "type": "STRING",
                        "description": "The description of the recorded exercise.",
                    },
                    {
                        "name": "distance",
                        "type": "FLOAT",
                        "description": "The distance traveled during the recorded exercise.",
                    },
                    {
                        "name": "duration",
                        "type": "INTEGER",
                        "description": "The activeDuration (milliseconds) + any pauses that occurred during the \
                            activity recording.",
                    },
                    {
                        "name": "has_active_zone_minutes",
                        "type": "BOOLEAN",
                        "description": "True | False",
                    },
                    {
                        "name": "has_start_time",
                        "type": "BOOLEAN",
                        "description": "True | False",
                    },
                    {
                        "name": "is_favorite",
                        "type": "BOOLEAN",
                        "description": "True | False",
                    },
                    {
                        "name": "log_id",
                        "type": "INTEGER",
                        "description": "The activity log identifier for the exercise.",
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "Name of the recorded exercise.",
                    },
                    {
                        "name": "start_datetime",
                        "type": "TIMESTAMP",
                        "description": "The start time of the recorded exercise.",
                    },
                    {
                        "name": "steps",
                        "type": "INTEGER",
                        "description": "User defined goal for daily step count.",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    if len(activity_summary_list) > 0:

        try:

            bulk_activity_summary_df = pd.concat(activity_summary_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_activity_summary_df,
                destination_table=_tablename("activity_summary"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "description": "The date information was logged",
                    },
                    {
                        "name": "activity_score",
                        "type": "INTEGER",
                        "description": "No Description",
                    },
                    {
                        "name": "activity_calories",
                        "type": "INTEGER",
                        "description": "The number of calories burned for the day during periods the user was active \
                            above sedentary level. This includes both activity burned calories and BMR.",
                    },
                    {
                        "name": "calories_bmr",
                        "type": "INTEGER",
                        "description": "Total BMR calories burned for the day.",
                    },
                    {
                        "name": "calories_out",
                        "type": "INTEGER",
                        "description": "Total calories burned for the day (daily timeseries total).",
                    },
                    {
                        "name": "elevation",
                        "type": "INTEGER",
                        "description": "The elevation traveled for the day.",
                    },
                    {
                        "name": "fairly_active_minutes",
                        "type": "INTEGER",
                        "description": "Total minutes the user was fairly/moderately active.",
                    },
                    {
                        "name": "floors",
                        "type": "INTEGER",
                        "description": "The equivalent floors climbed for the day.",
                    },
                    {
                        "name": "lightly_active_minutes",
                        "type": "INTEGER",
                        "description": "	Total minutes the user was lightly active.",
                    },
                    {
                        "name": "marginal_calories",
                        "type": "INTEGER",
                        "description": "Total marginal estimated calories burned for the day.",
                    },
                    {
                        "name": "resting_heart_rate",
                        "type": "INTEGER",
                        "description": "The resting heart rate for the day",
                    },
                    {
                        "name": "sedentary_minutes",
                        "type": "INTEGER",
                        "description": "Total minutes the user was sedentary.",
                    },
                    {
                        "name": "very_active_minutes",
                        "type": "INTEGER",
                        "description": "Total minutes the user was very active.",
                    },
                    {
                        "name": "steps",
                        "type": "INTEGER",
                        "description": "Total steps taken for the day.",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    if len(activity_goals_list) > 0:

        try:

            bulk_activity_goals_df = pd.concat(activity_goals_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_activity_goals_df,
                destination_table=_tablename("activity_goals"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "description": "The date information was logged"
                    },
                    {
                        "name": "active_minutes",
                        "type": "INTEGER",
                        "description": "User defined goal for daily active minutes.",
                    },
                    {
                        "name": "calories_out",
                        "type": "INTEGER",
                        "description": "User defined goal for daily calories burned.",
                    },
                    {
                        "name": "distance",
                        "type": "FLOAT",
                        "description": "User defined goal for daily distance traveled.",
                    },
                    {
                        "name": "floors",
                        "type": "INTEGER",
                        "description": "User defined goal for daily floor count.",
                    },
                    {
                        "name": "steps",
                        "type": "INTEGER",
                        "description": "User defined goal for daily step count.",
                    },
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    log.info("Activity Scope Loaded: " + str(execution_time))

    fitbit_bp.storage.user = None
    return last_resp


#
# Intraday Data
#
@bp.route("/fitbit_intraday_scope")
@token_required
def fitbit_intraday_scope():

    start = timeit.default_timer()
    global last_resp
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    user_list = fitbit_bp.storage.all_users()
    try:
        if request.headers["user"] in user_list:
            user_list = [request.headers["user"]]
    except (Exception):
        pass

    pd.set_option("display.max_columns", 500)

    intraday_steps_list = []
    # intraday_calories_list = []
    intraday_distance_list = []
    intraday_elevation_list = []
    intraday_floors_list = []

    for user in user_list:

        log.debug("user: %s", user)

        fitbit_bp.storage.user = user

        if fitbit_bp.session.token:
            del fitbit_bp.session.token

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("intraday_steps"),
                                     fitbit_bp.token['user_id'], "date_time"))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:

                resp = fitbit.get(
                    "/1/user/-/activities/steps/date/"
                    + pull_date.strftime("%Y-%m-%d")
                    + "/1d/1min.json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                intraday_steps = resp.json()["activities-steps-intraday"]["dataset"]
                intraday_steps_df = pd.json_normalize(intraday_steps)
                intraday_steps_columns = ["time", "value"]
                intraday_steps_df = _normalize_response(
                    intraday_steps_df, intraday_steps_columns, fitbit_bp.token['user_id']
                )
                intraday_steps_df["date_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + intraday_steps_df["time"]
                )
                intraday_steps_df = intraday_steps_df.drop(["time"], axis=1)
                intraday_steps_list.append(intraday_steps_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

        """
        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("intraday_calories"),
                                     fitbit_bp.token['user_id'], "date_time"))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:
                #
                # CALORIES
                resp = fitbit.get(
                    "/1/user/-/activities/calories/date/"
                    + pull_date.strftime("%Y-%m-%d")
                    + "/1d/1min.json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                intraday_calories = resp.json()["activities-calories-intraday"][
                    "dataset"
                ]
                intraday_calories_df = pd.json_normalize(intraday_calories)
                intraday_calories_columns = ["level", "mets", "time", "value"]
                intraday_calories_df = _normalize_response(
                    intraday_calories_df,
                    intraday_calories_columns,
                    fitbit_bp.token['user_id'],
                )
                intraday_calories_df["date_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + intraday_calories_df["time"]
                )
                intraday_calories_df = intraday_calories_df.drop(["time"], axis=1)
                intraday_calories_list.append(intraday_calories_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break
        """

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("intraday_distances"),
                                     fitbit_bp.token['user_id'], "date_time"))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:
                # DISTANCE
                resp = fitbit.get(
                    "/1/user/-/activities/distance/date/"
                    + pull_date.strftime("%Y-%m-%d")
                    + "/1d/1min.json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                intraday_distance = resp.json()["activities-distance-intraday"][
                    "dataset"
                ]
                intraday_distance_df = pd.json_normalize(intraday_distance)
                intraday_calories_columns = ["time", "value"]
                intraday_distance_df = _normalize_response(
                    intraday_distance_df,
                    intraday_calories_columns,
                    fitbit_bp.token['user_id'],
                )
                intraday_distance_df["date_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + intraday_distance_df["time"]
                )
                intraday_distance_df = intraday_distance_df.drop(["time"], axis=1)
                intraday_distance_list.append(intraday_distance_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("intraday_elevation"),
                                     fitbit_bp.token['user_id'], "date_time"))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:
                # ELEVATION
                resp = fitbit.get(
                    "/1/user/-/activities/elevation/date/"
                    + pull_date.strftime("%Y-%m-%d")
                    + "/1d/1min.json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                intraday_elevation = resp.json()["activities-elevation-intraday"][
                    "dataset"
                ]
                intraday_elevation_df = pd.json_normalize(intraday_elevation)
                intraday_elevation_columns = ["time", "value"]
                intraday_elevation_df = _normalize_response(
                    intraday_elevation_df,
                    intraday_elevation_columns,
                    fitbit_bp.token['user_id'],
                )
                intraday_elevation_df["date_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + intraday_elevation_df["time"]
                )
                intraday_elevation_df = intraday_elevation_df.drop(["time"], axis=1)
                intraday_elevation_list.append(intraday_elevation_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("intraday_floors"),
                                     fitbit_bp.token['user_id'], "date_time"))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:
                # FLOORS
                resp = fitbit.get(
                    "/1/user/-/activities/floors/date/"
                    + pull_date.strftime("%Y-%m-%d")
                    + "/1d/1min.json"
                )
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                intraday_floors = resp.json()["activities-floors-intraday"][
                    "dataset"
                ]
                intraday_floors_df = pd.json_normalize(intraday_floors)
                intraday_floors_columns = ["time", "value"]
                intraday_floors_df = _normalize_response(
                    intraday_floors_df, intraday_floors_columns, fitbit_bp.token['user_id']
                )
                intraday_floors_df["date_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + intraday_floors_df["time"]
                )
                intraday_floors_df = intraday_floors_df.drop(["time"], axis=1)
                intraday_floors_list.append(intraday_floors_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

    # end loop over users

    fitbit_stop = timeit.default_timer()
    fitbit_execution_time = fitbit_stop - start
    log.info("Intraday Scope: " + str(fitbit_execution_time))

    if len(intraday_steps_list) > 0:

        try:

            bulk_intraday_steps_df = pd.concat(intraday_steps_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_intraday_steps_df,
                destination_table=_tablename("intraday_steps"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "value",
                        "type": "INTEGER",
                        "description": "Number of steps at this time",
                    },
                    {
                        "name": "date_time",
                        "type": "TIMESTAMP",
                        "description": "Time of day",
                    },
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    """
    if len(intraday_calories_list) > 0:

        try:

            bulk_intraday_calories_df = pd.concat(
                intraday_calories_list, axis=0
            )

            pandas_gbq.to_gbq(
                dataframe=bulk_intraday_calories_df,
                destination_table=_tablename("intraday_calories"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {"name": "level", "type": "INTEGER"},
                    {
                        "name": "mets",
                        "type": "INTEGER",
                        "description": "METs value at the moment when the resource was recorded.",
                    },
                    {
                        "name": "value",
                        "type": "FLOAT",
                        "description": "The specified resource's value at the time it is recorded.",
                    },
                    {
                        "name": "date_time",
                        "type": "TIMESTAMP",
                        "description": "Time of day",
                    },
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))
    """

    if len(intraday_distance_list) > 0:

        try:

            bulk_intraday_distance_df = pd.concat(
                intraday_distance_list, axis=0
            )

            pandas_gbq.to_gbq(
                dataframe=bulk_intraday_distance_df,
                destination_table=_tablename("intraday_distances"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "value",
                        "type": "FLOAT",
                        "description": "The specified resource's value at the time it is recorded.",
                    },
                    {
                        "name": "date_time",
                        "type": "TIMESTAMP",
                        "description": "Time of day",
                    },
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    if len(intraday_elevation_list) > 0:

        try:

            bulk_intraday_elevation_df = pd.concat(
                intraday_elevation_list, axis=0
            )

            pandas_gbq.to_gbq(
                dataframe=bulk_intraday_elevation_df,
                destination_table=_tablename("intraday_elevation"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "value",
                        "type": "FLOAT",
                        "description": "The specified resource's value at the time it is recorded.",
                    },
                    {
                        "name": "date_time",
                        "type": "TIMESTAMP",
                        "description": "Time of day",
                    },
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    if len(intraday_floors_list) > 0:

        try:

            bulk_intraday_floors_df = pd.concat(intraday_floors_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_intraday_floors_df,
                destination_table=_tablename("intraday_floors"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "value",
                        "type": "FLOAT",
                        "description": "The specified resource's value at the time it is recorded.",
                    },
                    {
                        "name": "date_time",
                        "type": "TIMESTAMP",
                        "description": "Time of day",
                    },
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    log.info("Intraday Scope Loaded: " + str(execution_time))

    fitbit_bp.storage.user = None
    return last_resp


#
# Sleep Data
#
@bp.route("/fitbit_sleep_scope")
@token_required
def fitbit_sleep_scope():
    # TODO: Sleep endpoint v1 is deprecated, migrate to 1.2
    # https://dev.fitbit.com/build/reference/web-api/sleep-v1/
    start = timeit.default_timer()
    global last_resp
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    user_list = fitbit_bp.storage.all_users()
    try:
        if request.headers["user"] in user_list:
            user_list = [request.headers["user"]]
    except (Exception):
        pass

    pd.set_option("display.max_columns", 500)

    sleep_list = []
    sleep_summary_list = []
    # sleep_minutes_list = []

    for user in user_list:

        log.debug("user: %s", user)

        fitbit_bp.storage.user = user

        if fitbit_bp.session.token:
            del fitbit_bp.session.token

        # loop over last synced date in bigquery to now or date specified by user
        last_sync = request.args.get("date", _last_date_in_bq(_tablename("sleep_summary"),
                                     fitbit_bp.token['user_id'], "date", True))
        date_list = [last_sync + timedelta(days=x) for x in range((datetime.now()-last_sync).days)]

        for pull_date in date_list:
            try:

                resp = fitbit.get("/1/user/-/sleep/date/" + pull_date.strftime("%Y-%m-%d") + ".json")
                last_resp = resp.status_code

                log.debug("%s: %d [%s]", resp.url, resp.status_code, resp.reason)

                sleep = resp.json()["sleep"]

                """
                if "minuteData" in resp.json().keys():
                    sleep_minutes = resp.json()["sleep"][0]["minuteData"]
                    sleep_minutes_df = pd.json_normalize(sleep_minutes)
                    sleep_minutes_columns = ["dateTime", "value"]
                    sleep_minutes_df = _normalize_response(
                        sleep_minutes_df, sleep_minutes_columns, fitbit_bp.token['user_id']
                    )
                else:
                    cols = ["dateTime", "value"]
                    sleep_minutes_df = pd.DataFrame(columns=cols)
                    sleep_minutes_df = _normalize_response(
                        sleep_minutes_df, cols, fitbit_bp.token['user_id']
                    )
                sleep_minutes_df["date_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + sleep_minutes_df["date_time"]
                )
                sleep_minutes_list.append(sleep_minutes_df)
                """

                sleep_summary = resp.json()["summary"]
                sleep_summary["date"] = pull_date.strftime("%Y-%m-%d")
                sleep_df = pd.json_normalize(sleep)
                sleep_summary_df = pd.json_normalize(sleep_summary)

                try:
                    sleep_df = sleep_df.drop(["minuteData"], axis=1)
                except (Exception):
                    pass

                sleep_columns = [
                    "awakeCount",
                    "awakeDuration",
                    "awakeningsCount",
                    "dateOfSleep",
                    "duration",
                    "efficiency",
                    "endTime",
                    "isMainSleep",
                    "logId",
                    "minutesAfterWakeup",
                    "minutesAsleep",
                    "minutesAwake",
                    "minutesToFallAsleep",
                    "restlessCount",
                    "restlessDuration",
                    "startTime",
                    "timeInBed",
                ]
                sleep_summary_columns = [
                    "date",
                    "totalMinutesAsleep",
                    "totalSleepRecords",
                    "totalTimeInBed",
                    "stages.deep",
                    "stages.light",
                    "stages.rem",
                    "stages.wake",
                ]

                # Fill missing columns
                sleep_df = _normalize_response(
                    sleep_df, sleep_columns, fitbit_bp.token['user_id']
                )
                sleep_df["end_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + sleep_df["end_time"]
                )
                sleep_df["start_time"] = pd.to_datetime(
                    pull_date.strftime("%Y-%m-%d") + " " + sleep_df["start_time"]
                )

                sleep_summary_df = _normalize_response(
                    sleep_summary_df, sleep_summary_columns, fitbit_bp.token['user_id']
                )

                # Append dfs to df list
                sleep_list.append(sleep_df)
                sleep_summary_list.append(sleep_summary_df)

            except (Exception) as e:
                log.exception("exception occured: %s", str(e))

            if resp.status_code == 429:
                break

    # end loop over users

    fitbit_stop = timeit.default_timer()
    fitbit_execution_time = fitbit_stop - start
    log.info("Sleep Scope: " + str(fitbit_execution_time))

    if len(sleep_list) > 0:

        try:

            bulk_sleep_df = pd.concat(sleep_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_sleep_df,
                destination_table=_tablename("sleep"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "awake_count",
                        "type": "INTEGER",
                        "description": "Number of times woken up",
                    },
                    {
                        "name": "awake_duration",
                        "type": "INTEGER",
                        "description": "Amount of time the user was awake",
                    },
                    {
                        "name": "awakenings_count",
                        "type": "INTEGER",
                        "description": "Number of times woken up",
                    },
                    {
                        "name": "date_of_sleep",
                        "type": "DATE",
                        "description": "The date the user fell asleep",
                    },
                    {
                        "name": "duration",
                        "type": "INTEGER",
                        "description": "Length of the sleep in milliseconds.",
                    },
                    {
                        "name": "efficiency",
                        "type": "INTEGER",
                        "description": "Calculated sleep efficiency score. This is not the sleep score available in \
                            the mobile application.",
                    },
                    {
                        "name": "end_time",
                        "type": "TIMESTAMP",
                        "description": "Time the sleep log ended.",
                    },
                    {
                        "name": "is_main_sleep",
                        "type": "BOOLEAN",
                        "decription": "True | False",
                    },
                    {
                        "name": "log_id",
                        "type": "INTEGER",
                        "description": "Sleep log ID.",
                    },
                    {
                        "name": "minutes_after_wakeup",
                        "type": "INTEGER",
                        "description": "The total number of minutes after the user woke up.",
                    },
                    {
                        "name": "minutes_asleep",
                        "type": "INTEGER",
                        "description": "The total number of minutes the user was asleep.",
                    },
                    {
                        "name": "minutes_awake",
                        "type": "INTEGER",
                        "description": "The total number of minutes the user was awake.",
                    },
                    {
                        "name": "minutes_to_fall_asleep",
                        "type": "INTEGER",
                        "decription": "The total number of minutes before the user falls asleep. This value is \
                            generally 0 for autosleep created sleep logs.",
                    },
                    {
                        "name": "restless_count",
                        "type": "INTEGER",
                        "decription": "The total number of times the user was restless",
                    },
                    {
                        "name": "restless_duration",
                        "type": "INTEGER",
                        "decription": "The total amount of time the user was restless",
                    },
                    {
                        "name": "start_time",
                        "type": "TIMESTAMP",
                        "description": "Time the sleep log begins.",
                    },
                    {
                        "name": "time_in_bed",
                        "type": "INTEGER",
                        "description": "Total number of minutes the user was in bed.",
                    },
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    """
    if len(sleep_minutes_list) > 0:

        try:

            bulk_sleep_minutes_df = pd.concat(sleep_minutes_list, axis=0)
            bulk_sleep_minutes_df["value"] = bulk_sleep_minutes_df[
                "value"
            ].astype(int)

            pandas_gbq.to_gbq(
                dataframe=bulk_sleep_minutes_df,
                destination_table=_tablename("sleep_minutes"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {"name": "date_time", "type": "TIMESTAMP"},
                    {"name": "value", "type": "INTEGER"},
                ],
            )

        except (Exception) as e:
            log.exception("exception occured: %s", str(e))
    """

    if len(sleep_summary_list) > 0:

        try:

            bulk_sleep_summary_df = pd.concat(sleep_summary_list, axis=0)

            pandas_gbq.to_gbq(
                dataframe=bulk_sleep_summary_df,
                destination_table=_tablename("sleep_summary"),
                project_id=project_id,
                if_exists="append",
                progress_bar=False,
                table_schema=[
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "REQUIRED",
                        "description": "Primary Key",
                    },
                    {
                        "name": "pull_date",
                        "type": "DATE",
                        "mode": "REQUIRED",
                        "description": "The date values were extracted",
                    },
                    {
                        "name": "date",
                        "type": "DATE",
                        "description": "The date information was logged"
                    },
                    {
                        "name": "total_minutes_asleep",
                        "type": "INTEGER",
                        "description": "Total number of minutes the user was asleep across all sleep records in the \
                            sleep log.",
                    },
                    {
                        "name": "total_sleep_records",
                        "type": "INTEGER",
                        "description": "The number of sleep records within the sleep log.",
                    },
                    {
                        "name": "total_time_in_bed",
                        "type": "INTEGER",
                        "description": "Total number of minutes the user was in bed across all records in the sleep \
                            log.",
                    },
                    {
                        "name": "stages_deep",
                        "type": "INTEGER",
                        "description": "Total time of deep sleep",
                    },
                    {
                        "name": "stages_light",
                        "type": "INTEGER",
                        "description": "Total time of light sleep",
                    },
                    {
                        "name": "stages_rem",
                        "type": "INTEGER",
                        "description": "Total time of REM sleep",
                    },
                    {
                        "name": "stages_wake",
                        "type": "INTEGER",
                        "description": "Total time awake",
                    },
                ],
            )
        except (Exception) as e:
            log.exception("exception occured: %s", str(e))

    stop = timeit.default_timer()
    execution_time = stop - start
    log.info("Sleep Scope Loaded: " + str(execution_time))

    fitbit_bp.storage.user = None
    return last_resp
