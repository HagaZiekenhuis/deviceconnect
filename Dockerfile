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

FROM python:3.10-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

# Install production dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./
ENV FLASK_ENV=PRODUCTION

CMD if [ -n "$FILE_GOOGLE_APPLICATION_CREDENTIALS" ]; then echo $FILE_GOOGLE_APPLICATION_CREDENTIALS | base64 -i -d > /tmp/creds.json; export GOOGLE_APPLICATION_CREDENTIALS=/tmp/creds.json; fi && exec gunicorn --bind :5000 --workers 3 --threads 1 --timeout 0 app.main:app
