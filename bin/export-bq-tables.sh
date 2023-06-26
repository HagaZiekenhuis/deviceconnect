#!/bin/sh

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

DATASET=${1:-fitbit} 
GCSBUCKET=${2:-starterkit-export}

for x in `bq  --format json ls fitbit | jq '.[] | .id' | cut -d\" -f 2 | cut -d. -f 2`; 
  do echo "*** exporting ${DATASET}.$x ***";
  bq extract --compression GZIP ${DATASET}.$x gs://${GCSBUCKET}/${DATASET}.$x.gz; 
done

