#!/usr/bin/env bash

# Copyright 2025 Versity Software
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# generate github-actions matrix for system.yml
set -euo pipefail

files=()
while IFS= read -r f; do
  file_without_header=${f/tests\/test_/}
  file_without_sh=${file_without_header/.sh/}
  run_set=${file_without_sh//_/-}
  if [ "$run_set" == "rest" ]; then
    files+=("rest-base")
    continue
  fi
  files+=("$run_set")
done < <(find tests -name 'test_*.sh')
echo "${files[*]}"
files_json="$(printf '%s\n' "${files[@]}" | jq -Rsc '
  split("\n")[:-1]
  | {
      include: map(
        . as $f
        | [
            { desc: ("Run " + $f + " (non-static bucket)"), RUN_SET: $f, RECREATE_BUCKETS: true, DELETE_BUCKETS_AFTER_TEST: "true", IAM_TYPE: "folder", BACKEND: "posix", AWS_REGION: "us-east-1"}
          ]
      )
      | add
    }
')"

idx=0
while IFS= read -r f; do
  if (( idx % 3 )); then
    iam="s3"
  else
    iam="folder"
  fi
  if (( idx % 4)); then
    region="us-west-1"
  else
    region="us-east-1"
  fi
  echo "$f, iam: $iam, region: $region"
done < <(find tests -name 'test_*')

echo "$files_json"