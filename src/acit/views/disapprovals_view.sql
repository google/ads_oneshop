-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Create the product disapprovals view
--
-- @param ${PROJECT_NAME} The GCP project name
-- @param ${DATASET_NAME} The BQ dataset name

CREATE OR REPLACE VIEW ${PROJECT_NAME}.${DATASET_NAME}.disapprovals AS
SELECT
  P.account_id,
  P.offer_id,
  item_level_issue.description AS disapproval_description,
  item_level_issue.detail AS disapproval_detail,
  A.* EXCEPT (merchant_id, offer_id),
FROM
  `${PROJECT_NAME}.${DATASET_NAME}.products` AS P,
  P.status.item_level_issues AS item_level_issue
INNER JOIN
  `${PROJECT_NAME}.${DATASET_NAME}.acit` AS A
  ON P.account_id = A.merchant_id AND P.offer_id = A.offer_id
WHERE
  item_level_issue.servability = 'disapproved'
  AND destination = 'Shopping';
