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
