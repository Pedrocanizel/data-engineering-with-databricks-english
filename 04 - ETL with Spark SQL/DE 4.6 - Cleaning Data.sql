

SELECT * FROM users_dirty



SELECT count(user_id), count(user_first_touch_timestamp), count(email), count(updated), count(*)
FROM users_dirty



SELECT
  count_if(user_id IS NULL) AS missing_user_ids, 
  count_if(user_first_touch_timestamp IS NULL) AS missing_timestamps, 
  count_if(email IS NULL) AS missing_emails,
  count_if(updated IS NULL) AS missing_updates
FROM users_dirty



SELECT count(DISTINCT(*))
FROM users_dirty


SELECT count(DISTINCT(user_id))
FROM users_dirty



SELECT count(DISTINCT(user_first_touch_timestamp))
FROM users_dirty


SELECT 
  count(user_id) AS total_ids,
  count(DISTINCT user_id) AS unique_ids,
  count(email) AS total_emails,
  count(DISTINCT email) AS unique_emails,
  count(updated) AS total_updates,
  count(DISTINCT(updated)) AS unique_updates,
  count(*) AS total_rows, 
  count(DISTINCT(*)) AS unique_non_null_rows
FROM users_dirty


CREATE OR REPLACE TEMP VIEW users_deduped AS
  SELECT DISTINCT(*) FROM users_dirty;

SELECT * FROM users_deduped



SELECT COUNT(*) FROM users_deduped



SELECT * FROM users_dirty
WHERE
  user_id IS NULL AND
  user_first_touch_timestamp IS NULL AND
  email IS NULL AND
  updated IS NULL



SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL



CREATE OR REPLACE TEMP VIEW deduped_users AS
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users



SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)


SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)


SELECT *,
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-
