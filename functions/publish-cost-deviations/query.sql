SELECT
  *
FROM (
  SELECT
    project_id,
    cost_current + discount_current AS net_current_cost,
    cost_lag + discount_lag AS net_lag_cost,
    ((cost_current + discount_current) - (cost_lag + discount_lag)) / NULLIF((cost_lag + discount_lag), 0) * 100 AS change
  FROM (
    SELECT
      project.id AS project_id,
      SUM(CASE WHEN DATE(usage_start_time) = current_date() - 1 THEN cost ELSE 0 END) AS cost_current,
      SUM(CASE WHEN DATE(usage_start_time) = current_date() - 1 THEN c.amount ELSE 0 END) AS discount_current,
      SUM(CASE WHEN DATE(usage_start_time) BETWEEN (current_date() - 8) AND (current_date() - 2) THEN cost ELSE 0 END) / 7 AS cost_lag,
      SUM(CASE WHEN DATE(usage_start_time) BETWEEN (current_date() - 8) AND (current_date() - 2) THEN c.amount ELSE 0 END) / 7 AS discount_lag
    FROM $DATASET
      LEFT JOIN UNNEST(credits) as c
    WHERE
      DATE(usage_start_time) >= current_date() - 8
    GROUP BY
      project.id
    )
  )
WHERE
  change >= 150
  AND net_current_cost >= 0.10;
