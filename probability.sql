CREATE TABLE dm.ttd_visit_probability (
  card12 STRING,
  plant STRING,
  total_visits_90days integer,
  avg_daily_visits float,
  last_purchase_date date,
  days_since_last_purchase integer,
  probability float,
  report_date date
)
PARTITIONED BY SPEC ( report_date )
STORED AS ICEBERG
TBLPROPERTIES (
  'format-version'='2'
);

--единичный расчет - 21 секунда

INSERT INTO dm.ttd_visit_probability
SELECT
    card12,
    plant,
    CAST(SUM(rt_norecre) AS integer) AS total_visits_90days,
    CASE
      WHEN SUM(rt_norecre) / 90 > 1 THEN 1
      ELSE SUM(rt_norecre) / 90
    END AS avg_daily_visits,   -- средняя частота посещений в день
    MAX(calday) AS last_purchase_date,
    DATEDIFF(CURRENT_DATE(), MAX(calday)) AS days_since_last_purchase,
    -- Расчет вероятности по формуле P = 1 - (1 - Ā)^D
    1 - POWER(
			1 - (SUM(rt_norecre) / 90),
            DATEDIFF(CURRENT_DATE(), MAX(calday))
		) AS probability,
	CURRENT_DATE() AS report_date,
FROM
    ttd_pos_rec_itm
WHERE
    calday >= DATE_ADD(CURRENT_DATE(), -90)
    AND calday < CURRENT_DATE()
GROUP BY
    card12,
    plant
HAVING
    sum(rt_norecre) < 180;