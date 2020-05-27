SELECT
  c.subject_id,
  c.hadm_id,
  c.icustay_id,
  c.mv_durations_hours,
  c.mv_starttime,
  c.mv_endtime,
  s.Spo2,
  DATETIME(s.Spo2_charttime) AS Spo2_charttime,
  -- we select all sf ratios taken during the mechanical ventilation period along with their timestamps
FROM
  `ync-capstones.NMB.COHORT_BASELINE_P2` c
LEFT OUTER JOIN
  `ync-capstones.NMB.SPO2` s
ON
  c.hadm_id = s.hadm_id
WHERE
  c.mv_starttime <= DATETIME(s.Spo2_charttime)
  AND c.mv_endtime >= DATETIME(s.Spo2_charttime)
ORDER BY
  c.subject_id,
  c.hadm_id,
  c.icustay_id
