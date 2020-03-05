SELECT
  c.subject_id,
  c.hadm_id,
  c.icustay_id,
  c.mv_durations_hours,
  c.mv_starttime,
  c.mv_endtime,
  sf.SF_ratio,
  DATETIME(sf.charttime) AS sf_charttime,
  -- we select all sf ratios taken during the mechanical ventilation period along with their timestamps
FROM
  `ync-capstones.NMB.COHORT_BASELINE_P2` c
LEFT OUTER JOIN
  `ync-capstones.NMB.SF_RATIO` sf
ON
  c.hadm_id = sf.hadm_id
WHERE
  c.mv_starttime <= DATETIME(sf.charttime)
  AND c.mv_endtime >= DATETIME(sf.charttime)
ORDER BY
  c.subject_id,
  c.hadm_id,
  c.icustay_id