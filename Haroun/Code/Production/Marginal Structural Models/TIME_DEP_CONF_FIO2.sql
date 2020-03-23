SELECT
  c.subject_id,
  c.hadm_id,
  c.icustay_id,
  c.mv_durations_hours,
  c.mv_starttime,
  c.mv_endtime,
  f.Fio2,
  DATETIME(f.Fio2_charttime) AS Fio2_charttime,
  -- we select all sf ratios taken during the mechanical ventilation period along with their timestamps
FROM
  `ync-capstones.NMB.COHORT_BASELINE_P2` c
LEFT OUTER JOIN
  `ync-capstones.NMB.FIO2` f
ON
  c.hadm_id = f.hadm_id
WHERE
  c.mv_starttime <= DATETIME(f.Fio2_charttime)
  AND c.mv_endtime >= DATETIME(f.Fio2_charttime)
ORDER BY
  c.subject_id,
  c.hadm_id,
  c.icustay_id
