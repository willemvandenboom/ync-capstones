SELECT
  c.subject_id,
  c.hadm_id,
  c.icustay_id,
  c.mv_durations_hours,
  c.mv_starttime,
  c.mv_endtime,
  bgfd.pao2fio2,
  bgfd.charttime AS pf_charttime,
  -- we select all PF ratios taken during the mechanical ventilation period along with their timestamps
FROM
  `ync-capstones.NMB.COHORT_BASELINE_P2` c
LEFT OUTER JOIN
  `physionet-data.mimiciii_derived.bloodgasfirstdayarterial` bgfd
ON
  c.subject_id = bgfd.subject_id
  AND c.hadm_id = bgfd.hadm_id
  AND c.icustay_id = bgfd.icustay_id
WHERE
  c.mv_starttime <= bgfd.charttime
  AND c.mv_endtime >= bgfd.charttime
  AND bgfd.pao2fio2 IS NOT NULL
ORDER BY
  c.subject_id,
  c.hadm_id,
  c.icustay_id