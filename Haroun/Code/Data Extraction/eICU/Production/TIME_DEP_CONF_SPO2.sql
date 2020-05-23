WITH
  spo2 AS --spo2 from pivoted vital
  (
  SELECT
    patientunitstayid,
    spo2,
    chartoffset
  FROM
    `physionet-data.eicu_crd_derived.pivoted_vital` )
SELECT
  c.patientunitstayid,
  c.vent_start,
  c.vent_end,
  s.spo2,
  s.chartoffset,
  -- we select all sf values taken during the mechanical ventilation period along with their timestamps
FROM
  `ync-capstones.NMB_eICU.COHORT_BASELINE_P2` c
LEFT OUTER JOIN
  spo2 s
ON
  c.patientunitstayid = s.patientunitstayid
WHERE
  c.vent_start <= s.chartoffset
  AND c.vent_end >= s.chartoffset
  AND spo2 IS NOT NULL
ORDER BY
  c.patientunitstayid