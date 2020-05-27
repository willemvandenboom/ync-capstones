WITH
  fio2 AS (
  SELECT
    DISTINCT rp.patientunitstayid,
    CASE
      WHEN CAST(respchartvalue AS numeric) > 0 AND CAST(respchartvalue AS numeric) <= 1 THEN CAST(respchartvalue AS numeric) * 100
    -- improperly input data - looks like O2 flow in litres
      WHEN CAST(respchartvalue AS numeric) > 1
    AND CAST(respchartvalue AS numeric) < 21 THEN NULL
      WHEN CAST(respchartvalue AS numeric) >= 21 AND CAST(respchartvalue AS numeric) <= 100 THEN CAST(respchartvalue AS numeric)
    ELSE
    NULL
  END
    -- unphysiological
    AS fio2,
    -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
    rp.respchartoffset AS chartoffset
  FROM
    `physionet-data.eicu_crd.respiratorycharting` rp
  WHERE
    respchartvalue <> ''
    AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
  ORDER BY
    patientunitstayid)
SELECT
  c.patientunitstayid,
  c.vent_start,
  c.vent_end,
  f.fio2,
  f.chartoffset,
  -- we select all sf values taken during the mechanical ventilation period along with their timestamps
FROM
  `ync-capstones.NMB_eICU.COHORT_BASELINE_P2` c
LEFT OUTER JOIN
  fio2 f
ON
  c.patientunitstayid = f.patientunitstayid
WHERE
  c.vent_start <= f.chartoffset
  AND c.vent_end >= f.chartoffset
  AND fio2 IS NOT NULL
ORDER BY
  c.patientunitstayid