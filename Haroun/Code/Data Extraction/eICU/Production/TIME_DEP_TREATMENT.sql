WITH
  NMBAs AS (
  SELECT
    *
  FROM
    `physionet-data.eicu_crd.infusiondrug`
  WHERE
    LOWER(drugname) LIKE "%tracu%" -- cisatracurium
    OR LOWER(drugname) LIKE "nim%" -- nimbex
    OR LOWER(drugname) LIKE "doxacu%" -- Doxacurium
    OR LOWER(drugname) LIKE "traciu%" --  Tracium (common misspelling)
    OR LOWER(drugname) LIKE "tracriu%" -- Tracrium
    )
SELECT
  c.patientunitstayid,
  c.vent_start,
  c.vent_end,
  d.infusionoffset,
  d.drugamount
FROM
  `NMB_eICU.COHORT_BASELINE_P2` c
LEFT OUTER JOIN
  NMBAs d
ON
  c.patientunitstayid = d.patientunitstayid
WHERE
  d.infusionoffset IS NOT NULL