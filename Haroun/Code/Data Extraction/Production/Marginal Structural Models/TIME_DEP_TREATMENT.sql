SELECT DISTINCT * FROM (
  SELECT
    c.subject_id,
    c.hadm_id,
    c.icustay_id,
    c.mv_durations_hours,
    c.mv_starttime,
    c.mv_endtime,
    mv.amount AS NMBA_amount,
    mv.starttime AS NMBA_timestamp
    -- we select all PF ratios taken during the mechanical ventilation period along with their timestamps
  FROM
    `ync-capstones.NMB.COHORT_BASELINE_P2` c
  LEFT OUTER JOIN
    `ync-capstones.MIMIC3_V1_4.INPUTEVENTS_MV` mv
  ON
    c.subject_id = mv.subject_id
    AND c.hadm_id = mv.hadm_id
    AND c.icustay_id = mv.icustay_id
  WHERE
    mv.itemid IN (
    SELECT
      itemid
    FROM
      `ync-capstones.NMB.NMBAs` )
    AND mv.starttime >= c.mv_starttime -- N.B.: first mv is Metvavision and second mv is mechanical ventilation  

  UNION ALL

  SELECT
    c.subject_id,
    c.hadm_id,
    c.icustay_id,
    c.mv_durations_hours,
    c.mv_starttime,
    c.mv_endtime,
    cv.amount AS NMBA_amount,
    cv.charttime AS NMBA_timestamp
  FROM
    `ync-capstones.NMB.COHORT_BASELINE_P2` c
  LEFT OUTER JOIN
    `ync-capstones.MIMIC3_V1_4.INPUTEVENTS_CV` cv
  ON
    c.subject_id = cv.subject_id
    AND c.hadm_id = cv.hadm_id
    AND c.icustay_id = cv.icustay_id
  WHERE
    cv.itemid IN (
    SELECT
      itemid
    FROM
      `ync-capstones.NMB.NMBAs` )
    AND cv.charttime >= c.mv_starttime
    --       AND
    --         LOWER(cv.AMOUNTUOM) LIKE "mg" -- to ensure patients only took MNBAs in g
    --         AND COUNT(DISTINCT cv.AMOUNTUOM) <= 1 ) -- to ensure patients did not take another NMBA in a differenct uom
)
ORDER BY subject_id,	hadm_id, icustay_id