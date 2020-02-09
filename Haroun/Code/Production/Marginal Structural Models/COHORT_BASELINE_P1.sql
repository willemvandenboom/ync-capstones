WITH
  all_ICU_stays AS  -- all ICU stays
  (
  SELECT
    DISTINCT p.subject_id,
    a.hadm_id,
    i.icustay_id,
    TRUNC(DATE_DIFF(DATE(a.admittime), DATE(p.dob),MONTH)/12,0) AS age,
    a.admittime,
    a.dischtime,
    i.intime,
    i.outtime,
    los,
    rank () OVER (PARTITION BY a.hadm_id ORDER BY i.intime) AS icu_stay_rank
  FROM
    `ync-capstones.MIMIC3_V1_4.PATIENTS` p,
    `ync-capstones.MIMIC3_V1_4.ADMISSIONS` a,
    `ync-capstones.MIMIC3_V1_4.ICUSTAYS` i
  WHERE
    p.subject_id=a.subject_id
    AND p.subject_id=i.subject_id
    AND a.hadm_id=i.hadm_id
    -- order by p.subject_id, a.hadm_id, i.icustay_id
    ),
  ----------------------------------------------------------------------------------------------------------------
  all_ICU_stays_2 AS  -- exclude patients younger than 16yo and patients who stayed in ICU less than 12 hours
  (
  SELECT
    DISTINCT subject_id,
    hadm_id,
    icustay_id,
    age,
    intime,
    outtime,
    los,
    icu_stay_rank
  FROM
    all_ICU_stays
  WHERE
    (age >= 16
      AND los*24 >= 12 ) -- los uom is fraction of days
  GROUP BY
    subject_id,
    hadm_id,
    icustay_id,
    icu_stay_rank,
    age,
    los,
    intime,
    outtime
  ORDER BY
    subject_id,
    hadm_id,
    icustay_id,
    icu_stay_rank,
    age,
    los,
    intime,
    outtime ),
  ----------------------------------------------------------------------------------------------------------------
  all_ICU_stays_3 AS -- more information added about the icustay and the patient
  (
  SELECT
    ie.subject_id,
    ie.hadm_id,
    ie.icustay_id,
    ie.intime,
    ie.outtime,
    ie.los,
    ie.age,
    p.gender,
    p.dob,
    h.Height,
  IF
    (w.Weight IS NULL,
    IF
      (w.Weight_Admit IS NULL,
      IF
        (w.Weight_Daily IS NULL,
        IF
          (w.Weight_EchoInHosp IS NULL,
          IF
            (w.Weight_EchoPreHosp IS NULL,
              NULL,
              w.Weight_EchoPreHosp),
            w.Weight_EchoInHosp),
          w.Weight_Daily),
        w.Weight_Admit),
      w.Weight) AS weight,
    dod,
    a.admission_type,
    a.admission_location,
    a.insurance,
    a.LANGUAGE,
    a.religion,
    a.marital_status,
    a.ethnicity,
    a.diagnosis,
    a.hospital_expire_flag,
    ie.icu_stay_rank
  FROM
    all_ICU_stays_2 ie
  LEFT JOIN
    `ync-capstones.MIMIC3_V1_4.PATIENTS` p
  ON
    ie.subject_id = p.subject_id
  LEFT JOIN
    `ync-capstones.MIMIC3_V1_4.ADMISSIONS` a
  ON
    a.subject_id = ie.subject_id
    AND a.hadm_id = ie.hadm_id
  LEFT JOIN
    `ync-capstones.MIMIC_V1_4_derived.heightfirstday` h
  ON
    h.icustay_id = ie.icustay_id
  LEFT JOIN
    `ync-capstones.MIMIC_V1_4_derived.weightfirstday` w
  ON
    w.icustay_id = ie.icustay_id
    --group by subject_id, hadm_id, icustay_id, intime, outtime, age, los
    --order by subject_id, hadm_id, icustay_id, intime, outtime, age, los
    ),
  ----------------------------------------------------------------------------------------------------------------
  first_ICU_stay AS -- select only first ICU stay from all_ICU_stays_3 within the same hadm_id
  (
  SELECT
    DISTINCT subject_id,
    hadm_id,
    icustay_id,
    age,
    intime,
    outtime,
    los,
    dod,
    weight,
    height,
    admission_type,
    admission_location,
    LANGUAGE,
    religion,
    marital_Status,
    ethnicity,
    diagnosis
  FROM
    all_ICU_stays_3
  GROUP BY
    subject_id,
    hadm_id,
    icustay_id,
    intime,
    outtime,
    age,
    los,
    dod,
    weight,
    height,
    admission_type,
    admission_location,
    LANGUAGE,
    religion,
    marital_Status,
    ethnicity,
    diagnosis,
    icu_stay_rank
  HAVING
    icu_stay_rank = 1
  ORDER BY
    subject_id,
    hadm_id,
    icustay_id,
    intime,
    outtime,
    age,
    los,
    dod ),
  ----------------------------------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------------------------------
  mech_venti_on_first_ICU_stay AS -- patients who have been on mech vent
  (
  SELECT
    DISTINCT a.subject_id,
    a.hadm_id,
    a.icustay_id,
    b.duration_hours AS mv_durations_hours,
    b.starttime AS mv_starttime,
    b.endtime AS mv_endtime,
    a.dod,
    a.age,
    a.intime,
    a.outtime,
    a.los,
    a.weight AS weight,
    a.height AS height,
    a.admission_type,
    a.admission_location,
    a.LANGUAGE,
    a.religion,
    a.marital_status,
    a.ethnicity,
    a.diagnosis
  FROM
    first_ICU_stay a
  INNER JOIN
    `ync-capstones.MIMIC_V1_4_derived.ventdurations` b
  ON
    a.icustay_id = b.icustay_id
  WHERE
    b.ventnum = 1
  ORDER BY
    subject_id,
    hadm_id,
    icustay_id,
    dod,
    intime,
    outtime,
    age,
    los),
  ----------------------------------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------------------------------
  pao2fio2_PEEP_within_24_hours_of_intubation AS
  -- pao2fio2 ratio taken from derived table bloodgasfirstdayarterial and is within 24 hours of the first mechanical ventialtion of the first ICU stay
  -- PEEP taken from chartime
  (
  SELECT
    mv.subject_id,
    mv.hadm_id,
    mv.icustay_id,
    mv.mv_durations_hours,
    mv.mv_starttime,
    mv.mv_endtime,
    bgfd.pao2fio2,
    bgfd.charttime AS pf_charttime,
    c.valuenum AS peep,
    c.charttime AS peep_charttime,
    mv.age,
    mv.dod,
    mv.los,
    mv.weight,
    mv.height,
    ROUND(weight/(height*height/10000),1) AS BMI,
    mv.admission_type,
    mv.admission_location,
    mv.LANGUAGE,
    mv.religion,
    mv.marital_status,
    mv.ethnicity,
    mv.diagnosis
  FROM
    mech_venti_on_first_ICU_stay mv
  LEFT OUTER JOIN
    `ync-capstones.MIMIC_V1_4_derived.bloodgasfirstdayarterial` bgfd
  ON
    mv.subject_id = bgfd.subject_id
    AND mv.hadm_id = bgfd.hadm_id
    AND mv.icustay_id = bgfd.icustay_id
  LEFT OUTER JOIN
    `ync-capstones.MIMIC3_V1_4.CHARTEVENTS` c
  ON
    mv.subject_id = c.subject_id
    AND mv.hadm_id = c.hadm_id
    AND mv.icustay_id = c.icustay_id
  WHERE
    datetime_diff(mv.mv_starttime,
      bgfd.charttime,
      HOUR) <= 24
    AND mv.mv_starttime < bgfd.charttime
    AND datetime_diff(mv.mv_starttime,
      c.charttime,
      HOUR) <= 24
    AND mv.mv_starttime < c.charttime
    AND c.itemid IN (60,
      437,
      505,
      506,
      686,
      220339,
      224700) -- itemid for PEEP
    ),
  ----------------------------------------------------------------------------------------------------------------
  cohort AS -- patient has ARDS if at least one PFratio calculated on the first day is < 200 and PEEP > 5
  (
  SELECT
    pf.subject_id,
    pf.hadm_id,
    pf.icustay_id,
    pf.mv_durations_hours,
    pf.mv_starttime,
    pf.mv_endtime,
    MIN(pf.pao2fio2) AS min_pao2fio2,
    MAX(peep) AS max_peep,
    pf.age,
    pf.dod,
    pf.los,
    pf.weight,
    pf.height,
    pf.BMI,
    pf.admission_type,
    pf.admission_location,
    pf.LANGUAGE,
    pf.religion,
    pf.marital_status,
    pf.ethnicity,
    pf.diagnosis
  FROM
    pao2fio2_PEEP_within_24_hours_of_intubation pf
  GROUP BY
    pf.subject_id,
    pf.hadm_id,
    pf.icustay_id,
    pf.mv_starttime,
    pf.mv_durations_hours,
    pf.mv_endtime,
    pf.age,
    pf.dod,
    pf.los,
    pf.weight,
    pf.height,
    pf.BMI,
    pf.admission_type,
    pf.admission_location,
    pf.LANGUAGE,
    pf.religion,
    pf.marital_status,
    pf.ethnicity,
    pf.diagnosis
  ORDER BY
    subject_id,
    hadm_id,
    icustay_id),

--   treatment AS (
--   SELECT
--     DISTINCT subject_id,
--     hadm_id,
--     icustay_id,
--     NMB_amount_per_count,
--     NMB_count,
--     NMB_amount_per_hour,
--     NMB_duration_h
--   FROM ( (
--       SELECT
--         c.subject_id,
--         c.hadm_id,
--         c.icustay_id,
--         AVG(mv.amount) AS NMB_amount_per_count,
--         COUNT(mv.amount) AS NMB_count,
--         -- uom are mg (9333)
--         SUM(DATETIME_DIFF(mv.endtime,
--             mv.starttime,
--             hour)) AS NMB_duration_h,
--         SUM(mv.amount) / (1 + DATETIME_DIFF(MAX(mv.starttime),
--             MIN(mv.starttime),
--             hour)) AS NMB_amount_per_hour -- add 1 to avoid division by zero
--       FROM
--         cohort c
--       LEFT OUTER JOIN
--         `ync-capstones.MIMIC3_V1_4.INPUTEVENTS_MV` mv
--       ON
--         c.subject_id = mv.subject_id
--         AND c.hadm_id = mv.hadm_id
--         AND c.icustay_id = mv.icustay_id
--       WHERE
--         mv.itemid IN (
--         SELECT
--           itemid
--         FROM
--           `NMB.NMBAs`)
--         AND mv.starttime > c.mv_starttime
--       GROUP BY
--         c.subject_id,
--         c.hadm_id,
--         c.icustay_id,
--         mv.AMOUNTUOM)
--     UNION DISTINCT (
--       SELECT
--         c.subject_id,
--         c.hadm_id,
--         c.icustay_id,
--         AVG(cv.amount) AS NMB_amount_per_count,
--         COUNT(cv.amount) AS NMB_count,
--         -- uom are mg (27127) and ml (2639)
--         COUNT(cv.amount) * 4.7 AS NMB_duration_h,
--         -- 4.7 is the average duration of ARDS dose in CareView
--         SUM(cv.amount) / (1 + COUNT(cv.amount) * 4.7) AS NMB_amount_per_hour -- add 1 to avoid division by zero
--       FROM
--         cohort c
--       LEFT OUTER JOIN
--         `ync-capstones.MIMIC3_V1_4.INPUTEVENTS_CV` cv
--       ON
--         c.subject_id = cv.subject_id
--         AND c.hadm_id = cv.hadm_id
--         AND c.icustay_id = cv.icustay_id
--       WHERE
--         cv.itemid IN (
--         SELECT
--           itemid
--         FROM
--           `NMB.NMBAs` )
--         AND cv.charttime > c.mv_starttime
--       GROUP BY
--         c.subject_id,
--         c.hadm_id,
--         c.icustay_id
--       HAVING
--         MAX(LOWER(cv.AMOUNTUOM)) LIKE "mg" -- to ensure patients only took MNBAs in g
--         AND COUNT(DISTINCT cv.AMOUNTUOM) <= 1 ) -- to ensure patients did not take another NMBA in a differenct uom
--       )
--   WHERE
--     NMB_count IS NOT NULL
--   ORDER BY
--     subject_id,
--     hadm_id,
--     icustay_id ),
    
  final AS (
  SELECT *
    -- DISTINCT c.*,
    -- t.NMB_count,
    -- t.NMB_amount_per_count,
    -- t.NMB_duration_h,
    -- t.NMB_amount_per_hour
  FROM
    cohort c
--   LEFT OUTER JOIN
--     treatment t
--   ON
--     c.subject_id = t.subject_id
--     AND c.hadm_id = t.hadm_id
--     AND c.icustay_id = t.icustay_id
  WHERE
    min_pao2fio2 <= 150 -- only include patients with hypoxemia
    -- and mv_durations_hours >= 48 -- only include patients who were on mechvent for >= 48 hours
    AND max_peep >= 5
    -- this adds up 4312 recoreds corresponding to unique hadm_id
    )
SELECT
  *
FROM
  final
-- WHERE
--   NMB_count IS NOT NULL