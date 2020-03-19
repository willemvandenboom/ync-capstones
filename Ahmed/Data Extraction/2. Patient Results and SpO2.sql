WITH 
mortality_type AS (
SELECT
  icu.icustay_id AS icustay_id,
  CASE WHEN admissions.deathtime BETWEEN admissions.admittime and admissions.dischtime
  THEN 1 
  ELSE 0
  END AS mortality_in_Hospt, 
  CASE WHEN admissions.deathtime BETWEEN icu.intime and icu.outtime
  THEN 1
  ELSE 0
  END AS mortality_in_ICU,
  admissions.deathtime as deathtime,
  admissions.admittime as hosp_intime,
  icu.intime as ICU_intime,
  admissions.ethnicity
FROM `MIMIC_V1_4_derived.icustays` AS icu
INNER JOIN `MIMIC_V1_4_derived.admissions` AS admissions
  ON icu.hadm_id = admissions.hadm_id), 
 
   oxygen_therapy AS (
  SELECT * FROM `MIMIC_V1_4_derived.oxygen_therapy`
  ), 
  
  ordered_oxy_therapy as (select o.*, 
      ROW_NUMBER() over(Partition by o.icustay_id 
                        order by o.vent_start) as oxy_order
      from oxygen_therapy o
    )

  -- Aggregate `oxygen_therapy` per ICU stay.
  , o_t AS (
    SELECT
      icustay_id
      , SUM(vent_duration) AS vent_duration
      , MAX(oxygen_therapy_type) AS oxygen_therapy_type
      , MAX(supp_oxygen) AS supp_oxygen
      , max(oxy_order) as number_of_oxy_therapy
    FROM ordered_oxy_therapy
    GROUP BY icustay_id
  ), 
  ce AS (
  SELECT DISTINCT 
    chart.icustay_id
    , chart.valuenum as spO2_Value
    , chart.charttime
  FROM `MIMIC_V1_4_derived.chartevents` AS chart
  WHERE chart.itemid in (220277, 646) 
    AND chart.valuenum IS NOT NULL
    -- exclude rows marked as error
    AND (chart.error <> 1 OR chart.error IS NULL) --chart.error IS DISTINCT FROM 1
    -- We remove oxygen measurements that are outside of the range [10, 100] #Why? 
    AND chart.valuenum >= 10
    AND chart.valuenum <= 100
),

oxy_therapy_times as (select one.*, two.* EXCEPT(icustay_id, vent_duration, oxygen_therapy_type, supp_oxygen) from 
o_t as two  left join ordered_oxy_therapy as one on one.icustay_id = two.icustay_id and one.oxygen_therapy_type = two.oxygen_therapy_type	
order by ventnum
),

-- `patients` on our Google cloud setup has each ICU stay duplicated 7 times.
-- We get rid of these duplicates.
pat AS (
	SELECT DISTINCT * FROM `MIMIC_V1_4_derived.patients`
),

SpO2_24 AS (
  -- Edited from https://github.com/cosgriffc/hyperoxia-sepsis
  SELECT DISTINCT
      ce.icustay_id
      -- We currently ignore the time aspect of the measurements.
      -- However, one ideally should take into account that
      -- certain measurements are less spread out than others.
    , COUNT(ce.spO2_Value) OVER(PARTITION BY ce.icustay_id) AS n_SpO2_24
    , PERCENTILE_CONT(ce.spO2_Value, 0.5) OVER(PARTITION BY ce.icustay_id) AS median_spo2_24
    , AVG(ce.spO2_Value) OVER(PARTITION BY ce.icustay_id) AS average_spo2_24
  FROM ce
    INNER JOIN oxygen_therapy ON ce.icustay_id = oxygen_therapy.icustay_id
  -- We are only interested in measurements during the first 24 hours of the oxygen therapy session.
  WHERE DATETIME_DIFF(ce.charttime, oxygen_therapy.vent_start_first, HOUR) <= 24
),

SpO2_minus_24 AS (
  -- Edited from https://github.com/cosgriffc/hyperoxia-sepsis
  SELECT DISTINCT
      ce.icustay_id
      -- We currently ignore the time aspect of the measurements.
      -- However, one ideally should take into account that
      -- certain measurements are less spread out than others.
    , COUNT(ce.spO2_Value) OVER(PARTITION BY ce.icustay_id) AS n_minus_SpO2_24
    , PERCENTILE_CONT(ce.spO2_Value, 0.5) OVER(PARTITION BY ce.icustay_id) AS median_minus_spo2_24
    , AVG(ce.spO2_Value) OVER(PARTITION BY ce.icustay_id) AS average_minus_spo2_24
  FROM ce
    INNER JOIN oxygen_therapy ON ce.icustay_id = oxygen_therapy.icustay_id
  -- We are only interested in measurements during the first 24 hours of the oxygen therapy session.
  WHERE DATETIME_DIFF(ce.charttime, oxygen_therapy.vent_start_first, HOUR) > 24
), 


SpO2_48 AS (
  -- Edited from https://github.com/cosgriffc/hyperoxia-sepsis
  SELECT DISTINCT
      ce.icustay_id
      -- We currently ignore the time aspect of the measurements.
      -- However, one ideally should take into account that
      -- certain measurements are less spread out than others.
    , COUNT(ce.spO2_Value) OVER(PARTITION BY ce.icustay_id) AS n_SpO2_48
    , PERCENTILE_CONT(ce.spO2_Value, 0.5) OVER(PARTITION BY ce.icustay_id) AS median_spo2_48
    , AVG(ce.spO2_Value) OVER(PARTITION BY ce.icustay_id) AS average_spo2_48
  FROM ce
    INNER JOIN oxygen_therapy ON ce.icustay_id = oxygen_therapy.icustay_id
  -- We are only interested in measurements during the first 24 hours of the oxygen therapy session.
  WHERE DATETIME_DIFF(ce.charttime, oxygen_therapy.vent_start_first, HOUR) <= 48
),


-- `icustays` has similar duplication, but the duplicates sometimes differ in the recorded careunit.
-- Note that no such duplicate care units are recorded in ICUSTAYS.csv available from Physionet.
-- We arbitrarily pick one care unit: This only affects 0.9% of ICU stays.
icu AS (SELECT *
FROM   (SELECT *,
               Row_number() OVER(PARTITION BY icustay_id ORDER BY first_careunit) rn
        FROM   `MIMIC_V1_4_derived.icustays`)
WHERE  rn = 1)




, heightweight AS (

WITH FirstVRawData AS
  (SELECT c.charttime,
    c.itemid,c.subject_id,c.icustay_id,
    CASE
      WHEN c.itemid IN (762, 763, 3723, 3580, 3581, 3582, 226512)
        THEN 'WEIGHT'
      WHEN c.itemid IN (920, 1394, 4187, 3486, 3485, 4188, 226707)
        THEN 'HEIGHT'
    END AS parameter,
    -- Ensure that all weights are in kg and heights are in centimeters
    CASE
      WHEN c.itemid   IN (3581, 226531)
        THEN c.valuenum * 0.45359237
      WHEN c.itemid   IN (3582)
        THEN c.valuenum * 0.0283495231
      WHEN c.itemid   IN (920, 1394, 4187, 3486, 226707)
        THEN c.valuenum * 2.54
      ELSE c.valuenum
    END AS valuenum
  FROM `MIMIC_V1_4_derived.chartevents` c
  WHERE c.valuenum   IS NOT NULL
  -- exclude rows marked as error
  AND (c.error <> 1 OR c.error IS NULL)  --c.error IS DISTINCT FROM 1
  AND  c.itemid  IN (762, 763, 3723, 3580, -- Weight Kg
    3581,                                     -- Weight lb
    3582,                                     -- Weight oz
    920, 1394, 4187, 3486,                    -- Height inches
    3485, 4188                                -- Height cm
    -- Metavision
    , 226707 -- Height (measured in inches)
    , 226512 -- Admission Weight (Kg)

    -- note we intentionally ignore the below ITEMIDs in metavision
    -- these are duplicate data in a different unit
    -- , 226531 -- Admission Weight (lbs.)
    -- , 226730 -- Height (cm)
    )
  AND c.valuenum <> 0 )
  
  --Select * from FirstVRawData
  , SingleParameters AS (
  SELECT DISTINCT subject_id,
         icustay_id,
         parameter,
         first_value(valuenum) over
            (partition BY subject_id, icustay_id, parameter
             order by charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
             AS first_valuenum
    FROM FirstVRawData

--   ORDER BY subject_id,
--            icustay_id,
--            parameter
  )
--select * from SingleParameters
, PivotParameters AS (SELECT subject_id, icustay_id,
    MAX(case when parameter = 'HEIGHT' then first_valuenum else NULL end) AS height_first,
    MAX(case when parameter = 'WEIGHT' then first_valuenum else NULL end) AS weight_first
  FROM SingleParameters
  GROUP BY subject_id,
    icustay_id
  )
--select * from PivotParameters
SELECT f.icustay_id,
  f.subject_id,
  ROUND( cast(f.height_first as numeric), 2) AS height_first,
  ROUND(cast(f.weight_first as numeric), 2) AS weight_first

FROM PivotParameters f)

    SELECT DISTINCT 
    icu.hadm_id AS HADM_id,       
    icu.icustay_id AS icustay_id,       
    icu.subject_id AS patient_ID,
    icu.outtime as icu_outtime,
    DATETIME_DIFF(oxy_therapy_times.vent_start_first, icu.intime, Minute)/60 AS icu_time_till_oxy_therapy,
    DATETIME_DIFF(oxy_therapy_times.vent_start_first, mortality_type.hosp_intime, Minute)/60 AS hosp_time_till_oxy_therapy,
    pat.gender AS gender,
    SAFE_CAST(heightweight.height_first AS FLOAT64) as height,
    SAFE_CAST(heightweight.weight_first AS FLOAT64) as weight,
    DATE_DIFF(DATE(icu.intime), DATE(pat.dob), YEAR) AS age,
    DATETIME_DIFF(icu.outtime, icu.intime, HOUR) / 24 AS icu_length_of_stay,
    mortality_type.* EXCEPT(icustay_id),
    sofa.sofa AS sofatotal,
    icd.* EXCEPT(hadm_id),
    ce.* EXCEPT(icustay_id), 
    o_t.* EXCEPT(icustay_id),
    oxy_therapy_times.*  EXCEPT(icustay_id, supp_oxygen, oxygen_therapy_type, vent_duration, number_of_oxy_therapy),
    SpO2_24.* EXCEPT(icustay_id), 
    SpO2_48.* EXCEPT(icustay_id),
    SpO2_minus_24.* EXCEPT(icustay_id)
    
    FROM icu
    LEFT JOIN ce
    ON icu.icustay_id = ce.icustay_id  
    LEFT JOIN pat
    ON icu.subject_id = pat.subject_id
    LEFT JOIN mortality_type
    ON icu.icustay_id = mortality_type.icustay_id
    LEFT JOIN `MIMIC3_V1_4.DIAGNOSES_ICD` AS icd 
    ON icu.hadm_id = icd.hadm_id
    LEFT JOIN `MIMIC_V1_4_derived.sofa` sofa 
    ON icu.hadm_id = sofa.hadm_id
    LEFT JOIN heightweight
    ON icu.icustay_id = heightweight.icustay_id
    LEFT JOIN o_t
    ON icu.icustay_id = o_t.icustay_id
    LEFT JOIN oxy_therapy_times
    on icu.icustay_id = oxy_therapy_times.icustay_id
    LEFT JOIN SpO2_24
    on icu.icustay_id = SpO2_24.icustay_id
    LEFT JOIN SpO2_minus_24
    on icu.icustay_id = SpO2_minus_24.icustay_id
    LEFT JOIN SpO2_48
    on icu.icustay_id = SpO2_48.icustay_id
    
   
order by icu.icustay_id
    