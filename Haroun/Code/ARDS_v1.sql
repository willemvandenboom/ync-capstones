with all_ICU_stays as  -- all ICU stays 
(select distinct p.subject_id, 
         a.hadm_id,
         i.icustay_id,
         trunc(date_diff(DATE(a.admittime), DATE(p.dob),MONTH)/12,0) as age,
         a.admittime, a.dischtime,
         i.intime, i.outtime,
         los,
        rank () over (partition by a.hadm_id order by i.intime) as icu_stay_rank        
 from `ync-capstones.MIMIC3_V1_4.PATIENTS` p, `ync-capstones.MIMIC3_V1_4.ADMISSIONS` a, `ync-capstones.MIMIC3_V1_4.ICUSTAYS` i
 where p.subject_id=a.subject_id
 and p.subject_id=i.subject_id
 and a.hadm_id=i.hadm_id
 -- order by p.subject_id, a.hadm_id, i.icustay_id
 ),
 
----------------------------------------------------------------------------------------------------------------

all_ICU_stays_2 as  -- exclude paitents younger than 19yo and patients who stayed in ICU less than 12 hours
(select distinct subject_id, hadm_id, icustay_id, age, intime, outtime, los, icu_stay_rank 
 from all_ICU_stays where (age >= 19 and los*24 >= 12 ) -- los uom is fraction of days 
 group by subject_id, hadm_id, icustay_id, icu_stay_rank, age, los, intime, outtime
 order by subject_id, hadm_id, icustay_id, icu_stay_rank, age, los, intime, outtime
),

----------------------------------------------------------------------------------------------------------------

first_ICU_stay as -- select only first ICU stay from all_ICU_stays_2 within the same hadm_id 
(select distinct subject_id, 
         hadm_id,
         icustay_id,
         age,
         intime, outtime,
         los        
from all_ICU_stays_2
where icu_stay_rank = 1
group by subject_id, hadm_id, icustay_id, intime, outtime, age, los
order by subject_id, hadm_id, icustay_id, intime, outtime, age, los
), 

----------------------------------------------------------------------------------------------------------------
first_ICU_stay_2 as -- more information added about the icustay and the patient
(select ie.subject_id,
        ie.hadm_id,
        ie.icustay_id,
        ie.intime,
        ie.outtime,
        ie.los,
        p.gender,
        p.dob,
        DATETIME_DIFF(ie.intime,p.dob,YEAR) AS age,
        h.Height,
        IF(w.Weight is null, IF(w.Weight_Admit is null, IF(w.Weight_Daily is null, IF(w.Weight_EchoInHosp is null, IF(w.Weight_EchoPreHosp is null,null,w.Weight_EchoPreHosp),w.Weight_EchoInHosp),w.Weight_Daily), w.Weight_Admit), w.Weight) AS weight,
        p.dod,
        a.admission_type,
        a.admission_location,
        a.insurance,
        a.language,
        a.religion,
        a.marital_status,
        a.ethnicity,
        a.diagnosis,
        a.hospital_expire_flag
from first_ICU_stay ie
LEFT JOIN `ync-capstones.MIMIC3_V1_4.PATIENTS` p
ON ie.subject_id = p.subject_id
LEFT JOIN `ync-capstones.MIMIC3_V1_4.ADMISSIONS` a
ON a.subject_id = ie.subject_id AND a.hadm_id = ie.hadm_id
LEFT JOIN `ync-capstones.MIMIC_V1_4_derived.heightfirstday` h
ON h.icustay_id = ie.icustay_id
LEFT JOIN `ync-capstones.MIMIC_V1_4_derived.weightfirstday` w
ON w.icustay_id = ie.icustay_id
ORDER BY subject_id, hadm_id, icustay_id),

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

ventil as -- table of all ventilation chartevents
(
select distinct subject_id, hadm_id, icustay_id, charttime
  -- case statement determining whether it is an instance of mech vent
  , max( -- we use max because we group by subject_id, hadm_id, icustay_id, charttime, and not itemid
    case
      when (itemid is null or value is null) then 0 -- can't have null values
      when (itemid = 720 and value != 'Other/Remarks') then 1  -- VentTypeRecorded
      when (itemid = 223848 and value != 'Other') then 1
      when (itemid = 223849) then 1 -- ventilator mode
      when (itemid = 467 and value = 'Ventilator') then 1 -- O2 delivery device == ventilator
      when itemid in
        (
        445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
        , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
        , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
        , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
        , 543 -- PlateauPressure
        , 5865,5866,224707,224709,224705,224706 -- APRV pressure
        , 60,437,505,506,686,220339,224700 -- PEEP
        , 3459 -- high pressure relief
        , 501,502,503,224702 -- PCV
        , 223,667,668,669,670,671,672 -- TCPCV
        , 224701 -- PSVlevel
        )
        then 1
      else 0
    end
    ) as MechVent
    
from `ync-capstones.MIMIC3_V1_4.CHARTEVENTS` ce
where ce.value is not null
and itemid in
(
    -- the below are settings used to indicate ventilation
      720, 223849 -- vent mode
    , 223848 -- vent type
    , 445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
    , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
    , 218,436,535,444,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean ("RespPressure")
    , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
    , 543 -- PlateauPressure
    , 5865,5866,224707,224709,224705,224706 -- APRV pressure
    , 60,437,505,506,686,220339,224700 -- PEEP
    , 3459 -- high pressure relief
    , 501,502,503,224702 -- PCV
    , 223,667,668,669,670,671,672 -- TCPCV
    , 224701 -- PSVlevel
    -- the below are settings used to indicate extubation
    , 640 -- extubated

    -- the below indicate oxygen/NIV, i.e. the end of a mechanical vent event
    , 468 -- O2 Delivery Device#2
    , 469 -- O2 Delivery Mode
    , 470 -- O2 Flow (lpm)
    , 471 -- O2 Flow (lpm) #2
    , 227287 -- O2 Flow (additional cannula)
    , 226732 -- O2 Delivery Device(s)
    , 223834 -- O2 Flow

    -- used in both oxygen + vent calculation
    , 467 -- O2 Delivery Device
)
group by subject_id, hadm_id, icustay_id, charttime
--order by subject_id, hadm_id, icustay_id, charttime
),
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

first_mech_ventil as 
(select distinct subject_id,
        hadm_id,
        icustay_id,
        MIN(charttime) firstVentilationCharttime
from ventil
where MechVent = 1
group by subject_id, hadm_id,icustay_id
order by subject_id, hadm_id, icustay_id, firstVentilationCharttime
),

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

mech_venti_on_first_ICU_stay as 
(
select distinct 
         a.subject_id, 
         a.hadm_id,
         a.icustay_id,
         a.dod,
         a.age,
         a.intime, 
         a.outtime,
         a.los,
         b.FirstVentilationcharttime as FirstVentilationChartTime,
         a.weight as weight, 
         a.height as height,
         a.admission_type,
         a.admission_location,
         a.insurance,
         a.language,
         a.religion,
         a.marital_status,
         a.ethnicity,
         a.diagnosis
         
         
from first_ICU_stay_2 a inner join first_mech_ventil b
on b.subject_id = a.subject_id and a.hadm_id = b.hadm_id and a.icustay_id = b.icustay_id
group by subject_id, 
         hadm_id,
         icustay_id, 
         dod, 
         intime, 
         FirstVentilationChartTime, 
         outtime, 
         age, 
         los, 
         a.weight, 
         a.height, 
         a.admission_type,
         a.admission_location,
         a.insurance,
         a.language,
         a.religion,
         a.marital_status,
         a.ethnicity,
         a.diagnosis
having FirstVentilationChartTime >= intime and FirstVentilationChartTime <= outtime
order by subject_id, hadm_id, icustay_id, dod, intime, FirstVentilationChartTime, outtime, age, los),

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
FiO2 as -- add FiO2 value to icustays on mech_venti_on_first_ICU_stay
(
select mv.subject_id, 
       mv.hadm_id, 
       mv.icustay_id, 
       ce.itemid, 
       ce.charttime as FiO2_charttime, 
        (case 
        when itemid = 223835
        then case
                when valuenum > 0 and valuenum <= 1
                then valuenum * 100          -- FiO2 taken from metavision
                when valuenum > 1 and valuenum < 21  -- improperly input data - looks like O2 flow in litres
                then null
                when valuenum >= 21 and valuenum <= 100
                then valuenum
                else null end                    -- unphysiological
        when itemid in (3420, 3422)    -- all these values are well formatted
         then valuenum
        when itemid = 190 and valuenum > 0.20 and valuenum <= 1     -- FiO2 taken from carevue -- well formatted but not in %
         then valuenum * 100
        else null
        end) as FiO2_valuenum, -- value numeric
       valueuom as FiO2_valueuom, -- value unit of measure
       mv.age, 
       mv.dod, 
       mv.los, 
       mv.weight, 
       mv.height, 
       mv.admission_type,
       mv.admission_location,
       mv.insurance,
       mv.language,
       mv.religion,
       mv.marital_status,
       mv.ethnicity,
       mv.diagnosis
from mech_venti_on_first_ICU_stay mv left join `ync-capstones.MIMIC3_V1_4.CHARTEVENTS` ce 
on ce.subject_id = mv.subject_id 
  AND ce.hadm_id = mv.hadm_id 
  AND ce.icustay_id = mv.icustay_id
where ITEMID in (190,223835,3420,3422) -- itemid of FiO2
  AND ce.charttime >= mv.FirstVentilationChartTime -- measure taken after mech venti
  AND datetime_diff(ce.charttime,mv.intime,HOUR) >= 0 -- measure taken after intime
order by subject_id, hadm_id, icustay_id, FiO2_charttime
),

----------------------------------------------------------------------------------------------------------------
PaO2 as 
(select mv.subject_id, 
       mv.hadm_id, 
       mv.icustay_id, 
       itemid as PaO2_itemid, 
       charttime as PaO2_charttime, 
       valuenum as PaO2_value, 
       valueuom as PaO2_valueuom
       /*
       mv.weight, 
       mv.height, 
       mv.admission_type,
       mv.admission_location,
       mv.insurance,
       mv.language,
       mv.religion,
       mv.marital_status,
       mv.ethnicity,
       mv.diagnosis
       */
from mech_venti_on_first_ICU_stay mv left join `ync-capstones.MIMIC3_V1_4.CHARTEVENTS` ce 
on ce.subject_id = mv.subject_id 
  AND ce.hadm_id = mv.hadm_id 
  AND ce.icustay_id = mv.icustay_id
where ITEMID in (779,220224) -- itemid of PaO2
  AND ce.charttime >= mv.FirstVentilationChartTime 
  AND datetime_diff(ce.charttime,mv.intime,HOUR) >= 0 and valuenum is not null
  order by subject_id, hadm_id, icustay_id, PaO2_charttime
),

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

pao2fio2_within_24_hours_of_intubation as 
-- pao2fio2 ratio taken from derived table bloodgasfirstdayarterial and is within 24 hours of the first mechanical ventialtion of the first ICU stay
(select mv.subject_id,
       mv.hadm_id,
       mv.icustay_id,
       bgfd.pao2fio2,
       mv.FirstVentilationChartTime,
       bgfd.charttime, 
       mv.age, 
       mv.dod, 
       mv.los,
       mv.weight, 
       mv.height,
       round(weight/(height*height/10000),1) as BMI,
       mv.admission_type,
       mv.admission_location,
       mv.insurance,
       mv.language,
       mv.religion,
       mv.marital_status,
       mv.ethnicity,
       mv.diagnosis
       
from mech_venti_on_first_ICU_stay mv left outer join MIMIC_V1_4_derived.bloodgasfirstdayarterial bgfd 
on mv.subject_id = bgfd.subject_id
  and mv.hadm_id = bgfd.hadm_id
  and mv.icustay_id = bgfd.icustay_id
where datetime_diff(mv.FirstVentilationChartTime,bgfd.charttime,HOUR) <= 24
), 

----------------------------------------------------------------------------------------------------------------

ARDS as -- patient has ARDS if at least one PFratio calculated on the first day is < 200
(
select pf.subject_id,
       pf.hadm_id,
       pf.icustay_id,
       min(pf.pao2fio2) as min_pao2fio2,
       case 
        when min(pf.pao2fio2) < 200 then 1 
        else 0 
        end as ARDS,
       pf.FirstVentilationChartTime,
       pf.age, 
       pf.dod, 
       pf.los,
       pf.weight, 
       pf.height,
       pf.BMI,
       pf.admission_type,
       pf.admission_location,
       pf.insurance,
       pf.language,
       pf.religion,
       pf.marital_status,
       pf.ethnicity,
       pf.diagnosis
from pao2fio2_within_24_hours_of_intubation pf
group by 
pf.subject_id,
       pf.hadm_id,
       pf.icustay_id,
       pf.FirstVentilationChartTime,
       pf.age, 
       pf.dod, 
       pf.los,
       pf.weight, 
       pf.height,
       pf.BMI,
       pf.admission_type,
       pf.admission_location,
       pf.insurance,
       pf.language,
       pf.religion,
       pf.marital_status,
       pf.ethnicity,
       pf.diagnosis
order by subject_id, hadm_id, icustay_id)
  
select * from ARDS
-- this adds up 17081 patients (total is 19620 but some do not have pf ratio) 