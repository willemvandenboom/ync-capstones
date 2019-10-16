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

mech_venti_on_first_ICU_stay as 
(
select distinct 
         a.subject_id, 
         a.hadm_id,
         a.icustay_id,
         b.duration_hours as mv_durations_hours,
         b.starttime as mv_starttime,
         b.endtime as mv_endtime,
         a.dod,
         a.age,
         a.intime, 
         a.outtime,
         a.los,
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
from first_ICU_stay_2 a 
inner join `MIMIC_V1_4_derived.ventdurations` b
on a.icustay_id = b.icustay_id
where b.ventnum = 1 
order by subject_id, hadm_id, icustay_id, dod, intime, outtime, age, los),

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

pao2fio2_PEEP_within_24_hours_of_intubation as 
-- pao2fio2 ratio taken from derived table bloodgasfirstdayarterial and is within 24 hours of the first mechanical ventialtion of the first ICU stay
-- PEEP taken from chartime
(select mv.subject_id,
       mv.hadm_id,
       mv.icustay_id,
       mv.mv_durations_hours,
       mv.mv_starttime,
       mv.mv_endtime,
       bgfd.pao2fio2,
       bgfd.charttime as pf_charttime,
       c.valuenum as peep, 
       c.charttime as peep_charttime, 
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
       
from mech_venti_on_first_ICU_stay mv 
left outer join MIMIC_V1_4_derived.bloodgasfirstdayarterial bgfd 
on mv.subject_id = bgfd.subject_id
  and mv.hadm_id = bgfd.hadm_id
  and mv.icustay_id = bgfd.icustay_id
left outer join `MIMIC3_V1_4.CHARTEVENTS` c 
on mv.subject_id = c.subject_id
  and mv.hadm_id = c.hadm_id
  and mv.icustay_id = c.icustay_id
where datetime_diff(mv.mv_starttime ,bgfd.charttime,HOUR) <= 24
and mv.mv_starttime < bgfd.charttime
and datetime_diff(mv.mv_starttime ,c.charttime,HOUR) <= 24
and mv.mv_starttime < c.charttime
and c.itemid in (60,437,505,506,686,220339,224700)
), 

----------------------------------------------------------------------------------------------------------------

cohort as -- patient has ARDS if at least one PFratio calculated on the first day is < 200 and PEEP > 8 
(
select pf.subject_id,
       pf.hadm_id,
       pf.icustay_id,
       pf.mv_durations_hours,
       pf.mv_starttime,
       pf.mv_endtime,
       min(pf.pao2fio2) as min_pao2fio2,
       max(peep) as max_peep,
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
from pao2fio2_PEEP_within_24_hours_of_intubation pf
group by 
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
       pf.insurance,
       pf.language,
       pf.religion,
       pf.marital_status,
       pf.ethnicity,
       pf.diagnosis
order by subject_id, hadm_id, icustay_id)
  
select distinct * from cohort 
where min_pao2fio2 <= 200 -- only include patients with hypoxemia 
and mv_durations_hours <= 48 -- only include patients who were on mechvent for < 48 hours 
and max_peep >= 5
-- this adds up 4172 records corresponding to unique hadm_id
