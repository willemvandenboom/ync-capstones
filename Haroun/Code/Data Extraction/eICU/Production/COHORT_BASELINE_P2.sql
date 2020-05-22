-- add variables before
-- oasis for mimic / APACHE for eICU -- apachepatientresult
-- elixhauser for mimic / --  
-- explicit_sepsis	INTEGER	NULLABLE -- sepsis_from_diagnosis
-- vaso	INTEGER	NULLABLE -- pivoted_treatment_vasopressor

select  cohort.* , apache.apache, sepsis.sepsis, vaso.vaso
from `NMB_eICU.COHORT_BASELINE_P1` cohort
left outer join 
  (select patientunitstayid , avg(apachescore) as apache
  from `physionet-data.eicu_crd.apachepatientresult` 
  group by patientunitstayid) apache
on cohort.patientunitstayid = apache.patientunitstayid
left outer join 
  (select patientunitstayid , max(sepsis) as sepsis
  from `physionet-data.eicu_crd_derived.sepsis_from_diagnosis` 
  group by patientunitstayid) sepsis
on cohort.patientunitstayid = sepsis.patientunitstayid
left outer join 
  (select patientunitstayid , max(vasopressor) as vaso
  from `physionet-data.eicu_crd_derived.pivoted_treatment_vasopressor` 
  group by patientunitstayid) vaso
on cohort.patientunitstayid = vaso.patientunitstayid






