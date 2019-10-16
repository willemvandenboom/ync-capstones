select * 
from `MIMIC3_V1_4.INPUTEVENTS_CV`  
where itemid in (30114, 221555) -- item id for Cisatracurium from carevue and from metavision
order by subject_id, hadm_id, icustay_id