select distinct subject_id, hadm_id, icustay_id, amount, count
from 
  ((select c.subject_id, c.hadm_id, c.icustay_id, avg(cv.amount) as amount, count(cv.amount) as count
  from `NMB.cohort` c
  left outer join
  `MIMIC3_V1_4.INPUTEVENTS_CV` cv
  on c.subject_id = cv.subject_id 
  and c.hadm_id = cv.hadm_id 
  and c.icustay_id = cv.icustay_id 
  where cv.itemid in (select itemid from `NMB.NMBs` ) 
  and cv.charttime > c.mv_starttime
  group by c.subject_id, c.hadm_id, c.icustay_id)
  
  union all 
  
  (select c.subject_id, c.hadm_id, c.icustay_id, avg(mv.amount) as amount, count(mv.amount) as count
  from `NMB.cohort` c
  left outer join
  `MIMIC3_V1_4.INPUTEVENTS_MV` mv
  on c.subject_id = mv.subject_id 
  and c.hadm_id = mv.hadm_id 
  and c.icustay_id = mv.icustay_id 
  where mv.itemid in (select itemid from `NMB.NMBs`)
  and mv.starttime > c.mv_starttime
  group by c.subject_id, c.hadm_id, c.icustay_id))

where amount is not null
order by subject_id, hadm_id, icustay_id