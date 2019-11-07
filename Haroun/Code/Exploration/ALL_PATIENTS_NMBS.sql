select distinct subject_id, hadm_id, icustay_id
from 
(select cv.subject_id, cv.hadm_id, cv.icustay_id from 
`MIMIC3_V1_4.INPUTEVENTS_CV` cv
where cv.itemid in (select itemid from `NMB.NMBs`  )-- item id for Cisatracurium from carevue and from metavision
group by cv.subject_id, cv.hadm_id, cv.icustay_id)

union distinct 

(select mv.subject_id, mv.hadm_id, mv.icustay_id from 
`MIMIC3_V1_4.INPUTEVENTS_MV` mv
where mv.itemid in (select itemid from `NMB.NMBs` )-- item id for Cisatracurium from carevue and from metavision
group by mv.subject_id, mv.hadm_id, mv.icustay_id)
order by subject_id, hadm_id, icustay_id

-- this adds up to 1398 icustays 