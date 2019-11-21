with vasopressor as (
select distinct icustay_id, 1 as vaso
from `MIMIC_V1_4_derived.vasopressordurations` 
group by icustay_id),

final as
(
select distinct c.*, case p.gender when 'F' then 1 else 0 end as female, o.oasis, e.elixhauser_vanwalraven as elixhauser, v.vaso 
from `NMB.cohort_trac` c

left outer join `MIMIC_V1_4_derived.oasis` o
-- Oxford Acute Severity of Illness Score (OASIS) calculated on the first dat of ICU stay - one per icustay_id (checked) 
on c.subject_id = o.subject_id
and c.hadm_id = o.hadm_id
and c.icustay_id = o.icustay_id
-- cohort size does not change 

left outer join `MIMIC_V1_4_derived.elixhauser_ahrq_score` e 
-- comorbidity is the presence of one or more additional conditions co-occurring with ARDS - use of elixhauser_vanwalraven method - one per hadm_id (checked) 
on c.subject_id = e.subject_id
and c.hadm_id = e.hadm_id
-- cohort size does not change 

left outer join vasopressor v
on c.icustay_id = v.icustay_id
-- cohort size does not change 

left outer join `MIMIC3_V1_4.PATIENTS` p 
on p.subject_id = c.subject_id 
) 

select * except (vaso), case vaso when 1 then 1 else 0 end as vaso  
from final 
	