with allICU as 
  (select 
    uniquepid, 
    patienthealthsystemstayid, 
    patientunitstayid,	
    gender,
    age,
    ethnicity,
    hospitaladmitoffset,
    unitdischargeoffset,
    hospitaldischargeoffset,
    unitdischargestatus,
    hospitaldischargestatus,
    ROW_NUMBER() OVER (PARTITION BY patienthealthsystemstayid ORDER BY hospitaladmitoffset) as ICU_rank
  --   hospitalid,
  --   wardid,
  --   apacheadmissiondx,
  --   admissionheight,
  --   hospitaladmitsource,
  --   hospitaldischargeyear,
  --   hospitaldischargetime24, 
  --   hospitaladmittime24,
  --   hospitaldischargelocation,
  --   unittype,
  --   unitadmitsource,
  --   unitvisitnumber,
  --   unitstaytype,
  --   admissionweight,
  --   dischargeweight,
  --   unitdischargetime24,
  --   unitdischargelocation		
  from `physionet-data.eicu_crd.patient` -- table of ICU stays confusingly called "patients"
  where unitDischargeOffset > 720 -- los is longer than 12 hours
  ), 

firstICU as (
  select * from allICU 
  where ICU_rank = 1 -- first icu stay within the same hospital admission
  ),

on_mech_vent as(
  select icu.*, ox.vent_start, ox.vent_end,	ox.oxygen_therapy_type,	ox.supp_oxygen	
  from firstICU icu 
  inner join `NMB_eICU.eicu_oxygen_therapy` ox
  on icu.patientunitstayid = ox.icustay_id
  where ox.ventnum = 1),

------------------------------------------------------------------------------------------------------------------------
-- now we compute PF ratios

pao2 as --pao2 from lab
(
select lab.patientunitstayid, labresult as pao2, lab.labresultoffset
from 
  (select * 
  from `physionet-data.eicu_crd.lab` lab
  where lower(labname) like 'pao2%') lab
left outer join on_mech_vent mv 
on lab.patientunitstayid = mv.patientunitstayid
where labresultoffset between -1440 + vent_start and 1440 + vent_start
-- group by patientunitstayid
)
,

fio2 as --FIO2 from respchart
  (SELECT
      DISTINCT rp.patientunitstayid,
      case 
              when CAST(respchartvalue AS numeric) > 0 and CAST(respchartvalue AS numeric) <= 1
                then CAST(respchartvalue AS numeric) * 100
              -- improperly input data - looks like O2 flow in litres
              when CAST(respchartvalue AS numeric) > 1 and CAST(respchartvalue AS numeric) < 21
                then null
              when CAST(respchartvalue AS numeric) >= 21 and CAST(respchartvalue AS numeric) <= 100
                then CAST(respchartvalue AS numeric)
              else null end -- unphysiological
       as fio2,
      -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
      rp.respchartoffset
    FROM
      `physionet-data.eicu_crd.respiratorycharting` rp
      left outer join on_mech_vent mv 
      on rp.patientunitstayid = mv.patientunitstayid
    WHERE
      respchartoffset BETWEEN -1440 + vent_start and 1440 + vent_start
      AND respchartvalue <> ''
      AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
  ORDER BY
    patientunitstayid),
    
pf_ratio as 
(select fio2.patientunitstayid, 100 * pao2.pao2 / fio2.fio2 as pf, fio2.respchartoffset as fio2_offset, pao2.labresultoffset as pao2_offset
from fio2
inner join pao2 
on fio2.patientunitstayid = pao2.patientunitstayid
where fio2.respchartoffset between pao2.labresultoffset - 240 and pao2.labresultoffset + 240
-- values are less than 4 hours apart
), 

peep as 
(
  select lab.patientunitstayid, max(labresult) as peep, patientunitstayid as peep_offset
  from `physionet-data.eicu_crd.lab` lab
  where LOWER(labname) like "%peep%"
  group by patientunitstayid
),

min_pf_ratio as (
select patientunitstayid, min(pf) as min_pf
from pf_ratio
group by patientunitstayid), 
 
final as 
(select mv.*, pf.min_pf, peep.peep
from on_mech_vent mv
inner join min_pf_ratio pf
on mv.patientunitstayid = pf.patientunitstayid 
inner join peep 
on mv.patientunitstayid = peep.patientunitstayid)

select * 
from final 
where min_pf <= 150 
and peep >= 5
-- 6947