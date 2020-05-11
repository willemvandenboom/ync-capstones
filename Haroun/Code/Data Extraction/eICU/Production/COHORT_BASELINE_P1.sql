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
  from `oxygenators-209612.eicu.patient` -- table of ICU stays confusingly called "patients"
  where unitDischargeOffset > 720 -- los is longer than 12 hours
  ), 

firstICU as (
  select * from allICU 
  where ICU_rank = 1 -- first icu stay within the same hospital admission
  ),

on_mech_vent as(
  select icu.*, ox.vent_start, ox.vent_end,	ox.oxygen_therapy_type,	ox.supp_oxygen	
  from firstICU icu 
  inner join `oxygenators-209612.eicu.eicu_oxygen_therapy` ox
  on icu.patientunitstayid = ox.icustay_id
  where ox.ventnum = 1),

------------------------------------------------------------------------------------------------------------------------
-- now we compute PF ratios

pao2 as --pao2 from lab
(
select patientunitstayid, min(labresult) as pao2
from 
  (select * from `oxygenators-209612.eicu.lab`
  where lower(labname) like 'pao2%') 
where labresultoffset between -1440 and 1440
group by patientunitstayid)
,

fio2 as --FIO2 from respchart
  (
  SELECT
    *
  FROM (
    SELECT
      DISTINCT patientunitstayid,
      case 
              when MAX(CAST(respchartvalue AS numeric)) > 0 and MAX(CAST(respchartvalue AS numeric)) <= 1
                then MAX(CAST(respchartvalue AS numeric)) * 100
              -- improperly input data - looks like O2 flow in litres
              when MAX(CAST(respchartvalue AS numeric)) > 1 and MAX(CAST(respchartvalue AS numeric)) < 21
                then null
              when MAX(CAST(respchartvalue AS numeric)) >= 21 and MAX(CAST(respchartvalue AS numeric)) <= 100
                then MAX(CAST(respchartvalue AS numeric))
              else null end -- unphysiological
       as fio2
      -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
    FROM
      `oxygenators-209612.eicu.respiratorycharting`
    WHERE
      respchartoffset BETWEEN -1440 AND 1440
      AND respchartvalue <> ''
      AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
    GROUP BY
      patientunitstayid) AS tempo
  ORDER BY
    patientunitstayid),
    
pf_ratio as 
(select fio2.patientunitstayid, 100 * pao2.pao2 / fio2.fio2 as pf_ratio
from fio2
inner join pao2 
on fio2.patientunitstayid = pao2.patientunitstayid), 
 
final as 
(select mv.*, pf.pf_ratio, lab.labresult as peep
from on_mech_vent mv
inner join pf_ratio pf
on mv.patientunitstayid = pf.patientunitstayid 
inner join `oxygenators-209612.eicu.lab` lab
on lab.patientunitstayid = mv.patientunitstayid
where LOWER(labname) like "%peep%"
and lab.labresultoffset BETWEEN -1440 AND 1440)

select * 
from final 
where pf_ratio <= 150 
and peep >= 5