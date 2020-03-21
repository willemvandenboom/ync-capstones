-- with stg_spo2 as
-- (
--   select HADM_ID, CHARTTIME
--     -- avg here is just used to group SpO2 by charttime
--     , avg(valuenum) as SpO2
--   from `ync-capstones.MIMIC3_V1_4.CHARTEVENTS` 
--   -- o2 sat
--   where ITEMID in
--   (
--     646 -- SpO2
--   , 220277 -- O2 saturation pulseoxymetry
--   )
--   and valuenum > 0 and valuenum <= 100
--   and hadm_id in (select hadm_id from `ync-capstones.NMB.COHORT_BASELINE_P1`) -- added this line to reduce complexity of later extraction  
--   group by HADM_ID, CHARTTIME
-- )
-- , 
with stg_fio2 as
(
  select HADM_ID, CHARTTIME
    -- pre-process the FiO2s to ensure they are between 21-100%
    , max(
        case
          when itemid = 223835
            then case
              when valuenum > 0 and valuenum <= 1
                then valuenum * 100
              -- improperly input data - looks like O2 flow in litres
              when valuenum > 1 and valuenum < 21
                then null
              when valuenum >= 21 and valuenum <= 100
                then valuenum
              else null end -- unphysiological
        when itemid in (3420, 3422)
        -- all these values are well formatted
            then valuenum
        when itemid = 190 and valuenum > 0.20 and valuenum < 1
        -- well formatted but not in %
            then valuenum * 100
      else null end
    ) as Fio2
  from `ync-capstones.MIMIC3_V1_4.CHARTEVENTS`
  where ITEMID in
  (
    3420 -- FiO2
  , 190 -- FiO2 set
  , 223835 -- Inspired O2 Fraction (FiO2)
  , 3422 -- FiO2 [measured]
  )
  and valuenum > 0 and valuenum < 100
  and hadm_id in (select hadm_id from `ync-capstones.NMB.COHORT_BASELINE_P1`) -- added this line to reduce complexity of later extraction  
  -- exclude rows marked as error
--   and error IS NOT TRUE
  group by HADM_ID, CHARTTIME
)

select distinct f.hadm_id, f.Fio2, TIMESTAMP(f.charttime) as Fio2_charttime -- TIMESTAMP_TRUNC(TIMESTAMP(s.charttime), HOUR) as charttime
-- from stg_spo2 s join 
from stg_fio2 f 
-- on s.hadm_id = f.hadm_id 
-- and  TIMESTAMP_TRUNC(TIMESTAMP(s.charttime), HOUR) = TIMESTAMP_TRUNC(TIMESTAMP(f.charttime), HOUR) 