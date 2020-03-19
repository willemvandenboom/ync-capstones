with orderedtable as ( 
    select table1.*,
--     table1.icustay_id, table1.gender, table1.age, table1.ethnicity, table1.height, table1.weight, table1.sofatotal, table1.vent_duration, table1.oxygen_therapy_type, table1.supp_oxygen,icu_length_of_stay, table1.mortality_in_Hospt, table1.mortality_in_ICU, table1.deathtime,  table1.SFratio, table1.FiO2_valuenum, table1.new_Spo2,
    ROW_NUMBER() over( Partition by table1.icustay_id 
                        order by table1.charttime ASC) as rk
    from Ahmed_Project_Final.data_with_SFRatio_final  as table1
), 

firstvalues as ( select s.rk, s.icustay_id, s.gender, s.age, s.height, s.weight, s.FiO2_valuenum as first_FiO2, s.new_spo2 as first_SpO2, s.sofatotal as first_sofatotal, s.SFRatio as first_SFRatio
from orderedtable s 
where s.rk = 1 
order by s.icustay_id
),

aggregates as (select icustay_id,
    subject_id,
    gender,
    age,
    ethnicity,
    max(sofatotal) as max_sofatotal,
    max(vent_duration) as max_vent_duration,
    max(oxygen_therapy_type) as oxygen_therapy_type,
    max(supp_oxygen) as supp_oxygen,
    icu_length_of_stay, mortality_in_Hospt,
    mortality_in_ICU, deathtime,
    avg(SFratio) as avg_SFRatio,
    avg(FiO2_valuenum) as avg_FiO2,
    POW(avg(FiO2_valuenum), -1) as reciprocal_FiO2,
    avg(new_SPO2) as avg_SPO2,
    -- avg(median_SFRatio_24) as median_SFRatio_24, 
    -- avg(average_SFRatio_24) as average_SFRatio_24,
    -- avg(median_spo2_24) as median_spo2_24, 
    -- avg(average_spo2_24) as average_spo2_24, 
    -- avg(median_fio2_24) as median_fio2_24, 
    -- avg(average_fio2_24) as average_fio2_24
    from Ahmed_Project_Final.data_with_SFRatio_final
group by icustay_id, subject_id, gender, age, ethnicity, icu_length_of_stay, mortality_in_Hospt, mortality_in_ICU, deathtime
Order by icustay_id
), 

medians as (select icustay_id,  -- medians and some averages in this table
    subject_id,
    gender,
    age,
    ethnicity,
    PERCENTILE_CONT(sofatotal, 0.5) over (partition by icustay_id, subject_id)  as median_sofatotal,
    PERCENTILE_CONT(SFratio, 0.5) over (partition by icustay_id, subject_id)  as median_SFratio, 
    PERCENTILE_CONT(FiO2_valuenum, 0.5) over (partition by icustay_id, subject_id)  as median_FiO2, 
    PERCENTILE_CONT(new_SPO2, 0.5) over (partition by icustay_id, subject_id)  as median_new_SPO2,
    PERCENTILE_CONT(POW(FiO2_valuenum, -1), 0.5) over (partition by icustay_id, subject_id)  as median_reciprocal_FiO2,
    hosp_intime,
    ICU_intime,
    number_of_oxy_therapy,
    icu_time_till_oxy_therapy,
    hosp_time_till_oxy_therapy,
    vent_start,
    average_fio2_24,
    median_fio2_24,
    average_spo2_24,
    median_spo2_24,
    average_SFRatio_24,
    median_SFRatio_24, 
    median_SFRatio_minus__24, 
    average_SFRatio_minus__24, 
    median_fio2_minus__24,
    average_fio2_minus__24,
    median_minus_spo2_24, 
    average_minus_spo2_24,
    n_SpO2_24,
    n_minus_SpO2_24,
    n_FiO2_24,
    n_fio2_minus__24		
--     max(vent_duration) as max_vent_duration,
--     max(oxygen_therapy_type) as oxygen_therapy_type,
--     max(supp_oxygen) as supp_oxygen,
--     icu_length_of_stay, mortality_in_Hospt,
--     mortality_in_ICU, deathtime,
--     avg(SFratio) as avg_SFRatio,
--     avg(FiO2) as avg_FiO2,
--     POW(avg(FiO2), -1) as reciprocal_FiO2,
--     avg(new_SPO2) as avg_SPO2
from Ahmed_Project_Final.data_with_SFRatio_final 
Order by icustay_id)



select distinct f.*, a.* EXCEPT (icustay_id, gender, age), m.* EXCEPT (icustay_id, gender, age, subject_id, ethnicity)
from firstvalues f 
full outer join 
aggregates a on f.icustay_id = a.icustay_id 
full outer join medians m on f.icustay_id = m.icustay_id 
-- where m.number_of_oxy_therapy > 1
order by f.icustay_id

