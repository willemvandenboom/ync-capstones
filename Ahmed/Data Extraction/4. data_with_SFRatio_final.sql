select distinct * EXCEPT(height, weight), 
  -- now going to use the latest SPO2 measurement per FiO2 measurement to calculate SF ratio 
  -- as in https://github.com/MIT-LCP/mimic-cookbook/blob/master/postgres/sofa_score_inserts.sql
  new_spO2/FiO2_valuenum * 100 as SFratio, 
  median_spo2_24/ median_fio2_24 * 100 as median_SFRatio_24,
  average_spo2_24/ average_fio2_24 * 100 as average_SFRatio_24,
  median_minus_spo2_24/ median_fio2_minus__24 * 100 as median_SFRatio_minus__24, -- Median SF Ratio over ICU stay excl. first 24 hours
  average_minus_spo2_24/ average_fio2_minus__24 * 100 as average_SFRatio_minus__24, -- Average SF Ratio over ICU stay excl. first 24 hours
  first_value(height) over (partition by icustay_id
             order by charttime) as height, 
  first_value(weight) over (partition by icustay_id
             order by charttime) as weight
from Ahmed_Project_Final.data_with_spo2and_fio2_final
--    WHERE DATETIME_DIFF (FiO2_charttime,charttime, DAY) < 1 -- Use only SPO2 values within 1 day of the FiO2 value but doesn't make much difference since all SpO2 and FiO2 values overlap
--    AND DATETIME_DIFF (FiO2_charttime,charttime, DAY) < 1  
Order by icustay_id, charttime