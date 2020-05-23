# template
# bq rm -f -t oxygenators-209612:mimiciii_clinical.icd_codes

bq rm -f -t ync-capstones:NMB_eICU.COHORT_BASELINE_P1 
bq rm -f -t ync-capstones:NMB_eICU.COHORT_BASELINE_P2
bq rm -f -t ync-capstones:NMB_eICU.TIME_DEP_CONF_FIO2
bq rm -f -t ync-capstones:NMB_eICU.TIME_DEP_CONF_SPO2
bq rm -f -t ync-capstones:NMB_eICU.TIME_DEP_TR

# template
# bq mk --use_legacy_sql=false --view "$(cat icd_codes.sql)" oxygenators-209612:mimiciii_clinical.icd_codes

bq mk --use_legacy_sql=false --view "$(cat COHORT_BASELINE_P1.sql)" ync-capstones:NMB_eICU.COHORT_BASELINE_P1
bq mk --use_legacy_sql=false --view "$(cat COHORT_BASELINE_P2.sql)" ync-capstones:NMB_eICU.COHORT_BASELINE_P2
bq mk --use_legacy_sql=false --view "$(cat TIME_DEP_CONF_FIO2.sql)" ync-capstones:NMB_eICU.TIME_DEP_CONF_FIO2
bq mk --use_legacy_sql=false --view "$(cat TIME_DEP_CONF_SPO2.sql)" ync-capstones:NMB_eICU.TIME_DEP_CONF_SPO2
bq mk --use_legacy_sql=false --view "$(cat TIME_DEP_TREATMENT.sql)" ync-capstones:NMB_eICU.TIME_DEP_TR
