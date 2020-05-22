# template
# bq rm -f -t oxygenators-209612:mimiciii_clinical.icd_codes

bq rm -f -t ync-capstones:NMB_eICU.COHORT_BASELINE_P1 


# template
# bq mk --use_legacy_sql=false --view "$(cat icd_codes.sql)" oxygenators-209612:mimiciii_clinical.icd_codes

bq mk --use_legacy_sql=false --view "$(cat COHORT_BASELINE_P1.sql)" ync-capstones:NMB_eICU.COHORT_BASELINE_P1

