# template
# bq rm -f -t oxygenators-209612:mimiciii_clinical.icd_codes

bq rm -f -t ync-capstones:NMB.try 
bq rm -f -t ync-capstones:NMB.NMBAs
bq rm -f -t ync-capstones:NMB.NMBs
bq rm -f -t ync-capstones:NMB.COHORT_BASELINE_P1 
bq rm -f -t ync-capstones:NMB.COHORT_BASELINE_P2
bq rm -f -t ync-capstones:NMB.TIME_DEP_PF
bq rm -f -t ync-capstones:NMB.TIME_DEP_TR

# template
# bq mk --use_legacy_sql=false --view "$(cat icd_codes.sql)" oxygenators-209612:mimiciii_clinical.icd_codes

bq mk --use_legacy_sql=false --view "$(cat ITEM_ID_TRACURIUM.sql)" ync-capstones:NMB.NMBAs
bq mk --use_legacy_sql=false --view "$(cat COHORT_BASELINE_P1.sql)" ync-capstones:NMB.COHORT_BASELINE_P1
bq mk --use_legacy_sql=false --view "$(cat COHORT_BASELINE_P2.sql)" ync-capstones:NMB.COHORT_BASELINE_P2
bq mk --use_legacy_sql=false --view "$(cat TIME_DEP_CONF.sql)" ync-capstones:NMB.TIME_DEP_PF
bq mk --use_legacy_sql=false --view "$(cat TIME_DEP_TREATMENT.sql)" ync-capstones:NMB.TIME_DEP_TR
