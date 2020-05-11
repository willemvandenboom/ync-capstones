select *  
from `physionet-data.mimiciii_clinical.d_items` 
where LOWER(label) like "%tracu%" -- cisatracurium
or LOWER(label) like "nim%" -- nimbex
or LOWER(label) like "doxacu%" -- Doxacurium
or LOWER(label) like "traciu%" --  Tracium (common misspelling)
or LOWER(label) like "tracriu%" -- Tracrium
-- order by label