select *  
from `MIMIC3_V1_4.D_ITEMS` 
where LOWER(label) like "cis%" -- cisatracurium
or LOWER(label) like "nim%" -- nimbex
or LOWER(label) like "doxacu%" -- Doxacurium
-- or LOWER(label) like "succ%" -- Succynlocholine --exclude succinylcholine as it is an induction agent only for short term use (effect lasts <1h).
or LOWER(label) like "roc%" -- rocuronium
or LOWER(label) like "pancur%" -- Pancuronium
or LOWER(label) like "vecu%" -- Vecuronium
or LOWER(label) like "traciu%" --  Tracium (common misspelling)
or LOWER(label) like "tracriu%" -- Tracrium
or LOWER(label) like "atrac%" -- atracurium
order by label

-- cisatracurium
-- nimbex
-- Doxacurium
-- Succynlocholine
-- rocuronium
-- Pancuronium
-- Vecuronium