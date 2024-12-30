"create table ods.em_can_noc_tbl as  " : 
"with t1 as (select can_noc_cd, can_noc_cd||max(to_char(effdt,'YYYYMMDD')) as noc_key from CHIPS_STG.PS_CAN_NOC_TBL group by can_noc_cd), " : 
"t2 as (select * from chips_stg.ps_can_noc_tbl where can_noc_cd||to_char(effdt,'YYYYMMDD') in (select noc_key from t1)) " : 
"select * from t2;"