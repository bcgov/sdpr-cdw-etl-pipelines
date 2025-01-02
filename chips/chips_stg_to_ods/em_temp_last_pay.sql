create table ods.em_temp_last_pay as (
    select max(pay_end_dt) as pay_end_dt 
    from chips_stg.d_date 
    where pay_end_dt<sysdate
);