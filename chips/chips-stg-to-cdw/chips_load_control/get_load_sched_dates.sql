SELECT
    TO_NUMBER('#LOAD_SKEY#') LOAD_SKEY,
    PAYRUN_ID,
    PAY_PERIOD_START_DATE,
    PAY_PERIOD_END_DATE
FROM CHIPS_LOAD_SCHED
WHERE PAYRUN_ID =  '#PAYRUN_ID#'
;