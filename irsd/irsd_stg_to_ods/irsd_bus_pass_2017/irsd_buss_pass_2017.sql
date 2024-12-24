-- ORIGINAL VERSION of Bus Pass "valid" Orders for the current month
-- to be replaced by 2018-January with NEW ALGORITHMS + BUSINESS RULES

CREATE TABLE IRSD_BUS_PASS_O_2017 
  NOLOGGING COMPRESS PARALLEL AS
  SELECT 
    /*+ parallel */ 
    DISTINCT 
    con.contact_wid,
    con.X_CONTACT_NUM,
    
    CASE 
      WHEN con.D_X_PWD_STAT_CD = 'Eligible' THEN 'PWD' 
      ELSE 'Senior + other' 
    END AS "PERSON_PWD_CD",
    
    TO_CHAR(con.D_PWD_ADJUD_DT, 'yyyy-mm') AS "PERSON_PWD_START_MONTH",
    
    CASE 
      WHEN X_MSO_RSN_CD != 'Unspecified' THEN 'MSO flag' 
    END AS "PERSON_MSO_REASON",
    X_MSO_RSN_CD,
    
    CASE 
      WHEN con.X_ABOR_BAND_OU_ID != 'Unspecified' THEN 'INAC flag' 
    END AS "INAC_FLG",
    
    CASE 
      WHEN X_ABOR_BAND_OU_ID != 'Unspecified' THEN X_ABOR_BAND_OU_ID 
    END AS "INAC_ID",
    
    MAX(c.case_wid) OVER (PARTITION BY con.contact_wid) AS "BP_CASE_WID",
    MAX(c.case_num) OVER (PARTITION BY con.contact_wid) AS "BP_CASE_NUM",
    MAX(c.x_work_queue) OVER (PARTITION BY con.contact_wid) AS "BP_WORK_QUEUE",
    
    CASE 
      UPPER(NVL(MAX(X_TRANSIT_AREA) OVER (PARTITION BY con.contact_wid), 'Unknown')) 
      WHEN 'UNSPECIFIED' THEN 'Unknown' 
      WHEN 'UNKNOWN' THEN 'Unknown' 
      WHEN 'VANCOUVER' THEN 'Translink' 
      ELSE 'BC Transit' 
    END AS "TRANSIT_AREA_GRP",
    
    MAX(X_TRANSIT_AREA) OVER (PARTITION BY con.contact_wid) AS "TRANSIT_AREA",
    
    MAX(o.x_serial_num) OVER (PARTITION BY con.contact_wid) AS "SERIAL_NUM",
    
    -- Various 'B' Flags based on Approval and End Dates
    MAX(CASE 
      WHEN (CASE 
              WHEN o.X_APPROVAL_DT > o.X_EFF_START_DT 
              THEN o.X_APPROVAL_DT 
              ELSE o.X_EFF_START_DT 
            END) <= TO_DATE('20160801', 'yyyymmdd') 
      AND o.X_EFF_END_DT <= TO_DATE('20161231', 'yyyymmdd') 
      AND o.X_EFF_END_DT >= TO_DATE('20160801', 'yyyymmdd') 
      THEN 'B' 
      ELSE '.' 
    END) OVER (PARTITION BY con.contact_wid) AS "B_201608",

    MAX(CASE 
      WHEN (CASE 
              WHEN o.X_APPROVAL_DT > o.X_EFF_START_DT 
              THEN o.X_APPROVAL_DT 
              ELSE o.X_EFF_START_DT 
            END) <= LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -3)) + 1 
      AND o.X_EFF_END_DT <= TO_DATE('20171231', 'yyyymmdd') 
      AND o.X_EFF_END_DT >= LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -3)) + 1 
      THEN 'B' 
      ELSE '.' 
    END) OVER (PARTITION BY con.contact_wid) AS "B_PREV2",

    MAX(CASE 
      WHEN (CASE 
              WHEN o.X_APPROVAL_DT > o.X_EFF_START_DT 
              THEN o.X_APPROVAL_DT 
              ELSE o.X_EFF_START_DT 
            END) <= LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -2)) + 1 
      AND o.X_EFF_END_DT <= TO_DATE('20171231', 'yyyymmdd') 
      AND o.X_EFF_END_DT >= LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -2)) + 1 
      THEN 'B' 
      ELSE '.' 
    END) OVER (PARTITION BY con.contact_wid) AS "B_PREV",

    MAX(CASE 
      WHEN (CASE 
              WHEN o.X_APPROVAL_DT > o.X_EFF_START_DT 
              THEN o.X_APPROVAL_DT 
              ELSE o.X_EFF_START_DT 
            END) <= LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -1)) + 1 
      AND o.X_EFF_END_DT <= TO_DATE('20171231', 'yyyymmdd') 
      AND o.X_EFF_END_DT >= LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -1)) + 1 
      THEN 'B' 
      ELSE '.' 
    END) OVER (PARTITION BY con.contact_wid) AS "B_CURR"

  FROM 
    ods.IRSD_CASE c
  INNER JOIN 
    ods.IRSD_CONTACT con 
    ON con.contact_wid = c.D_CONTACT_KEY_PLAYER_WID
  INNER JOIN 
    ODS.IRSD_BENEFIT_PLAN bp 
    ON bp.case_wid = c.case_wid 
    AND bp.D_VALID_FLG = 'Y' 
    AND NVL(X_TRANSIT_AREA, 'Unspecified') != 'Unspecified'
  INNER JOIN 
    ods.IRSD_ORDER o 
    ON c.case_wid = o.case_wid 
    AND o.D_VALID_FLG = 'Y' 
    AND LENGTH(x_serial_num) = 20
    AND TO_CHAR(o.X_EFF_END_DT, 'yyyy') IN ('2016', '2017')
  WHERE 
    c.D_VALID_FLG = 'Y'
    AND c.TYPE_CD = 'Bus Pass'
;


-- ORIGINAL VERSION of all MIS Payments for the current month with any matching "Employment and Assistance" Cases 
-- to be replaced by 2018-January with NEW ALGORITHMS + BUSINESS RULES

CREATE TABLE IRSD_BUS_PASS_TSA_2017 
  NOLOGGING COMPRESS PARALLEL AS
  SELECT 
    /*+ parallel append */ 
    t.*, 

    CASE 
      WHEN CD24_PREV2 > 0 AND CD87_PREV2 < 0 THEN 'P'
      WHEN CD24_PREV2 > 0 THEN 'C'
      ELSE '.' 
    END || 
    CASE 
      WHEN CD24_CURR > 0 AND CD87_CURR < 0 THEN 'P'
      WHEN CD24_CURR > 0 THEN 'C'
      ELSE '.' 
    END || 
    CASE 
      WHEN CD24_CURR > 0 AND CD87_CURR < 0 THEN 'P'
      WHEN CD24_CURR > 0 THEN 'C'
      ELSE '.' 
    END AS "TSA_PATTERN"
  FROM 
  (
    SELECT 
      f.FIL_NUM,
      ea.CASE_WID AS "EA_CASE_WID",
      ea.CASE_NUM AS "EA_CASE_NUM",
      ea.STATUS_CD AS "EA_CASE_STATUS_CD",
      ea.X_WORK_QUEUE AS "EA_WORK_QUEUE",
      ea.X_CLASS_CD, 
      EA.X_CLASS_START_DT,
      EA.D_CONTACT_KEY_PLAYER_WID,
      EA.D_CONTACT_SPOUSE_WID,

      -- Previous month calculations
      SUM(CASE 
            WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -3)) + 1, 'yyyymm') 
            AND allowance_cd = '24' 
            THEN item_adj_amt 
          END) AS "CD24_PREV2",

      -- Previous month CD87 calculation
      SUM(CASE 
            WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -3)) + 1, 'yyyymm') 
            AND allowance_cd = '87' 
            THEN item_adj_amt 
          END) AS "CD87_PREV2",

      -- DA or TA flag for previous month
      NVL(MAX(CASE 
                WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -3)) + 1, 'yyyymm') 
                AND f.CORE_BUS_SK = 1 THEN 'DA' 
              END), 'TA') AS "DA_PREV2",

      -- Current month calculations
      SUM(CASE 
            WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -2)) + 1, 'yyyymm') 
            AND allowance_cd = '24' 
            THEN item_adj_amt 
          END) AS "CD24_PREV",

      SUM(CASE 
            WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -2)) + 1, 'yyyymm') 
            AND allowance_cd = '87' 
            THEN item_adj_amt 
          END) AS "CD87_PREV",

      NVL(MAX(CASE 
                WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -2)) + 1, 'yyyymm') 
                AND f.CORE_BUS_SK = 1 THEN 'DA' 
              END), 'TA') AS "DA_PREV",

      -- Current month calculations
      SUM(CASE 
            WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -1)) + 1, 'yyyymm') 
            AND allowance_cd = '24' 
            THEN item_adj_amt 
          END) AS "CD24_CURR",

      SUM(CASE 
            WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -1)) + 1, 'yyyymm') 
            AND allowance_cd = '87' 
            THEN item_adj_amt 
          END) AS "CD87_CURR",

      NVL(MAX(CASE 
                WHEN f.ASST_MTH_PART_NUM = TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -1)) + 1, 'yyyymm') 
                AND f.CORE_BUS_SK = 1 THEN 'DA' 
              END), 'TA') AS "DA_CURR"

    FROM 
      CDW.FN_PAYMENT_ITEM_F f
    LEFT JOIN 
      ODS.IRSD_CASE EA 
      ON ea.TYPE_CD = 'Employment and Assistance' 
      AND f.FIL_CD || f.FIL_NUM = ea.X_LEGACY_FILE_NUM
    WHERE 
      f.ASST_MTH_PART_NUM >= TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE), -3)) + 1, 'yyyymm')
      AND f.ALLOWANCE_CD IN ('87', '24') 
      AND f.POSTED_TO_GL_IND = 'Y'
    GROUP BY 
      f.FIL_NUM, 
      ea.CASE_WID, 
      ea.CASE_NUM, 
      ea.STATUS_CD, 
      ea.X_WORK_QUEUE, 
      ea.X_CLASS_CD, 
      EA.X_CLASS_START_DT,
      EA.D_CONTACT_KEY_PLAYER_WID, 
      EA.D_CONTACT_SPOUSE_WID
    HAVING 
      SUM(CASE WHEN allowance_cd = '24' THEN item_adj_amt END) != 0
      OR SUM(CASE WHEN allowance_cd = '87' THEN item_adj_amt END) != 0
  ) t
;

-- ORIGINAL VERSION of merge BP and EA, first match Key Player, then Spouse, then BP no-match then EA no-match
-- to be replaced by 2018-January with NEW ALGORITHMS + BUSINESS RULES

CREATE INDEX IRSD_BUS_PASS_O_2017_PK 
  ON IRSD_BUS_PASS_O_2017 (CONTACT_WID) NOLOGGING;

CREATE TABLE IRSD_BUS_PASS_2017 
  NOLOGGING COMPRESS AS 
  (
    SELECT 
      /*+ parallel append */ 
      o.CONTACT_WID AS "D_CONTACT_WID", 
      o.*, 
      'Key player' AS "BP_EA_MATCH_TYPE", 
      m.*
    FROM 
      IRSD_BUS_PASS_O_2017 o
    INNER JOIN 
      IRSD_BUS_PASS_TSA_2017 m 
      ON o.contact_wid = m.D_CONTACT_KEY_PLAYER_WID -- BP O matching EA Key players
  );

-- Insert for Spouse Matches
INSERT INTO IRSD_BUS_PASS_2017
SELECT 
  /*+ parallel append */ 
  o.CONTACT_WID AS "D_CONTACT_WID", 
  o.*, 
  'Spouse' AS "BP_EA_MATCH_TYPE", 
  m.*
FROM 
  IRSD_BUS_PASS_O_2017 o
INNER JOIN 
  IRSD_BUS_PASS_TSA_2017 m 
  ON o.contact_wid = m.D_CONTACT_SPOUSE_WID -- BP O matching EA Spouse
LEFT JOIN 
  IRSD_BUS_PASS_2017 bp 
  ON o.contact_wid = bp.contact_wid 
WHERE 
  bp.contact_wid IS NULL -- new unique CONTACT_WID only
  AND m.EA_CASE_WID NOT IN (SELECT EA_CASE_WID FROM IRSD_BUS_PASS_2017);

COMMIT;

-- Modify columns to allow NULLs
ALTER TABLE IRSD_BUS_PASS_2017 
  MODIFY (D_CONTACT_WID NULL, CONTACT_WID NULL);

-- Insert EA only records
INSERT INTO IRSD_BUS_PASS_2017
SELECT 
  /*+ parallel append */ 
  m.D_CONTACT_KEY_PLAYER_WID AS "D_CONTACT_WID", 
  o.*, 
  'EA ONLY' AS "BP_EA_MATCH_TYPE", 
  m.*
FROM 
  IRSD_BUS_PASS_TSA_2017 m -- EA remaining (not matched to BP O)
LEFT JOIN 
  IRSD_BUS_PASS_O_2017 o 
  ON 1 = 2
WHERE 
  m.FIL_NUM NOT IN (SELECT FIL_NUM FROM IRSD_BUS_PASS_2017);

COMMIT;

-- Insert BP only records
INSERT INTO IRSD_BUS_PASS_2017
SELECT 
  /*+ parallel append */ 
  o.CONTACT_WID AS "D_CONTACT_WID", 
  o.*, 
  'BP ONLY' AS "BP_EA_MATCH_TYPE", 
  m.*
FROM 
  IRSD_BUS_PASS_O_2017 o
LEFT JOIN 
  IRSD_BUS_PASS_TSA_2017 m 
  ON 1 = 2 -- BP O remaining (not matching EA)
LEFT JOIN 
  IRSD_BUS_PASS_2017 bp 
  ON o.contact_wid = bp.contact_wid
WHERE 
  bp.contact_wid IS NULL; -- new unique CONTACT_WID only;

COMMIT;
