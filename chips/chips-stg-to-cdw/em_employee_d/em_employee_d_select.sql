SELECT
    P.EMPLID,
    J.EMPL_RCD,
    P.NAME,
    P.NAME_PREFIX,  P.PREF_FIRST_NAME,
    P.COUNTRY,  
    'x' as ADDRESS1, --P.ADDRESS1
    'x' as ADDRESS2, --P.ADDRESS2,   
    'x' as ADDRESS3, --P.ADDRESS3, 
    'x' as ADDRESS4, --P.ADDRESS4,
    'x' as CITY, --P.CITY,     
    'x' as HOUSE_TYPE, --P.HOUSE_TYPE, 
    'x' as COUNTY, --P.COUNTY,   
    'x' as STATE, --P.STATE,
    'x' as POSTAL, --P.POSTAL,   
    'x' as GEO_CODE, --P.GEO_CODE,
    'x' as HOME_PHONE, --P.PHONE as HOME_PHONE,
    P.SEX,
    null as BIRTHDATE, --P.BIRTHDATE,
    P.DT_OF_DEATH,
    P.MAR_STATUS,
    P.DISABLED,

    E.HIRE_DT,
    E.REHIRE_DT,
    E.CMPNY_SENIORITY_DT,
    E.SERVICE_DT,
    E.LAST_DATE_WORKED,
    E.BUSINESS_TITLE,
    E.REPORTS_TO,
    E.SUPERVISOR_ID,
    E.POSITION_PHONE as WORK_PHONE,

    J.BUSINESS_UNIT,
    J.DEPTID,
    J.JOBCODE,
    J.POSITION_NBR,
    J.EMPL_STATUS,
    J.LOCATION,
    J.COMPANY,
    J.PAYGROUP,
    J.EMPL_TYPE,
    J.GRADE,
    J.EXPECTED_RETURN_DT,
    J.TERMINATION_DT,

    jc.descr as JOBTITLE,

    NULL as ORIG_HIRE_DT,
    NULL as NID_COUNTRY,
    NULL as NATIONAL_ID_TYPE,
    NULL as NATIONAL_ID

  FROM   PS_PERSONAL_DATA P
      JOIN (

	select
	       emplid,
	       empl_rcd,
	       BUSINESS_UNIT,
	       DEPTID,
	       JOBCODE,
	       setid_jobcode,
	       POSITION_NBR,
	       EMPL_STATUS,
	       LOCATION,
	       COMPANY,
	       PAYGROUP,
	       EMPL_TYPE,
	       GRADE,
	       EXPECTED_RETURN_DT,
	       TERMINATION_DT

	from ps_job
	where (emplid, empl_rcd, effdt,effseq) in
	( -- x3
		-- select only one employee profile  per employee(most recent record), giving preference to MHSD profile
	select emplid, empl_rcd ,effdt, effseq -- MHSD_YN
		from ( -- x2
			-- for each employee, identify last profile -- this profile will be used for load into CDW ; give preference to employee profile from MHSD
			select  emplid, MHSD_YN,effdt, effseq,empl_rcd ,
			      last_value(effdt) over (
			          partition by emplid   order by mhsd_yn,effdt    rows between unbounded preceding and unbounded following
			      ) as last_effdt,

			      last_value(effseq) over (
			          partition by emplid   order by mhsd_yn,effdt,effseq    rows between unbounded preceding and unbounded following
			      ) as last_effseq,

			     last_value(empl_rcd) over (
			          partition by emplid   order by mhsd_yn,effdt,effseq,empl_rcd    rows between unbounded preceding and unbounded following
			      ) as last_empl_rcd

			from ( -- x1
				-- an  employeemay have multiple profiles (identified by empl_rcd  field)
				-- for each employee profile (combination of emplid,empl_rcd),
				   -- show if the profile belongs to MHSD or not
				   -- identify the last ps_job  record (eff_dt, eddseq)
				select
				  emplid,empl_rcd,  decode(business_unit,'BC031','Y','N') as MHSD_YN,

				  effdt,
				  last_value(effdt) over ( partition by emplid, empl_rcd
				                          order by effdt rows between unbounded preceding and unbounded following) as last_eff_dt,

				  effseq,
				  last_value(effseq) over ( partition by emplid, empl_rcd
				                            order by effdt,effseq rows between unbounded preceding and unbounded following ) as last_effseq
				from ps_job  WHERE effdt<=sysdate
			)   -- x1
			-- only show the last record for each profile
			where effdt = last_eff_dt and effseq=last_effseq
		)  -- x2
		-- select only one employee profile per employee
		where effdt=last_effdt and effseq=last_effseq and last_empl_rcd=empl_rcd
	) -- x3
      ) J
          on p.emplid=j.emplid

      JOIN ps_jobcode_tbl jc on j.jobcode=jc.jobcode and J.setid_jobcode=jc.setid

      LEFT JOIN ps_employment E ON j.emplid=E.emplid and J.empl_rcd=E.empl_rcd

      WHERE
              jc.effdt = (select max(x2.effdt) from ps_jobcode_tbl x2 where  jc.setid=x2.setid and jc.jobcode=x2.jobcode and x2.effdt<=sysdate)