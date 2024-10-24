# CHIPS
CHIPS is the Corporate Human Resource Information and Payroll System, containing employee 
information. The CHIPS web application is found at https://timepayhome.gov.bc.ca/. It is an Oracle PeopleSoft application. Hence, CHIPS is sometimes referred to as PeopleSoft.

The MHRGRP API exposes data in the OCI database for the CHIPS application. It is the data source for all CHIPS data in the Oracle SDPR CDW.

### CHIPS_STG
Data is copied one-to-one from endpoints at the MHRGRP API to the CHIPS_STG schema in the Oracle  CDW. From there, analytical tables are built in other schemas used by Analysts across SDPR, such as the CDW and ODS schemas.