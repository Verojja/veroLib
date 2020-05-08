BEGIN TRANSACTION;
--DROP TABLE cs_dw.InsuranceProviderUserAudit
--DROP TABLE cs_dw.IPAuditActivityLog
/***********************************************
WorkItem: ISCCINTEL-1184
Date: 2018-11-14
Author: Robert David Warner and Dan Ravaglia
Description: Table provides single point of access
				and makes possible delta-only-update(s) for
				Insurance Provider Company and Employee information.
************************************************/
CREATE TABLE cs_dw.ipu_audit
(
	auditid VARCHAR(25) NOT NULL, /*No NULLs*/
	userid VARCHAR(50) NULL,  /*most are CHAR5, however it's possible that the datatype is for supporting userIds from disperate systems.*/
	username VARCHAR(250) NULL /*Source object is VARCHAR(30) but that seems small */,
	userjobclassification VARCHAR(50) NULL,
	usercompanycode CHAR(4) NULL /*99.96% identical to companyCode*/,
	companycode CHAR(4) NULL,
	companyname VARCHAR(55) NULL, /*Customer_lvl0 from V_MM_Hierarchy*/
	groupcode CHAR(4) NULL, /*lvl3 from V_MM_Hierarchy*/
	groupname VARCHAR(55) NULL, /*Customer_lvl3 from V_MM_Hierarchy*/
	officecode CHAR(5) NULL,
	officelocationfordisplay VARCHAR(85) NULL,
	ipaddress VARCHAR(50) NULL, /*largest observed LEN is "15" but I remember something about legit multi-ipAddresses in our searches RDW 20191104*/
	claimsearchid CHAR(11) NULL, /*4/5 of the time this field is NULL*/
	reportid VARCHAR(100) NULL,
	accesscategory VARCHAR(30) NULL, /*access description "action"*/
	accessmessage VARCHAR(200) NULL, /*customer facing access message*/
	searchcriteria VARCHAR(500) NULL,
	accessurl VARCHAR(250) NULL, /*MaxObserved 167*/
	accessdate TIMESTAMP NULL, /**/
	dateinserted TIMESTAMP NOT NULL,
	deltadate DATE NOT NULL,
	CONSTRAINT pk_insuranceprovideruseraudit_auditid
		PRIMARY KEY (auditid)
);
CREATE INDEX nix_ipuseraudit_userid
	ON cs_dw.ipu_audit (userid);
CREATE INDEX nix_ipuseraudit_companycode
	ON cs_dw.ipu_audit (companycode);
CREATE INDEX nix_ipuseraudit_officecode
	ON cs_dw.ipu_audit (officecode);
/***********************************************
WorkItem: ISCCINTEL-1184
Date: 2019-11-18
Author: Robert David Warner
Description: Logging table for the InsuranceProviderUserAudit object
************************************************/
CREATE TABLE cs_dw.ipauditactivitylog
(
	ipaudit_activitylogid BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
	productcode VARCHAR(50) NULL,
	sourcedatetime TIMESTAMP NOT NULL,
	executiondatetime TIMESTAMP NOT NULL,
	stepid SMALLINT NOT NULL,
	stepdescription VARCHAR(1000) NULL,
	stepstartdatetime TIMESTAMP NULL,
	stependdatetime TIMESTAMP NULL,
	recordsaffected BIGINT NULL,
	issuccessful BIT NOT NULL,
	stepexecutionnotes VARCHAR(1000) NULL,
	CONSTRAINT pk_ipauditactivitylog_ipauditactivitylogid
		PRIMARY KEY (ipaudit_activitylogid)
);
CREATE INDEX nix_ipauditactivitylog_issuccessful_stepid_executiondatetime
	ON cs_dw.ipauditactivitylog (issuccessful, stepid, executiondatetime);

--ROLLBACK TRANSACTION;
COMMIT TRANSACTION;