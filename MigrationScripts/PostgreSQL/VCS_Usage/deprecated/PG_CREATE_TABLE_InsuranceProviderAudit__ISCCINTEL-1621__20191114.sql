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
CREATE TABLE cs_dw."InsuranceProviderUserAudit"
(
	"auditId" VARCHAR(25) NOT NULL, /*No NULLs*/
	"userId" VARCHAR(50) NULL,  /*most are CHAR5, however it's possible that the datatype is for supporting userIds from disperate systems.*/
	"userName" VARCHAR(250) NULL /*Source object is VARCHAR(30) but that seems small */,
	"userJobClassification" VARCHAR(50) NULL,
	"userCompanyCode" CHAR(4) NULL /*99.96% identical to companyCode*/,
	"companyCode" CHAR(4) NULL,
	"companyName" VARCHAR(55) NULL, /*Customer_lvl0 from V_MM_Hierarchy*/
	"groupCode" CHAR(4) NULL, /*lvl3 from V_MM_Hierarchy*/
	"groupName" VARCHAR(55) NULL, /*Customer_lvl3 from V_MM_Hierarchy*/
	"officeCode" CHAR(5) NULL,
	"officeLocationForDisplay" VARCHAR(85) NULL,
	"ipAddress" VARCHAR(50) NULL, /*largest observed LEN is "15" but I remember something about legit multi-ipAddresses in our searches RDW 20191104*/
	"claimSearchId" CHAR(11) NULL, /*4/5 of the time this field is NULL*/
	"reportId" VARCHAR(100) NULL,
	"accessCategory" VARCHAR(30) NULL, /*access description "action"*/
	"accessMessage" VARCHAR(200) NULL, /*customer facing access message*/
	"searchCriteria" VARCHAR(500) NULL,
	"accessUrl" VARCHAR(250) NULL, /*MaxObserved 167*/
	"accessDate" TIMESTAMP NULL, /**/
	"dateInserted" TIMESTAMP NOT NULL,
	"deltaDate" DATE NOT NULL,
	CONSTRAINT "PK_InsuranceProviderUserAudit_auditId"
		PRIMARY KEY ("auditId")
);
CREATE INDEX "NIX_IPUserAudit_userId"
	ON public."InsuranceProviderUserAudit" ("userId");
CREATE INDEX "NIX_IPUserAudit_companyCode"
	ON public."InsuranceProviderUserAudit" ("companyCode");
CREATE INDEX "NIX_IPUserAudit_officeCode"
	ON public."InsuranceProviderUserAudit" ("officeCode");
/***********************************************
WorkItem: ISCCINTEL-1184
Date: 2019-11-18
Author: Robert David Warner
Description: Logging table for the InsuranceProviderUserAudit object
************************************************/
CREATE TABLE public."IPAuditActivityLog"
(
	"iPAuditActivityLogId" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
	"productCode" VARCHAR(50) NULL,
	"sourceDateTime" TIMESTAMP NOT NULL,
	"executionDateTime" TIMESTAMP NOT NULL,
	"stepId" SMALLINT NOT NULL,
	"stepDescription" VARCHAR(1000) NULL,
	"stepStartDateTime" TIMESTAMP NULL,
	"stepEndDateTime" TIMESTAMP NULL,
	"recordsAffected" BIGINT NULL,
	"isSuccessful" BIT NOT NULL,
	"stepExecutionNotes" VARCHAR(1000) NULL,
	CONSTRAINT "PK_IPAuditActivityLog_iPAuditActivityLogId"
		PRIMARY KEY ("iPAuditActivityLogId")
);
CREATE INDEX "NIX_IPAuditActivityLog_isSuccessful_stepId_executionDateTime"
	ON public."IPAuditActivityLog" ("isSuccessful", "stepId", "executionDateTime");
--ROLLBACK TRANSACTION;
COMMIT TRANSACTION;