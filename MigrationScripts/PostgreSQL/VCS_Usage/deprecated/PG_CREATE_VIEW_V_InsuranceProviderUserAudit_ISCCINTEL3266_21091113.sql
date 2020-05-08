/***********************************************
WorkItem: ISCCINTEL-3266
Date: 2018-11-26
Author: Robert David Warner
Description: Object for exposing uniform Display layer information for VSC Usage.

			 Performance: No current notes.
************************************************/
CREATE VIEW cs_dw."V_InsuranceProviderUserAudit"
AS 
	SELECT
		--auditId VARCHAR(25) NOT NULL /*No NULLs*/,
		"userId" /*most are CHAR5, however it's possible that the datatype is for supporting userIds from disperate systems.*/,
		"userName" /*Source object is VARCHAR(30) but that seems small */,
		"userJobClassification",
		"userCompanyCode" /*99.96% identical to companyCode*/,
		"companyCode",
		"companyName" /*Customer_lvl0 from V_MM_Hierarchy*/,
		"groupCode" /*lvl3 from V_MM_Hierarchy*/,
		"groupName" /*Customer_lvl3 from V_MM_Hierarchy*/,
		"officeCode" ,
		"officeLocationForDisplay" ,
		"ipAddress" /*largest observed LEN is "15" but I remember something about legit multi-ipAddresses in our searches RDW 20191104*/,
		"claimSearchId" /*4/5 of the time this field is NULL*/,
		"reportId",
		"accessCategory" /*access description "action"*/,
		"accessMessage" /*customer facing access message*/,
		"searchCriteria",
		"accessUrl" /*MaxObserved 167*/,
		"accessDate" /**/
		--dateInserted,
		--deltaDate DATE
	FROM
		public."InsuranceProviderUserAudit";

		"iso21"."audit".
				public."InsuranceProviderUserAudit" AS "ExistingAuditRecord"
				"natb"."pyrtab".
					Possibly join from "iso21"."audit" using "iso21"."audit"."userid" to "natb"."pyrtab"."i_usr"
				"iso21"."v_mbr_ofc"