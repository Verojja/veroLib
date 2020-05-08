BEGIN TRANSACTION;
/***********************************************
WorkItem: ISCCINTEL-3266
Date: 2019-12-11
Author: Robert David Warner
Description: Mechanism for data-refresh of the InsuranceProviderUserAudit Table.

			Performance: This was originally intended to be deployed in JDE/MSSQL Server, which means
				it required several boilerplate code that has been commeted out for historic purposes.

************************************************/
/*Possible name based team-convention*/
CREATE OR REPLACE FUNCTION public.pkg_hsp_UpdateIPUserAudit (
	"dateFilterParam" TIMESTAMP = NULL
	/*"dailyLoadOverride" BIT = CAST(0 AS BIT)*/
)
RETURNS void
LANGUAGE plpgsql
AS $BODY$
	DECLARE
		"affectedRows" BIGINT;
	BEGIN
		/*MSSQL version of TranSafeSprocs for Atomic(ACID) data principles*//*
			DECLARE @internalTransactionCount TINYINT = 0;
			IF (@@TRANCOUNT = 0)
			BEGIN
				BEGIN TRANSACTION;
				SET @internalTransactionCount = 1;
			END;
		*/
		/*/*MSSQL (more specifically Verisk-JDE) specific code-implementation:
			Scheduled jobs require multiple-execute-protection due to the nature of how they are scheduled.*/
		IF(
			dailyLoadOverride =  CAST(1 AS BIT)
			OR NOT EXISTS
			(
				SELECT NULL
				FROM public.IPAuditActivityLog
				WHERE
					IPAuditActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND IPAuditActivityLog.isSuccessful = 1
					AND IPAuditActivityLog.executionDateTime >(current_timestamp + INTERVAL'-12 hour')
			)
		) THEN*/
		
		/*Set Logging Variables for execution*/
		CREATE TEMPORARY TABLE "Variables"
		(
			"dateInserted" TIMESTAMP NULL,
			"executionDateTime" TIMESTAMP NULL,
			"productCode" VARCHAR(50) NULL,
			"sourceDateTime" TIMESTAMP NULL,

			"stepId" SMALLINT NULL,
			"stepDescription" VARCHAR(1000) NULL,
			"stepStartDateTime" TIMESTAMP NULL,
			"stepEndDateTime" TIMESTAMP NULL,
			"recordsAffected" BIGINT NULL,
			"isSuccessful" BIT NULL,
			"stepExecutionNotes" VARCHAR(1000) NULL
		);

		INSERT INTO "Variables"
		(
			"dateInserted", "executionDateTime", "productCode",
			"sourceDateTime", "stepId", "stepDescription",
			"stepStartDateTime", "stepEndDateTime",
			"recordsAffected", "isSuccessful", "stepExecutionNotes"
		)
		SELECT
			current_timestamp, current_timestamp, 'FM',
			CAST (/*Casting as Date currently necesary due to system's datatype inconsistancy*/

				COALESCE
				(
					"dateFilterParam", /*always prioritize using a provided dateFilterParam*/
					MAX("IPAuditActivityLog"."executionDateTime"), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
					CAST('2008-01-01' AS TIMESTAMP) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
				)
				AS DATE
			), 
			1, 'CaptureIPUserAuditDataToImport',
			current_timestamp, NULL,
			NULL, CAST(1 AS BIT), NULL
		FROM
			(
				VALUES
					(1)
			) AS "GuaranteedRowHack" ("UnusedValue")
			LEFT OUTER JOIN public."IPAuditActivityLog"
				ON "IPAuditActivityLog"."stepId" = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND "IPAuditActivityLog"."isSuccessful" = CAST(1 AS BIT);

		CREATE TEMPORARY TABLE "IPUserAuditDataToImport" AS
		SELECT
			CAST("audit"."id" AS VARCHAR(25)) AS "auditId", /*20191118 - currently NO NULLS*/
			"ExistingAuditRecord"."auditId" AS "existingAuditId",
			NULLIF(
				LTRIM(RTRIM(
					CAST("audit"."userid" AS VARCHAR(50))
				)),''
			) AS "userId", /*20191118 - most are CHAR5, however it's possible that the datatype is for supporting userIds from disperate systems*/
			NULLIF(
				LTRIM(RTRIM(
					CAST("pyrtab"."name" AS VARCHAR(250))
				)),''
			) AS "userName",
			NULLIF(
				LTRIM(RTRIM(
					CAST("JobClassificationMap"."userJobClassification" AS VARCHAR(50)) /*JDE helper table, transfered to PostgreSQL via helper. Could Create as additional DDL Structure based on need.*/
				)),''
			) AS "userJobClassification",
			NULLIF(
				LTRIM(RTRIM(
					CAST("audit"."usercompanycode" AS CHAR(4))
				)),''
			) AS "userCompanyCode", /*20191118 - 99.96% identical to companyCode... is difference indicative of error? or meaningful information?*/
			NULLIF(
				LTRIM(RTRIM(
					CAST(SUBSTRING("pyrtab".cmpnyid,1,4) AS CHAR(4))
				)),''
			) AS "companyCode",
			NULLIF(
				LTRIM(RTRIM(
					CAST("v_mbr_ofc"."mname" AS VARCHAR(55))
				)),''
			) AS "companyName", /*JDE Equivelent Assimilation: Customer_lvl0 from V_MM_Hierarchy*/
			COALESCE(
				NULLIF(
					LTRIM(RTRIM(
						CAST("v_mbr_ofc"."main_affiliate" AS CHAR(4))
					)),''
				),
				CAST(SUBSTRING("pyrtab".cmpnyid,1,4) AS CHAR(4)) /*company code*/
			) AS "groupCode", /*lvl3 from V_MM_Hierarchy*/  /*JDE Equivelent Assimilation: Customer_lvl0 from V_MM_Hierarchy*/
			NULLIF(
				LTRIM(RTRIM(
					CAST("v_mbr_ofc"."group_name" AS VARCHAR(55))
				)),''
			) AS "groupName", /*Customer_lvl3 from V_MM_Hierarchy*/
			NULLIF(
				LTRIM(RTRIM(
					CAST(SUBSTRING("pyrtab".CMPNYID,5,5) AS CHAR(5))
				)),''
			) AS "officeCode",

			"V_OfficeLocationKey"."officeLocationForDisplay",

			NULLIF(
				LTRIM(RTRIM(
					CAST("audit"."ipaddress" AS VARCHAR(50))
				)),''
			) AS "ipAddress", /*RDW 20191104 - largest observed LEN is "15" but I remember something about legit multi-ipAddresses in our searches*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit"."claimsearchid" AS CHAR(11))
					)),''
				),'NULL'
			) AS "claimSearchId",
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit"."reportid" AS VARCHAR(100))
					)),''
				),'NULL'
			) AS "reportId", /*MaxObserved 52*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit"."action" AS VARCHAR(30))
					)),''
				),'NULL'
			) AS "accessCategory", /*access description "action"*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit"."message" AS VARCHAR(200))
					)),''
				),'NULL'
			) AS "accessMessage",/*customer facing access message*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit"."searchcriteria" AS VARCHAR(500))
					)),''
				),'NULL'
			) AS "searchCriteria",
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit"."url" AS VARCHAR(250))
					)),''
				),'NULL'
			) AS "accessUrl", /*MaxObserved 167*/
			/*DEVNOTE: The following is a JDE-specific codeblock
						designed for string-to-date convertion; unnecessary in PostgreSQL*//*
			CASE
				WHEN
					char_length("audit"."dateofaccess") = 22
				THEN
					CAST(
						SUBSTRING("audit"."dateofaccess",1,10)
						+ ' '
						+ REPLACE((SUBSTRING("audit"."dateofaccess",12,8)),'.',':')
						+ (SUBSTRING("audit"."dateofaccess",20,8))
						AS TIMESTAMP
					)
				ELSE
					CAST(NULL AS TIMESTAMP)
			END AS "accessDate"
			*/
			"audit"."dateofaccess" AS "accessDate"
			/*dateInserted*/
			/*deltaDate*/
		FROM
			"iso21"."audit"
			LEFT OUTER JOIN public."InsuranceProviderUserAudit" AS "ExistingAuditRecord"
				ON CAST("audit"."id" AS VARCHAR(25)) = "ExistingAuditRecord"."auditId"
			LEFT OUTER JOIN "natb"."pyrtab"
				ON "audit"."userid" = "pyrtab"."i_usr"
			LEFT OUTER JOIN
			(
				VALUES
					('6','IS&T Modified Access for Production'),
					('7','Sales and Marketing'),
					('8','ClaimSearch Operations'),
					('9','Miscellaneous'),
					('H','Help Desk'),
					('M','Fire Marshal'),
					('V','Vehicle'),
					('A','Account Management'),
					('F','Fraud Bureau'),
					('U','Underwriter'),
					('S','SIU'),
					('O','Other '),
					('Q','Qualified'),
					('N','NICB User'),
					('C','Claims')
			) AS "JobClassificationMap" ("userJobCode","userJobClassification")
				ON pyrtab.jobclass = "JobClassificationMap"."userJobCode"
			LEFT OUTER JOIN "iso21"."v_mbr_ofc"
				ON CAST(SUBSTRING("pyrtab".cmpnyid,1,4) AS CHAR(4)) = "v_mbr_ofc"."memcomp"
			LEFT OUTER JOIN public."V_OfficeLocationKey"
				ON CAST(SUBSTRING("pyrtab".cmpnyid,5,5) AS CHAR(5)) = "V_OfficeLocationKey"."officeCode"
				AND CAST(SUBSTRING("pyrtab".cmpnyid,1,4) AS CHAR(4)) = "V_OfficeLocationKey"."companyCode"
		WHERE
			"audit"."id" IS NOT NULL;
			/*/*NOTE: There is no date_inserted in the PostgreSQL environment, which means that every single time
				this function or sproc is executed, it has to evaluate the entire history of data.
				This is a massive scalability and performance concern.
				
				Additional Note: once the dateInsert issue has been resolved the Variable TEMP table will needed to be joined against.*/
			WHERE
				...Date_Insert >= @dateFilterParam
			*/

		/*Set Logging Variables for Current Step_End_Success*/
		UPDATE "Variables"
		SET
			"stepEndDateTime" = current_timestamp,
			/*"recordsAffected" = ROW_COUNT,*/
			"isSuccessful" = CAST(1 AS BIT),
			"stepExecutionNotes" = NULL;
																 
		GET DIAGNOSTICS "affectedRows" = ROW_COUNT;
																 
		/*Log Activity*/
		INSERT INTO public."IPAuditActivityLog"
		(
			"productCode",
			"sourceDateTime",
			"executionDateTime",
			"stepId",
			"stepDescription",
			"stepStartDateTime",
			"stepEndDateTime",
			"recordsAffected",
			"isSuccessful",
			"stepExecutionNotes"
		)
		SELECT
			/*variables*/
			"productCode",
			"sourceDateTime",
			"executionDateTime",
			"stepId",
			"stepDescription",
			"stepStartDateTime",
			"stepEndDateTime",
			"affectedRows" /*variable*/,
			"isSuccessful",
			"stepExecutionNotes"
		FROM
			"Variables";

		/*Set Logging Variables for Current Step_Start*/
		UPDATE "Variables"
		SET
			"stepId" = 2,
			"stepDescription" = 'UpdateExistingIPUserAuditRecords',
			"stepStartDateTime" = current_timestamp;

		UPDATE "public"."InsuranceProviderUserAudit"
			SET
				"userId" = "SOURCE"."userId",
				"userName" = "SOURCE"."userName",
				"userJobClassification" = "SOURCE"."userJobClassification",
				"userCompanyCode" = "SOURCE"."userCompanyCode",
				"companyCode" = "SOURCE"."companyCode",
				"companyName" = "SOURCE"."companyName",
				"groupCode" = "SOURCE"."groupCode",
				"groupName" = "SOURCE"."groupName",
				"officeCode" = "SOURCE"."officeCode",
				"officeLocationForDisplay" = "SOURCE"."officeLocationForDisplay",
				"ipAddress" = "SOURCE"."ipAddress",
				"claimSearchId" = "SOURCE"."claimSearchId",
				"reportId" = "SOURCE"."reportId",
				"accessCategory" = "SOURCE"."accessCategory",
				"accessMessage" = "SOURCE"."accessMessage",
				"searchCriteria" = "SOURCE"."searchCriteria",
				"accessUrl" = "SOURCE"."accessUrl",
				"accessDate" = "SOURCE"."accessDate",
				"deltaDate" = "Variables"."dateInserted" /*date the charge was inserted*/
		FROM
			"IPUserAuditDataToImport" AS "SOURCE"
			INNER JOIN "Variables"
				ON  1=1
		WHERE
			"InsuranceProviderUserAudit"."auditId" = "SOURCE"."existingAuditId"
			AND 
			(
				/*auditId*/
				COALESCE("InsuranceProviderUserAudit"."userId",'NULL') <> COALESCE("SOURCE"."userId",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."userName",'NULL') <> COALESCE("SOURCE"."userName",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."userJobClassification",'NULL') <> COALESCE("SOURCE"."userJobClassification",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."userCompanyCode",'NULL') <> COALESCE("SOURCE"."userCompanyCode",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."companyCode",'NULL') <> COALESCE("SOURCE"."companyCode",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."companyName",'NULL') <> COALESCE("SOURCE"."companyName",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."groupCode",'NULL') <> COALESCE("SOURCE"."groupCode",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."groupName",'NULL') <> COALESCE("SOURCE"."groupName",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."officeCode",'NULL') <> COALESCE("SOURCE"."officeCode",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."officeLocationForDisplay",'NULL') <> COALESCE("SOURCE"."officeLocationForDisplay",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."ipAddress",'NULL') <> COALESCE("SOURCE"."ipAddress",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."claimSearchId",'NULL') <> COALESCE("SOURCE"."claimSearchId",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."reportId",'NULL') <> COALESCE("SOURCE"."reportId",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."accessCategory",'NULL') <> COALESCE("SOURCE"."accessCategory",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."accessMessage",'NULL') <> COALESCE("SOURCE"."accessMessage",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."searchCriteria",'NULL') <> COALESCE("SOURCE"."searchCriteria",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."accessUrl",'NULL') <> COALESCE("SOURCE"."accessUrl",'NULL')
				OR COALESCE("InsuranceProviderUserAudit"."accessDate",CAST('19000101' AS TIMESTAMP)) <> COALESCE("SOURCE"."accessDate",CAST('19000101' AS TIMESTAMP))
				/*dateInserted*/
				/*deltaDate*/
			);


		/*Set Logging Variables for Current Step_End_Success*/
		UPDATE "Variables"
		SET
			"stepEndDateTime" = current_timestamp,
			/*"recordsAffected" = ROW_COUNT,*/
			"isSuccessful" = CAST(1 AS BIT),
			"stepExecutionNotes" = NULL;
																											 
		GET DIAGNOSTICS "affectedRows" = ROW_COUNT;

		/*Log Activity*/
		INSERT INTO public."IPAuditActivityLog"
		(
			"productCode",
			"sourceDateTime",
			"executionDateTime",
			"stepId",
			"stepDescription",
			"stepStartDateTime",
			"stepEndDateTime",
			"recordsAffected",
			"isSuccessful",
			"stepExecutionNotes"
		)
		SELECT
			/*variables*/
			"productCode",
			"sourceDateTime",
			"executionDateTime",
			"stepId",
			"stepDescription",
			"stepStartDateTime",
			"stepEndDateTime",
			"affectedRows" /*variable*/,
			"isSuccessful",
			"stepExecutionNotes"
		FROM
			"Variables";

		/*Set Logging Variables for Current Step_Start*/
		UPDATE "Variables"
		SET
			"stepId" = 100,
			"stepDescription" = 'InsertNewIPUserAuditRecords',
			"stepStartDateTime" = current_timestamp;

		INSERT INTO public."InsuranceProviderUserAudit"
		(
			"auditId", "userId", "userName", "userJobClassification", "userCompanyCode",
			"companyCode", "companyName", "groupCode", "groupName", "officeCode",
			"officeLocationForDisplay", "ipAddress", "claimSearchId", "reportId", "accessCategory",
			"accessMessage", "searchCriteria", "accessUrl", "accessDate", "dateInserted",
			"deltaDate"
		)
		SELECT
			"SOURCE"."auditId",
			"SOURCE"."userId",
			"SOURCE"."userName",
			"SOURCE"."userJobClassification",
			"SOURCE"."userCompanyCode",
			"SOURCE"."companyCode",
			"SOURCE"."companyName",
			"SOURCE"."groupCode",
			"SOURCE"."groupName",
			"SOURCE"."officeCode",
			"SOURCE"."officeLocationForDisplay",
			"SOURCE"."ipAddress",
			"SOURCE"."claimSearchId",
			"SOURCE"."reportId",
			"SOURCE"."accessCategory",
			"SOURCE"."accessMessage",
			"SOURCE"."searchCriteria",
			"SOURCE"."accessUrl",
			"SOURCE"."accessDate",
			"Variables"."dateInserted" AS dateInserted,
			"Variables"."dateInserted" AS deltaDate
		FROM
			"IPUserAuditDataToImport" AS "SOURCE"
			INNER JOIN "Variables"
				ON  1=1
		WHERE
			"SOURCE"."existingAuditId" IS NULL;
			
		/*Set Logging Variables for Current Step_End_Success*/
		UPDATE "Variables"
		SET
			"stepEndDateTime" = current_timestamp,
			/*"recordsAffected" = ROW_COUNT,*/
			"isSuccessful" = CAST(1 AS BIT),
			"stepExecutionNotes" = NULL;
																											 
		GET DIAGNOSTICS "affectedRows" = ROW_COUNT;

		/*Log Activity*/
		INSERT INTO public."IPAuditActivityLog"
		(
			"productCode",
			"sourceDateTime",
			"executionDateTime",
			"stepId",
			"stepDescription",
			"stepStartDateTime",
			"stepEndDateTime",
			"recordsAffected",
			"isSuccessful",
			"stepExecutionNotes"
		)
		SELECT
			/*variables*/
			"productCode",
			"sourceDateTime",
			"executionDateTime",
			"stepId",
			"stepDescription",
			"stepStartDateTime",
			"stepEndDateTime",
			"affectedRows" /*variable*/,
			"isSuccessful",
			"stepExecutionNotes"
		FROM
			"Variables";
		/*******************EXCEPTION********************
		/*MSSQL version of TranSafeSprocs for Atomic(ACID) data principles*//*
			IF (@internalTransactionCount = 1)
			BEGIN
				ROLLBACK TRANSACTION;
			END
		*/
				
		/*Set Logging Variables for Current Step_End_Fail*/
		SELECT
			stepEndDateTime = current_timestamp,
			recordsAffected = ROW_COUNT,
			isSuccessful = 0,
			stepExecutionNotes = 'Error: TODO: expose\track postgreSQL error messages.';
		/*Log Activity*/
		INSERT INTO public.IPAuditActivityLog
		(
			productCode,
			sourceDateTime,
			executionDateTime,
			stepId,
			stepDescription,
			stepStartDateTime,
			stepEndDateTime,
			recordsAffected,
			isSuccessful,
			stepExecutionNotes
		)
		SELECT
			productCode,
			sourceDateTime,
			executionDateTime,
			stepId,
			stepDescription,
			stepStartDateTime,
			stepEndDateTime,
			recordsAffected,
			isSuccessful,
			stepExecutionNotes;
		/*/*MSSQL example of "Optional: We can bubble the error up to the calling level."*/
			IF (@internalTransactionCount = 0)
			BEGIN
				DECLARE
					raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
						'Error: in Step'
						+ CAST(@stepId AS VARCHAR(3))
						+ ' ('
						+ stepDescription
						+ ') '
						+ 'of hsp_UpdateIPUserAudit; ErrorMsg: '
						+ ERROR_MESSAGE(),
					errorSeverity INT,
					errorState INT;
				SELECT
					errorSeverity = ERROR_SEVERITY(),
					errorState = ERROR_STATE();
				RAISERROR(@raisError_message,@errorSeverity,@errorState);
			END*/
		************************************************/
		/*/*MSSQL (more specifically Verisk-JDE) specific code-implementation:
			Scheduled jobs require multiple-execute-protection due to the nature of how they are scheduled.*/
			END IF;
		*/
	END
$BODY$;

--ROLLBACK TRANSACTION;
COMMIT TRANSACTION;
/*
COMMIT

Query returned successfully in 220 msec.
*/