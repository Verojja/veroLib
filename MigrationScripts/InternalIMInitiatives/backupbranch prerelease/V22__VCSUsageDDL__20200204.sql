BEGIN TRANSACTION;
DROP VIEW cs_dw.v_insuranceprovideruseraudit;
/***********************************************
WorkItem: ISCCINTEL-3266
Date: 2018-11-26
Author: Robert David Warner
Description: Object for exposing uniform Display layer information for VSC Usage.

			 Performance: No current notes.
************************************************/
CREATE OR REPLACE VIEW cs_dw.v_insuranceprovideruseraudit
AS 
	SELECT
		insuranceprovideruseraudit.auditid, /*the ID column for the audit table*/
		insuranceprovideruseraudit.userid,/*most are CHAR5, however it's possible that the datatype is for supporting userIds from disperate systems.*/
		insuranceprovideruseraudit.username,/*Source object is VARCHAR(30) but that seems small*/
		insuranceprovideruseraudit.userjobclassification,
		insuranceprovideruseraudit.usercompanycode,/*99.96% identical to companyCode*/
		insuranceprovideruseraudit.companycode,
		insuranceprovideruseraudit.companyname,/*Customer_lvl0 from V_MM_Hierarchy*/
		insuranceprovideruseraudit.groupcode,/*lvl3 from V_MM_Hierarchy*/
		insuranceprovideruseraudit.groupname,/*Customer_lvl3 from V_MM_Hierarchy*/
		insuranceprovideruseraudit.officecode,
		insuranceprovideruseraudit.officelocationfordisplay,
		insuranceprovideruseraudit.ipaddress,/*largest observed LEN is "15" but I remember something about legit multi-ipAddresses in our searches RDW 20191104*/
		insuranceprovideruseraudit.claimsearchid,/*4/5 of the time this field is NULL*/
		insuranceprovideruseraudit.reportid,
		insuranceprovideruseraudit.accesscategory,/*access description "action"*/
		insuranceprovideruseraudit.accessmessage,/*customer facing access message*/
		insuranceprovideruseraudit.searchcriteria,
		insuranceprovideruseraudit.accessurl, /*MaxObserved 167*/
		insuranceprovideruseraudit.accessdate
		/*dateInserted,*/
		/*deltaDate DATE*/
	FROM
		cs_dw.insuranceprovideruseraudit
	WHERE
		insuranceprovideruseraudit.accesscategory = 'MATCH_REPORT_S3';
/***********************************************
WorkItem: ISCCINTEL-3266
Date: 2018-11-13
Author: Julia Lawrence
Description: Object for exposing uniform office-location-format,
				designed by Buisiness (Zack Miller).
				Also exposes INCOMP regoff mapping.

			 Performance: Current plan is to deploy VIEW to ClaimSearch_Prod
				despite source-object existing in *another-DB-Environment.
				*Even if indexes are built on source-object, this view will
				not be able to take advantage of them.
************************************************/
CREATE OR REPLACE VIEW cs_dw.v_officelocationkey AS
SELECT
	insotab.inscomp AS companycode,
	insotab.regoff AS officecode,
	CAST(
		COALESCE(
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.regoff)),'') != '' THEN CAST(insotab.regoff AS CHAR(5))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.offname)),'') != '' THEN ' - ' || CAST(insotab.offname AS VARCHAR(30))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.mcity)),'') != '' THEN ' - ' || CAST(insotab.mcity AS VARCHAR(30))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.mst)),'') != '' THEN ', ' || CAST(insotab.mst AS VARCHAR(2))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.mzip)),'') != '' THEN ' ' || CAST(insotab.mzip AS VARCHAR(9))
				ELSE ''
			END,
			''
		) AS VARCHAR(85)
	) AS officelocationfordisplay
FROM
	natb.insotab;
/***********************************************
workitem: isccintel-3266
date: 2019-12-11
author: robert david warner
description: mechanism for data-refresh of the insurance provider useraudit table.

			performance: this was originally intended to be deployed in jde/mssql server, which means
				it required several boilerplate code that has been commeted out for historic purposes.

************************************************/
/*possible name based team-convention*/
CREATE OR REPLACE FUNCTION cs_dw.pkg_hsp_updateipuseraudit (
	datefilterparam TIMESTAMP = NULL
	/*dailyloadoverride bit = cast(0 as bit)*/
)
RETURNS VOID
LANGUAGE plpgsql
AS $BODY$
	DECLARE
		affectedrows BIGINT;
	BEGIN
		/*mssql version of transafesprocs for atomic(acid) data principles*//*
			declare @internaltransactioncount tinyint = 0;
			if (@@trancount = 0)
			begin
				begin transaction;
				set @internaltransactioncount = 1;
			end;
		*/
		/*/*mssql (more specifically verisk-jde) specific code-implementation:
			scheduled jobs require multiple-execute-protection due to the nature of how they are scheduled.*/
		if(
			dailyloadoverride =  cast(1 as bit)
			or not exists
			(
				select null
				from cs_dw.ipauditactivitylog
				where
					ipauditactivitylog.stepid = 100 /*default stepid for finalstep of updateinsert hsp*/
					and ipauditactivitylog.issuccessful = 1
					and ipauditactivitylog.executiondatetime >(current_timestamp + interval'-12 hour')
			)
		) then*/
		
		/*set logging variables for execution*/
		CREATE TEMPORARY TABLE variables
		(
			dateinserted TIMESTAMP NULL,
			executiondatetime TIMESTAMP NULL,
			productcode VARCHAR(50) NULL,
			sourcedatetime TIMESTAMP NULL,

			stepid SMALLINT NULL,
			stepdescription VARCHAR(1000) NULL,
			stepstartdatetime TIMESTAMP NULL,
			stependdatetime TIMESTAMP NULL,
			recordsaffected BIGINT NULL,
			issuccessful BIT NULL,
			stepexecutionnotes VARCHAR(1000) NULL
		);

		INSERT INTO variables
		(
			dateinserted, executiondatetime, productcode,
			sourcedatetime, stepid, stepdescription,
			stepstartdatetime, stependdatetime,
			recordsaffected, issuccessful, stepexecutionnotes
		)
		SELECT
			CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'fm',
			CAST (/*casting as date currently necesary due to system's datatype inconsistancy*/

				COALESCE
				(
					datefilterparam, /*always prioritize using a provided datefilterparam*/
					MAX(ipauditactivitylog.executiondatetime), /*in the absence of a provided datefilterparam, use the last successful executiondatetime*/
					CAST('2008-01-01' AS TIMESTAMP) /*if the log table is empty (ie: first run), use the earliest recorded date for address data*/
				)
				AS DATE
			), 
			1, 'captureipuserauditdatatoimport',
			CURRENT_TIMESTAMP, NULL,
			NULL, CAST(1 AS BIT), NULL
		FROM
			(
				VALUES
					(1)
			) AS guaranteedrowhack (unusedvalue)
			LEFT OUTER JOIN cs_dw.ipauditactivitylog
				ON ipauditactivitylog.stepid = 100 /*default stepid for finalstep of updateinsert hsp*/
					AND ipauditactivitylog.issuccessful = CAST(1 AS BIT);

		CREATE temporary TABLE ipuserauditdatatoimport AS
		SELECT
			CAST("audit".id AS VARCHAR(25)) AS auditid, /*20191118 - currently no nulls*/
			existingauditrecord.auditid AS existingauditid,
			NULLIF(
				LTRIM(RTRIM(
					CAST("audit".userid AS VARCHAR(50))
				)),''
			) AS userid, /*20191118 - most are char5, however it's possible that the datatype is for supporting userids from disperate systems*/
			NULLIF(
				LTRIM(RTRIM(
					CAST(pyrtab.name AS VARCHAR(250))
				)),''
			) AS username,
			NULLIF(
				LTRIM(RTRIM(
					CAST(jobclassificationmap.userjobclassification AS VARCHAR(50)) /*jde helper table, transfered to postgresql via helper. could create as additional ddl structure based on need.*/
				)),''
			) AS userjobclassification,
			NULLIF(
				LTRIM(RTRIM(
					CAST("audit".usercompanycode AS CHAR(4))
				)),''
			) AS usercompanycode, /*20191118 - 99.96% identical to companycode... is difference indicative of error? or meaningful information?*/
			NULLIF(
				LTRIM(RTRIM(
					CAST(SUBSTRING(pyrtab.cmpnyid,1,4) AS CHAR(4))
				)),''
			) AS companycode,
			NULLIF(
				LTRIM(RTRIM(
					CAST(v_mbr_ofc.mname AS VARCHAR(55))
				)),''
			) AS companyname, /*jde equivelent assimilation: customer_lvl0 from v_mm_hierarchy*/
			COALESCE(
				NULLIF(
					LTRIM(RTRIM(
						CAST(v_mbr_ofc.main_affiliate AS CHAR(4))
					)),''
				),
				CAST(SUBSTRING(pyrtab.cmpnyid,1,4) AS CHAR(4)) /*company code*/
			) AS groupcode, /*lvl3 from v_mm_hierarchy*/  /*jde equivelent assimilation: customer_lvl0 from v_mm_hierarchy*/
			NULLIF(
				LTRIM(RTRIM(
					CAST(v_mbr_ofc.group_name AS VARCHAR(55))
				)),''
			) AS groupname, /*customer_lvl3 from v_mm_hierarchy*/
			NULLIF(
				LTRIM(RTRIM(
					CAST(SUBSTRING(pyrtab.cmpnyid,5,5) AS CHAR(5))
				)),''
			) AS officecode,
			v_officelocationkey.officelocationfordisplay,
			NULLIF(
				LTRIM(RTRIM(
					CAST("audit".ipaddress AS VARCHAR(50))
				)),''
			) AS ipaddress, /*rdw 20191104 - largest observed len is 15 but i remember something about legit multi-ipaddresses in our searches*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit".claimsearchid AS CHAR(11))
					)),''
				),'null'
			) AS claimsearchid,
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit".reportid AS VARCHAR(100))
					)),''
				),'null'
			) AS reportid, /*maxobserved 52*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit".action AS VARCHAR(30))
					)),''
				),'null'
			) AS accesscategory, /*access description action*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit".message AS VARCHAR(200))
					)),''
				),'null'
			) AS accessmessage,/*customer facing access message*/
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit".searchcriteria AS VARCHAR(500))
					)),''
				),'null'
			) AS searchcriteria,
			NULLIF(
				NULLIF(
					LTRIM(RTRIM(
						CAST("audit".url AS VARCHAR(250))
					)),''
				),'null'
			) AS accessurl, /*maxobserved 167*/
			/*devnote: the following is a jde-specific codeblock
						designed for string-to-date convertion; unnecessary in postgresql*//*
			case
				when
					char_length("audit".dateofaccess) = 22
				then
					cast(
						substring("audit".dateofaccess,1,10)
						+ ' '
						+ replace((substring("audit".dateofaccess,12,8)),'.',':')
						+ (substring("audit".dateofaccess,20,8))
						as timestamp
					)
				else
					cast(null as timestamp)
			end as accessdate
			*/
			"audit".dateofaccess AS accessdate
			/*dateinserted*/
			/*deltadate*/
		FROM
			iso21."audit"
			LEFT OUTER JOIN cs_dw.insuranceprovideruseraudit AS existingauditrecord
				on cast("audit".id AS VARCHAR(25)) = existingauditrecord.auditid
			LEFT OUTER JOIN natb.pyrtab
				ON "audit".userid = pyrtab.i_usr
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
			) AS jobclassificationmap (userjobcode,userjobclassification)
				ON pyrtab.jobclass = jobclassificationmap.userjobcode
			LEFT OUTER JOIN iso21.v_mbr_ofc
				ON CAST(SUBSTRING(pyrtab.cmpnyid,1,4) AS CHAR(4)) = v_mbr_ofc.memcomp
			LEFT OUTER JOIN cs_dw.v_officelocationkey
				ON CAST(SUBSTRING(pyrtab.cmpnyid,5,5) AS CHAR(5)) = v_officelocationkey.officecode
				AND CAST(SUBSTRING(pyrtab.cmpnyid,1,4) AS CHAR(4)) = v_officelocationkey.companycode
		WHERE
			"audit".id is not null;
			/*/*note: there is no date_inserted in the postgresql environment, which means that every single time
				this function or sproc is executed, it has to evaluate the entire history of data.
				this is a massive scalability and performance concern.
				
				additional note: once the dateinsert issue has been resolved the variable temp table will needed to be joined against.*/
			where
				...date_insert >= @datefilterparam
			*/

		/*set logging variables for current step_end_success*/
		UPDATE variables
		SET
			stependdatetime = CURRENT_TIMESTAMP,
			/*recordsaffected = row_count,*/
			issuccessful = CAST(1 AS BIT),
			stepexecutionnotes = null;
																 
		GET diagnostics affectedrows = row_count;
																 
		/*log activity*/
		INSERT INTO cs_dw.ipauditactivitylog
		(
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			recordsaffected,
			issuccessful,
			stepexecutionnotes
		)
		SELECT
			/*variables*/
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			affectedrows /*variable*/,
			issuccessful,
			stepexecutionnotes
		FROM
			variables;

		/*set logging variables for current step_start*/
		UPDATE variables
		SET
			stepid = 2,
			stepdescription = 'updateexistingipuserauditrecords',
			stepstartdatetime = CURRENT_TIMESTAMP;

		UPDATE cs_dw.insuranceprovideruseraudit
			SET
				userid = source.userid,
				username = source.username,
				userjobclassification = source.userjobclassification,
				usercompanycode = source.usercompanycode,
				companycode = source.companycode,
				companyname = source.companyname,
				groupcode = source.groupcode,
				groupname = source.groupname,
				officecode = source.officecode,
				officelocationfordisplay = source.officelocationfordisplay,
				ipaddress = source.ipaddress,
				claimsearchid = source.claimsearchid,
				reportid = source.reportid,
				accesscategory = source.accesscategory,
				accessmessage = source.accessmessage,
				searchcriteria = source.searchcriteria,
				accessurl = source.accessurl,
				accessdate = source.accessdate,
				deltadate = variables.dateinserted /*date the charge was inserted*/
		FROM
			ipuserauditdatatoimport AS source
			inner join variables
				ON 1=1
		WHERE
			insuranceprovideruseraudit.auditid = source.existingauditid
			and 
			(
				/*auditid*/
				COALESCE(insuranceprovideruseraudit.userid,'null') <> COALESCE(source.userid,'null')
				or COALESCE(insuranceprovideruseraudit.username,'null') <> COALESCE(source.username,'null')
				or COALESCE(insuranceprovideruseraudit.userjobclassification,'null') <> COALESCE(source.userjobclassification,'null')
				or COALESCE(insuranceprovideruseraudit.usercompanycode,'null') <> COALESCE(source.usercompanycode,'null')
				or COALESCE(insuranceprovideruseraudit.companycode,'null') <> COALESCE(source.companycode,'null')
				or COALESCE(insuranceprovideruseraudit.companyname,'null') <> COALESCE(source.companyname,'null')
				or COALESCE(insuranceprovideruseraudit.groupcode,'null') <> COALESCE(source.groupcode,'null')
				or COALESCE(insuranceprovideruseraudit.groupname,'null') <> COALESCE(source.groupname,'null')
				or COALESCE(insuranceprovideruseraudit.officecode,'null') <> COALESCE(source.officecode,'null')
				or COALESCE(insuranceprovideruseraudit.officelocationfordisplay,'null') <> COALESCE(source.officelocationfordisplay,'null')
				or COALESCE(insuranceprovideruseraudit.ipaddress,'null') <> COALESCE(source.ipaddress,'null')
				or COALESCE(insuranceprovideruseraudit.claimsearchid,'null') <> COALESCE(source.claimsearchid,'null')
				or COALESCE(insuranceprovideruseraudit.reportid,'null') <> COALESCE(source.reportid,'null')
				or COALESCE(insuranceprovideruseraudit.accesscategory,'null') <> COALESCE(source.accesscategory,'null')
				or COALESCE(insuranceprovideruseraudit.accessmessage,'null') <> COALESCE(source.accessmessage,'null')
				or COALESCE(insuranceprovideruseraudit.searchcriteria,'null') <> COALESCE(source.searchcriteria,'null')
				or COALESCE(insuranceprovideruseraudit.accessurl,'null') <> COALESCE(source.accessurl,'null')
				or COALESCE(insuranceprovideruseraudit.accessdate,CAST('19000101' AS TIMESTAMP)) <> COALESCE(source.accessdate,CAST('19000101' AS TIMESTAMP))
				/*dateinserted*/
				/*deltadate*/
			);


		/*set logging variables for current step_end_success*/
		UPDATE variables
		SET
			stependdatetime = CURRENT_TIMESTAMP,
			/*recordsaffected = row_count,*/
			issuccessful = CAST(1 AS BIT),
			stepexecutionnotes = null;

		GET diagnostics affectedrows = row_count;

		/*log activity*/
		INSERT INTO cs_dw.ipauditactivitylog
		(
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			recordsaffected,
			issuccessful,
			stepexecutionnotes
		)
		SELECT
			/*variables*/
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			affectedrows /*variable*/,
			issuccessful,
			stepexecutionnotes
		FROM
			variables;

		/*set logging variables for current step_start*/
		UPDATE variables
		SET
			stepid = 100,
			stepdescription = 'insertnewipuserauditrecords',
			stepstartdatetime = CURRENT_TIMESTAMP;

		INSERT INTO cs_dw.insuranceprovideruseraudit
		(
			auditid, userid, username, userjobclassification, usercompanycode,
			companycode, companyname, groupcode, groupname, officecode,
			officelocationfordisplay, ipaddress, claimsearchid, reportid, accesscategory,
			accessmessage, searchcriteria, accessurl, accessdate, dateinserted,
			deltadate
		)
		SELECT
			source.auditid,
			source.userid,
			source.username,
			source.userjobclassification,
			source.usercompanycode,
			source.companycode,
			source.companyname,
			source.groupcode,
			source.groupname,
			source.officecode,
			source.officelocationfordisplay,
			source.ipaddress,
			source.claimsearchid,
			source.reportid,
			source.accesscategory,
			source.accessmessage,
			source.searchcriteria,
			source.accessurl,
			source.accessdate,
			variables.dateinserted AS dateinserted,
			variables.dateinserted AS deltadate
		FROM
			ipuserauditdatatoimport AS source
			inner join variables
				ON  1=1
		WHERE
			source.existingauditid is null;
			
		/*set logging variables for current step_end_success*/
		UPDATE variables
		SET
			stependdatetime = CURRENT_TIMESTAMP,
			/*recordsaffected = row_count,*/
			issuccessful = CAST(1 as bit),
			stepexecutionnotes = null;

		GET diagnostics affectedrows = row_count;

		/*log activity*/
		INSERT INTO cs_dw.ipauditactivitylog
		(
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			recordsaffected,
			issuccessful,
			stepexecutionnotes
		)
		SELECT
			/*variables*/
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			affectedrows /*variable*/,
			issuccessful,
			stepexecutionnotes
		FROM
			variables;
		/*******************exception********************
		/*mssql version of transafesprocs for atomic(acid) data principles*//*
			if (@internaltransactioncount = 1)
			begin
				rollback transaction;
			end
		*/
				
		/*set logging variables for current step_end_fail*/
		select
			stependdatetime = current_timestamp,
			recordsaffected = row_count,
			issuccessful = 0,
			stepexecutionnotes = 'error: todo: expose\track postgresql error messages.';
		/*log activity*/
		insert into cs_dw.ipauditactivitylog
		(
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			recordsaffected,
			issuccessful,
			stepexecutionnotes
		)
		select
			productcode,
			sourcedatetime,
			executiondatetime,
			stepid,
			stepdescription,
			stepstartdatetime,
			stependdatetime,
			recordsaffected,
			issuccessful,
			stepexecutionnotes;
		/*/*mssql example of optional: we can bubble the error up to the calling level.*/
			if (@internaltransactioncount = 0)
			begin
				declare
					raiserror_message varchar(2045) = /*constructs an intuative error message*/
						'error: in step'
						+ cast(@stepid as varchar(3))
						+ ' ('
						+ stepdescription
						+ ') '
						+ 'of hsp_updateipuseraudit; errormsg: '
						+ error_message(),
					errorseverity int,
					errorstate int;
				select
					errorseverity = error_severity(),
					errorstate = error_state();
				raiserror(@raiserror_message,@errorseverity,@errorstate);
			end*/
		************************************************/
		/*/*mssql (more specifically verisk-jde) specific code-implementation:
			scheduled jobs require multiple-execute-protection due to the nature of how they are scheduled.*/
			end if;
		*/
	END
$BODY$;
--ROLLBACK TRANSACTION;
COMMIT TRANSACTION;