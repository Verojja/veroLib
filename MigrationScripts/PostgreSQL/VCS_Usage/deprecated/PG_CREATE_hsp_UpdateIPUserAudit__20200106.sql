BEGIN TRANSACTION;
/***********************************************
workitem: isccintel-3266
date: 2019-12-11
author: robert david warner
description: mechanism for data-refresh of the insuranceprovideruseraudit table.

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
			LEFT OUTER JOIN cs_dw.ipu_audit AS existingauditrecord
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

		UPDATE cs_dw.ipu_audit
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
			ipu_audit.auditid = source.existingauditid
			and 
			(
				/*auditid*/
				COALESCE(ipu_audit.userid,'null') <> COALESCE(source.userid,'null')
				or COALESCE(ipu_audit.username,'null') <> COALESCE(source.username,'null')
				or COALESCE(ipu_audit.userjobclassification,'null') <> COALESCE(source.userjobclassification,'null')
				or COALESCE(ipu_audit.usercompanycode,'null') <> COALESCE(source.usercompanycode,'null')
				or COALESCE(ipu_audit.companycode,'null') <> COALESCE(source.companycode,'null')
				or COALESCE(ipu_audit.companyname,'null') <> COALESCE(source.companyname,'null')
				or COALESCE(ipu_audit.groupcode,'null') <> COALESCE(source.groupcode,'null')
				or COALESCE(ipu_audit.groupname,'null') <> COALESCE(source.groupname,'null')
				or COALESCE(ipu_audit.officecode,'null') <> COALESCE(source.officecode,'null')
				or COALESCE(ipu_audit.officelocationfordisplay,'null') <> COALESCE(source.officelocationfordisplay,'null')
				or COALESCE(ipu_audit.ipaddress,'null') <> COALESCE(source.ipaddress,'null')
				or COALESCE(ipu_audit.claimsearchid,'null') <> COALESCE(source.claimsearchid,'null')
				or COALESCE(ipu_audit.reportid,'null') <> COALESCE(source.reportid,'null')
				or COALESCE(ipu_audit.accesscategory,'null') <> COALESCE(source.accesscategory,'null')
				or COALESCE(ipu_audit.accessmessage,'null') <> COALESCE(source.accessmessage,'null')
				or COALESCE(ipu_audit.searchcriteria,'null') <> COALESCE(source.searchcriteria,'null')
				or COALESCE(ipu_audit.accessurl,'null') <> COALESCE(source.accessurl,'null')
				or COALESCE(ipu_audit.accessdate,CAST('19000101' AS TIMESTAMP)) <> COALESCE(source.accessdate,CAST('19000101' AS TIMESTAMP))
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

		INSERT INTO cs_dw.ipu_audit
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
/*
commit

query returned successfully in 220 msec.
*/