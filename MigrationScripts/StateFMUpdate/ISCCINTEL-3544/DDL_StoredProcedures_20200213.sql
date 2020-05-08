SET NOEXEC OFF;

USE ClaimSearch_Dev;
--USE ClaimSearch_Prod;

/******MSGLog Snippet. Can be added to comment block at end of query after execute for recordkeeping.******/
DECLARE @tab CHAR(1) = CHAR(9);
DECLARE @newLine CHAR(2) = CHAR(13) + CHAR(10);
DECLARE @currentDBEnv VARCHAR(100) = CAST(@@SERVERNAME + '.' + DB_NAME() AS VARCHAR(100));
DECLARE @currentUser VARCHAR(100) = CAST(CURRENT_USER AS VARCHAR(100));
DECLARE @executeTimestamp VARCHAR(20) = CAST(GETDATE() AS VARCHAR(20));
Print '*****************************************' + @newLine
	+ '*' + @tab + 'Env: ' + 
	+ CASE
	WHEN
		LEN(@currentDBEnv) >=27
	THEN
		@currentDBEnv
	ELSE
		@currentDBEnv + @tab
	END
	+ @tab + '*' +@newLine
	+ '*' + @tab + 'User: ' + @currentUser + @tab + @tab + @tab + @tab + '*' +@newLine
	+ '*' + @tab + 'Time: ' + @executeTimestamp + @tab + @tab + @tab + '*' +@newLine
	+'*****************************************';
/**********************************************************************************************************/
BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-02-20
Author: Dan Ravaglia and Robert David Warner
Description: Mechanism for data-refresh of the Adjuster Table.
			
			Performance:

************************************************
WorkItem: ISCCINTEL-3544
Date: 2020-01-30
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
			
			Performance: No current notes.

************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertAdjuster
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.AdjusterActivityLog
				WHERE
					AdjusterActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND AdjusterActivityLog.isSuccessful = 1
					AND AdjusterActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(AdjusterActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.AdjusterActivityLog
			WHERE
				AdjusterActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND AdjusterActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureAdjusterDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingAdjuster.adjusterId AS existingAdjusterId, /*surrogate id*/
				CLT00002.I_ALLCLM AS isoClaimId,
				CLT00002.N_ADJ_SEQ AS adjusterSequenceId,
				NULLIF(LTRIM(RTRIM(CLT00002.I_CUST)),'') AS adjusterCompanyCode,
				NULLIF(LTRIM(RTRIM(CompanyHeirarchy.Customer_lvl0)),'') AS adjusterCompanyName,
				NULLIF(LTRIM(RTRIM(CLT00002.I_REGOFF)),'') AS adjusterOfficeCode,
				CLT00002.D_ADJ_SUBM AS adjusterDateSubmitted,
				NULLIF(LTRIM(RTRIM(CLT00002.M_FUL_NM)),'') AS adjusterName,
				CAST(
					ISNULL(
						NULLIF(
							RIGHT(
								'000' + LTRIM( RTRIM(
									CAST(
										CLT00002.N_AREA AS CHAR(3)
									)
								)),
								3
							),
							'000'
						),
						''
					) + 
					NULLIF(
						RIGHT(
							'0000000' + LTRIM(RTRIM(
								CAST(
									CLT00002.N_TEL AS CHAR(7)
								)
							)),
							7
						),
						'0000000'
					)
					AS VARCHAR(10)
				) AS adjusterPhoneNumber
				/*isActive,*/
				/*dateInserted,*/
				/*deltaDate,*/
				INTO #AdjusterData
			FROM
				dbo.FireMarshalDriver WITH (NOLOCK)
				INNER JOIN ClaimSearch_Prod.dbo.CLT00002 WITH (NOLOCK)
					ON FireMarshalDriver.isoClaimId = CLT00002.I_ALLCLM
				INNER JOIN ClaimSearch_Prod.dbo.V_MM_Hierarchy AS CompanyHeirarchy WITH (NOLOCK)
					ON CLT00002.I_CUST = CompanyHeirarchy.lvl0
				LEFT OUTER JOIN dbo.V_ActiveAdjuster AS ExistingAdjuster WITH (NOLOCK)
					ON CLT00002.I_ALLCLM = ExistingAdjuster.isoClaimId
						AND CLT00002.N_ADJ_SEQ = ExistingAdjuster.adjusterSequenceId
			WHERE
				/*Deprecating due to performance costs, and current profile state. RDW 20190328:
					NULLIF(LTRIM(RTRIM(CLT00002.I_ALLCLM)),'') IS NOT NULL
				*/
				CLT00002.Date_Insert >= CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
					'-','')
					AS INT
				);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AdjusterActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'UpdateExistingAdjusterData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Adjuster WITH (TABLOCKX)
				SET
					/*Adjuster.adjusterId*/
					/*Adjuster.isoClaimId*/
					/*Adjuster.adjusterSequenceId*/
					Adjuster.adjusterCompanyCode = SOURCE.adjusterCompanyCode,
					Adjuster.adjusterCompanyName = SOURCE.adjusterCompanyName,
					Adjuster.adjusterOfficeCode = SOURCE.adjusterOfficeCode,
					Adjuster.adjusterDateSubmitted = SOURCE.adjusterDateSubmitted,
					Adjuster.adjusterName = SOURCE.adjusterName,
					Adjuster.adjusterPhoneNumber = SOURCE.adjusterPhoneNumber,
					/*Adjuster.isActive*/
					/*Adjuster.dateInserted*/
					Adjuster.deltaDate = @dateInserted
			FROM
				#AdjusterData AS SOURCE
			WHERE
				SOURCE.existingAdjusterId IS NOT NULL
				AND Adjuster.isActive = CAST(1 AS BIT)
				AND Adjuster.adjusterId = SOURCE.existingAdjusterId /*adjusterId is determined through isoClaimId & adjusterSequenceId match*/
				AND 
				(
					Adjuster.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(Adjuster.adjusterCompanyCode,'~~~~') <> ISNULL(SOURCE.adjusterCompanyCode,'~~~~')
					OR ISNULL(Adjuster.adjusterCompanyName,'~~~') <> ISNULL(SOURCE.adjusterCompanyName,'~~~')
					OR ISNULL(Adjuster.adjusterOfficeCode,'~~~~~') <> ISNULL(SOURCE.adjusterOfficeCode,'~~~~~')
					OR ISNULL(Adjuster.adjusterDateSubmitted,CAST('19000115' AS DATE)) <> ISNULL(SOURCE.adjusterDateSubmitted,CAST('19000115' AS DATE))
					OR ISNULL(Adjuster.adjusterName,'~~~') <> ISNULL(SOURCE.adjusterName,'~~~')
					OR ISNULL(Adjuster.adjusterPhoneNumber,'-1') <> ISNULL(SOURCE.adjusterPhoneNumber,'-1')
				);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AdjusterActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'InsertNewAdjusterData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Adjuster WITH (TABLOCKX)
			(
				/*adjusterId*/
				isoClaimId,
				adjusterSequenceId,
				adjusterCompanyCode,
				adjusterCompanyName,
				adjusterOfficeCode,
				adjusterDateSubmitted,
				adjusterName,
				adjusterPhoneNumber,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				SOURCE.isoClaimId,
				SOURCE.adjusterSequenceId,
				SOURCE.adjusterCompanyCode,
				SOURCE.adjusterCompanyName,
				SOURCE.adjusterOfficeCode,
				SOURCE.adjusterDateSubmitted,
				SOURCE.adjusterName,
				SOURCE.adjusterPhoneNumber,
				1 AS isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#AdjusterData AS SOURCE
			WHERE
				SOURCE.adjusterId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AdjusterActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'DeprecateOrphanISOClaimData_adj',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.Adjuster WITH (TABLOCKX)
				SET
					/*Adjuster.adjusterId*/
					/*Adjuster.isoClaimId*/
					/*Adjuster.adjusterSequenceId*/
					Adjuster.isActive = CAST(0 AS BIT),
					Adjuster.deltaDate = @dateInserted
			FROM
				#AdjusterData AS SOURCE
			WHERE
				Adjuster.isoClaimId = SOURCE.isoClaimId /*adjusterId is determined through isoClaimId & adjusterSequenceId match*/
				AND Adjuster.isActive = CAST(1 AS BIT)
				AND Adjuster.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AdjusterActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();

		/*Log Activity*/
		INSERT INTO dbo.AdjusterActivityLog
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
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertAdjuster; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-08
Author: Robert David Warner && Dan Ravaglia
Description: Mechanism for data-refresh of the Address Table.
			
			Performance: Joins between tables of 5+ billion rows.
				1+ billion rows, and 500,000,000 rows.
				Explore partitioning.
				Explore LOCK_ESCALATION at the partition level 
					set the LOCK_ESCALATION option of the ALTER TABLE statement to AUTO.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
			NOTE: Sanitized\Scrubbed-Address-data for NON-LocationOfLoss is currently not being imported\updated due to performance reasons.
			
			Performance: Significant performance improvements on one-query(with-date-calc)
							over cross-db-query-with-index-added (40 sec vs. 30 min in worst case).
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertAddress
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.AddressActivityLog  WITH (NOLOCK)
				WHERE
					AddressActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND AddressActivityLog.isSuccessful = 1
					AND AddressActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/

				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(AddressActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2014-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.AddressActivityLog  WITH (NOLOCK)
			WHERE
				AddressActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND AddressActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureLocationOfLossDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingAddress.addressId AS existingAddressId,
				CAST(LTRIM(RTRIM(CLT00001.I_ALLCLM)) AS VARCHAR(11)) AS isoClaimId,
				/*involvedPartySequenceId*/
				/*isLocationOfLoss*/
				CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR1)),'') AS VARCHAR(50))AS originalAddressLine1,
				CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR2)),'') AS VARCHAR(50))AS originalAddressLine2,
				CAST(NULLIF(LTRIM(RTRIM(CLT00001.M_LOL_CITY)),'') AS VARCHAR(25))AS originalCityName,
				CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ST_ALPH)),'') AS CHAR(2))AS originalStateCode,
				CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ZIP)),'') AS VARCHAR(9))AS originalZipCode,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Address)),'') AS VARCHAR(50))AS scrubbedAddressLine1,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Address2)),'') AS VARCHAR(50))AS scrubbedAddressLine2,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_City)),'') AS VARCHAR(25))AS scrubbedCityName,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_State)),'') AS CHAR(2))AS scrubbedStateCode,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Zip)),'') AS CHAR(5))AS scrubbedZipCode,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Plus4)),'') AS CHAR(4))AS scrubbedZipCodeExtended,
				CAST(
					COALESCE
					(
						NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_AddrCountyName)),''),
						NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_GeoCounty)),'')
					) AS VARCHAR(25)
				) AS scrubbedCountyName,
				CAST(
					COALESCE
					(
						NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_AddrCountyFIPS)),''),
						NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_GeoCountyFIPS)),'')
					) AS VARCHAR(25)
				) AS scrubbedCountyFIPS,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Country)),'') AS VARCHAR(3))AS scrubbedCountryCode,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Latitude)),'') AS VARCHAR(15))AS latitude,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Longitude)),'') AS VARCHAR(15))AS longitude,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.MD_Results)),'') AS VARCHAR(15))AS geoAccuracy,
				CAST(CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey AS BIGINT) AS melissaMappingKey
				/*isActive*/
				/*dateInserted*/
				/*deltaDate*/
				INTO #LocationOfLossData
			FROM			
				dbo.FireMarshalDriver WITH (NOLOCK)
				INNER JOIN ClaimSearch_Prod.dbo.CLT00001 WITH (NOLOCK)
					ON FireMarshalDriver.isoClaimId = CLT00001.I_ALLCLM
				LEFT OUTER JOIN dbo.V_ActiveLocationOfLoss AS ExistingAddress WITH (NOLOCK)
					ON CLT00001.I_ALLCLM = ExistingAddress.isoClaimId
				/*Re-Adding Melissa Scrubbing table due to StateFM Requirements*/
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 WITH (NOLOCK)
					ON CLT00001.ALLCLMROWID = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.ALLCLMROWID
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
					ON CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
			WHERE
				CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
						'-',
						''
					)
					AS INT
				) <= CASE
					WHEN /*when the scrub data is "more new" than the claimData, operate on scrub DATE*/
						CLT00001.Date_Insert <=
							CAST(
								REPLACE(
									CAST(
										CS_Lookup_Unique_Addresses_Melissa_Output.Date_Insert
										AS VARCHAR(10)
									),
								'-','')
							AS INT)
					THEN
						CAST(
							REPLACE(
								CAST(
									CS_Lookup_Unique_Addresses_Melissa_Output.Date_Insert
									AS VARCHAR(10)
								),
							'-','')
						AS INT)
					ELSE /*operate on claim date*/
						CLT00001.Date_Insert
				END;
	

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'UpdateExistingLocationOfLossData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Address WITH (TABLOCKX)
				SET
					/*addressId*/
					/*isoClaimId*/
					/*involvedPartySequenceId*/
					/*isLocationOfLoss*/
					Address.originalAddressLine1 = SOURCE.originalAddressLine1,
					Address.originalAddressLine2 = SOURCE.originalAddressLine2,
					Address.originalCityName = SOURCE.originalCityName,
					Address.originalStateCode = SOURCE.originalStateCode,
					Address.originalZipCode = SOURCE.originalZipCode,
					Address.scrubbedAddressLine1 = SOURCE.scrubbedAddressLine1,
					Address.scrubbedAddressLine2 = SOURCE.scrubbedAddressLine2,
					Address.scrubbedCityName = SOURCE.scrubbedCityName,
					Address.scrubbedStateCode = SOURCE.scrubbedStateCode,
					Address.scrubbedZipCode = SOURCE.scrubbedZipCode,
					Address.scrubbedZipCodeExtended = SOURCE.scrubbedZipCodeExtended,
					Address.scrubbedCountyName = SOURCE.scrubbedCountyName,
					Address.scrubbedCountyFIPS = SOURCE.scrubbedCountyFIPS,
					Address.scrubbedCountryCode = SOURCE.scrubbedCountryCode,
					Address.latitude = SOURCE.latitude,
					Address.longitude = SOURCE.longitude,
					Address.geoAccuracy = SOURCE.geoAccuracy,
					Address.melissaMappingKey = SOURCE.melissaMappingKey,
					/*isActive*/
					/*dateInserted*/
					Address.deltaDate = @dateInserted
			FROM
				#LocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId IS NOT NULL
				AND Address.isActive = CAST(1 AS BIT)
				AND Address.addressId = SOURCE.existingAddressId
				AND 
				(
					Address.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(Address.originalAddressLine1,'') <> ISNULL(SOURCE.originalAddressLine1,'')
					OR ISNULL(Address.originalAddressLine2,'') <> ISNULL(SOURCE.originalAddressLine2,'')
					OR ISNULL(Address.originalCityName,'') <> ISNULL(SOURCE.originalCityName,'')
					OR ISNULL(Address.originalStateCode,'') <> ISNULL(SOURCE.originalStateCode,'')
					OR ISNULL(Address.originalZipCode,'') <> ISNULL(SOURCE.originalZipCode,'')
					OR ISNULL(Address.scrubbedAddressLine1,'') <> ISNULL(SOURCE.scrubbedAddressLine1,'')
					OR ISNULL(Address.scrubbedAddressLine2,'') <> ISNULL(SOURCE.scrubbedAddressLine2,'')
					OR ISNULL(Address.scrubbedCityName,'') <> ISNULL(SOURCE.scrubbedCityName,'')
					OR ISNULL(Address.scrubbedStateCode,'') <> ISNULL(SOURCE.scrubbedStateCode,'')
					OR ISNULL(Address.scrubbedZipCode,'') <> ISNULL(SOURCE.scrubbedZipCode,'')
					OR ISNULL(Address.scrubbedZipCodeExtended,'') <> ISNULL(SOURCE.scrubbedZipCodeExtended,'')
					OR ISNULL(Address.scrubbedCountyName,'') <> ISNULL(SOURCE.scrubbedCountyName,'')
					OR ISNULL(Address.scrubbedCountyFIPS,'') <> ISNULL(SOURCE.scrubbedCountyFIPS,'')
					OR ISNULL(Address.scrubbedCountryCode,'') <> ISNULL(SOURCE.scrubbedCountryCode,'')
					OR ISNULL(Address.longitude,'') <> ISNULL(SOURCE.longitude,'')
					OR ISNULL(Address.latitude,'') <> ISNULL(SOURCE.latitude,'')
					OR ISNULL(Address.geoAccuracy,'') <> ISNULL(SOURCE.geoAccuracy,'')
					OR ISNULL(Address.melissaMappingKey,-1) <> ISNULL(SOURCE.melissaMappingKey,-1)
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'InsertNewLocationOfLossData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Address WITH (TABLOCKX)
			(
				/*addressId,*/
				isoClaimId,
				involvedPartySequenceId,
				isLocationOfLoss,
				originalAddressLine1,
				originalAddressLine2,
				originalCityName,
				originalStateCode,
				originalZipCode,
				scrubbedAddressLine1,
				scrubbedAddressLine2,
				scrubbedCityName,
				scrubbedStateCode,
				scrubbedZipCode,
				scrubbedZipCodeExtended,
				scrubbedCountyName,
				scrubbedCountyFIPS,
				scrubbedCountryCode,
				latitude,
				longitude,
				geoAccuracy,
				melissaMappingKey,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				SOURCE.isoClaimId,
				CAST(NULL AS INT) AS involvedPartySequenceId,
				CAST(1 AS BIT) AS isLocationOfLoss,
				SOURCE.originalAddressLine1,
				SOURCE.originalAddressLine2,
				SOURCE.originalCityName,
				SOURCE.originalStateCode,
				SOURCE.originalZipCode,
				SOURCE.scrubbedAddressLine1,
				SOURCE.scrubbedAddressLine2,
				SOURCE.scrubbedCityName,
				SOURCE.scrubbedStateCode,
				SOURCE.scrubbedZipCode,
				SOURCE.scrubbedZipCodeExtended,
				SOURCE.scrubbedCountyName,
				SOURCE.scrubbedCountyFIPS,
				SOURCE.scrubbedCountryCode,
				SOURCE.latitude,
				SOURCE.longitude,
				SOURCE.geoAccuracy,
				SOURCE.melissaMappingKey,
				CAST(1 AS BIT) AS isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#LocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 4,
				@stepDescription = 'DeprecateOrphanISOClaimData_LOL',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Address WITH (TABLOCKX)
				SET
					Address.isActive = CAST(0 AS BIT),
					Address.deltaDate = @dateInserted
			FROM
				#LocationOfLossData AS SOURCE
			WHERE
				Address.isoClaimId = SOURCE.isoClaimId
				AND Address.isLocationOfLoss = CAST(1 AS BIT)
				AND Address.isActive = CAST(1 AS BIT)
				AND Address.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 5,
				@stepDescription = 'CaptureNoNLocationOfLossDataToImport',
				@stepStartDateTime = GETDATE();
		
			SELECT
				ExistingAddress.addressId AS existingAddressId,
				CAST(LTRIM(RTRIM(CLT00004.I_ALLCLM)) AS VARCHAR(11)) AS isoClaimId,
				CAST(CLT00004.I_NM_ADR AS INT) AS involvedPartySequenceId,
				/*isLocationOfLoss*/
				CAST(NULLIF(LTRIM(RTRIM(CLT00004.T_ADR_LN1)),'') AS VARCHAR(50))AS originalAddressLine1,
				CAST(NULLIF(LTRIM(RTRIM(CLT00004.T_ADR_LN2)),'') AS VARCHAR(50))AS originalAddressLine2,
				CAST(NULLIF(LTRIM(RTRIM(CLT00004.M_CITY)),'') AS VARCHAR(25))AS originalCityName,
				CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_ST_ALPH)),'') AS CHAR(2))AS originalStateCode,
				CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_ZIP)),'') AS VARCHAR(9))AS originalZipCode,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(50))AS scrubbedAddressLine1,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(50))AS scrubbedAddressLine2,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(25))AS scrubbedCityName,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(2))AS scrubbedStateCode,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(5))AS scrubbedZipCode,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS CHAR(4))AS scrubbedZipCodeExtended,
				CAST(
					COALESCE
					(
						NULLIF(LTRIM(RTRIM(NULL)),''),
						NULLIF(LTRIM(RTRIM(NULL)),'')
					) AS VARCHAR(25)
				) AS scrubbedCountyName,
				CAST(
					COALESCE
					(
						NULLIF(LTRIM(RTRIM(NULL)),''),
						NULLIF(LTRIM(RTRIM(NULL)),'')
					) AS VARCHAR(25)
				) AS scrubbedCountyFIPS,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(3))AS scrubbedCountryCode,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(15))AS latitude,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(15))AS longitude,
				CAST(NULLIF(LTRIM(RTRIM(NULL)),'') AS VARCHAR(15))AS geoAccuracy,
				CAST(NULL AS BIGINT) AS melissaMappingKey
				/*isActive*/
				/*dateInserted*/
				/*deltaDate*/
				INTO #NonLocationOfLossData
			FROM
				dbo.FireMarshalDriver WITH (NOLOCK)
				INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
					ON FireMarshalDriver.isoClaimId = CLT00004.I_ALLCLM
				LEFT OUTER JOIN dbo.V_ActiveNonLocationOfLoss AS ExistingAddress WITH (NOLOCK)
					ON CLT00004.I_ALLCLM  = ExistingAddress.isoClaimId
						AND CLT00004.I_NM_ADR = ExistingAddress.involvedPartySequenceId
				/*Deprecated for performance reasons back in 20190101 RDW
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
					ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
					ON CS_Lookup_EntityIDs.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
				*/
			WHERE
				/*Deprecating due to performance costs, and current profile state. RDW 20190306:
					NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
					AND CLT00004.I_NM_ADR IS NOT NULL
				*/
				CLT00004.Date_Insert >= CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
					'-','')
					AS INT
				);
		
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 6,
				@stepDescription = 'UpdateExistingNonLocationOfLossData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Address WITH (TABLOCKX)
				SET
					/*addressId*/
					/*isoClaimId*/
					/*involvedPartySequenceId*/
					/*isLocationOfLoss*/
					Address.originalAddressLine1 = SOURCE.originalAddressLine1,
					Address.originalAddressLine2 = SOURCE.originalAddressLine2,
					Address.originalCityName = SOURCE.originalCityName,
					Address.originalStateCode = SOURCE.originalStateCode,
					Address.originalZipCode = SOURCE.originalZipCode,
					/*Address.scrubbedAddressLine1 = SOURCE.scrubbedAddressLine1,
					Address.scrubbedAddressLine2 = SOURCE.scrubbedAddressLine2,
					Address.scrubbedCityName = SOURCE.scrubbedCityName,
					Address.scrubbedStateCode = SOURCE.scrubbedStateCode,
					Address.scrubbedZipCode = SOURCE.scrubbedZipCode,
					Address.scrubbedZipCodeExtended = SOURCE.scrubbedZipCodeExtended,
					Address.scrubbedCountyName = SOURCE.scrubbedCountyName,
					Address.scrubbedCountyFIPS = SOURCE.scrubbedCountyFIPS,
					Address.scrubbedCountryCode = SOURCE.scrubbedCountryCode,
					Address.latitude = SOURCE.latitude,
					Address.longitude = SOURCE.longitude,
					Address.geoAccuracy = SOURCE.geoAccuracy,
					Address.melissaMappingKey = SOURCE.melissaMappingKey,*/
					Address.deltaDate = @dateInserted
			FROM
				#NonLocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId IS NOT NULL
				AND Address.isActive = CAST(1 AS BIT)
				AND Address.addressId = SOURCE.existingAddressId
				AND 
				(
					Address.deltaDate <> @dateInserted
					OR ISNULL(Address.originalAddressLine1,'') <> ISNULL(SOURCE.originalAddressLine1,'')
					OR ISNULL(Address.originalAddressLine2,'') <> ISNULL(SOURCE.originalAddressLine2,'')
					OR ISNULL(Address.originalCityName,'') <> ISNULL(SOURCE.originalCityName,'')
					OR ISNULL(Address.originalStateCode,'') <> ISNULL(SOURCE.originalStateCode,'')
					OR ISNULL(Address.originalZipCode,'') <> ISNULL(SOURCE.originalZipCode,'')
					/*OR ISNULL(Address.scrubbedAddressLine1,'') <> ISNULL(SOURCE.scrubbedAddressLine1,'')
					OR ISNULL(Address.scrubbedAddressLine2,'') <> ISNULL(SOURCE.scrubbedAddressLine2,'')
					OR ISNULL(Address.scrubbedCityName,'') <> ISNULL(SOURCE.scrubbedCityName,'')
					OR ISNULL(Address.scrubbedStateCode,'') <> ISNULL(SOURCE.scrubbedStateCode,'')
					OR ISNULL(Address.scrubbedZipCode,'') <> ISNULL(SOURCE.scrubbedZipCode,'')
					OR ISNULL(Address.scrubbedZipCodeExtended,'') <> ISNULL(SOURCE.scrubbedZipCodeExtended,'')*/
					/*OR ISNULL(Address.scrubbedCountyName,'') <> ISNULL(SOURCE.scrubbedCountyName,'')*/
					/*OR ISNULL(Address.scrubbedCountyFIPS,'') <> ISNULL(SOURCE.scrubbedCountyFIPS,'')*/
					/*OR ISNULL(Address.scrubbedCountryCode,'') <> ISNULL(SOURCE.scrubbedCountryCode,'')*/
					/*OR ISNULL(Address.longitude,'') <> ISNULL(SOURCE.longitude,'')
					/*OR ISNULL(Address.latitude,'') <> ISNULL(SOURCE.latitude,'')
					OR ISNULL(Address.geoAccuracy,'') <> ISNULL(SOURCE.geoAccuracy,'')
					OR ISNULL(Address.melissaMappingKey,-1) <> ISNULL(SOURCE.melissaMappingKey,-1)*/*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 7,
				@stepDescription = 'InsertNewNonLocationOfLossData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Address WITH (TABLOCKX)
			(
				/*addressId,*/
				isoClaimId,
				involvedPartySequenceId,
				isLocationOfLoss,
				originalAddressLine1,
				originalAddressLine2,
				originalCityName,
				originalStateCode,
				originalZipCode,
				scrubbedAddressLine1,
				scrubbedAddressLine2,
				scrubbedCityName,
				scrubbedStateCode,
				scrubbedZipCode,
				scrubbedZipCodeExtended,
				scrubbedCountyName,
				scrubbedCountyFIPS,
				scrubbedCountryCode,
				latitude,
				longitude,
				geoAccuracy,
				melissaMappingKey,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				CAST(0 AS BIT) AS isLocationOfLoss,
				SOURCE.originalAddressLine1,
				SOURCE.originalAddressLine2,
				SOURCE.originalCityName,
				SOURCE.originalStateCode,
				SOURCE.originalZipCode,
				SOURCE.scrubbedAddressLine1,
				SOURCE.scrubbedAddressLine2,
				SOURCE.scrubbedCityName,
				SOURCE.scrubbedStateCode,
				SOURCE.scrubbedZipCode,
				SOURCE.scrubbedZipCodeExtended,
				SOURCE.scrubbedCountyName,
				SOURCE.scrubbedCountyFIPS,
				SOURCE.scrubbedCountryCode,
				SOURCE.latitude,
				SOURCE.longitude,
				SOURCE.geoAccuracy,
				SOURCE.melissaMappingKey,
				CAST(1 AS BIT) AS isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#NonLocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'DeprecateOrphanISOClaimData_NonLOL',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Address WITH (TABLOCKX)
				SET
					Address.isActive = CAST(0 AS BIT),
					Address.deltaDate = @dateInserted
			FROM
				#NonLocationOfLossData AS SOURCE
			WHERE
				Address.isoClaimId = SOURCE.isoClaimId
				AND Address.isLocationOfLoss = CAST(0 AS BIT)
				AND Address.isActive = CAST(1 AS BIT)
				AND Address.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();

		/*Log Activity*/
		INSERT INTO dbo.AddressActivityLog
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
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertAddress; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 20190225
Author: Robert David Warner
Description: Mechanism for data-refresh of the Policy Table
			
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
			
			Performance: No current notes.
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertPolicy
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.PolicyActivityLog
				WHERE
					PolicyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND PolicyActivityLog.isSuccessful = 1
					AND PolicyActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(PolicyActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.PolicyActivityLog
			WHERE
				PolicyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND PolicyActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CapturePolicyDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingPolicy.policyId AS existingPolicyId,
				CLT00001.I_ALLCLM AS isoClaimId,
				NULLIF(LTRIM(RTRIM(CLT00001.I_CUST)),'') AS insuranceProviderCompanyCode,
				NULLIF(LTRIM(RTRIM(CLT00001.I_REGOFF)),'') AS insuranceProviderOfficeCode,
				ISNULL(LTRIM(RTRIM(CLT00001.N_POL)),'NA') AS originalPolicyNumber,
				NULLIF(LTRIM(RTRIM(CLT00001.C_POL_TYP)),'') AS policyTypeCode,
				NULLIF(LTRIM(RTRIM(UniquePolicyTypeInstance.policyTypeDescription)),'') AS policyTypeDescription,
				NULLIF(LTRIM(RTRIM(CLT00001.D_POL_INCP)),'') AS originalPolicyInceptionDate,
				CLT00001.D_POL_EXPIR AS originalPolicyExperiationDate
				/*dateInserted,*/
				INTO #PolicyData
			FROM
				dbo.FireMarshalDriver
				INNER JOIN ClaimSearch_Prod.dbo.CLT00001
					ON FireMarshalDriver.isoClaimId = CLT00001.I_ALLCLM
				/*The following DupRemovalPartition appears unnecessary, but it might have
					been required back in early 2019 - RDW*/
				LEFT OUTER JOIN
				(
					SELECT
						COALESCE(Dim_Policy_Type.C_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP) AS policyTypeCode,
						COALESCE(Dim_Policy_Type.T_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP_DESC) AS policyTypeDescription,
						ROW_NUMBER() OVER(
							PARTITION BY
								COALESCE(Dim_Policy_Type.C_POL_TYP, Lookup_Pol_Type_Code.C_POL_TYP)
							ORDER BY
								COALESCE(Dim_Policy_Type.Date_Insert,'20000101') DESC
						) AS uniqueInstanceValue
					FROM
						ClaimSearch_Prod.dbo.Dim_Policy_Type WITH (NOLOCK)
						FULL OUTER JOIN ClaimSearch_Prod.dbo.Lookup_Pol_Type_Code WITH (NOLOCK)
							ON Dim_Policy_Type.C_POL_TYP = Lookup_Pol_Type_Code.C_POL_TYP
				) AS UniquePolicyTypeInstance
					ON CLT00001.C_POL_TYP = UniquePolicyTypeInstance.policyTypeCode
				LEFT OUTER JOIN dbo.V_ActivePolicy AS ExistingPolicy
					ON CLT00001.I_ALLCLM = ExistingPolicy.isoClaimId
			WHERE
				/*Deprecating due to performance costs, and current profile state. RDW 20190328:
					NULLIF(LTRIM(RTRIM(CLT00001.I_ALLCLM)),'') IS NOT NULL
					additionally: current count for CLT00001.Date_Insert IS NULL is 0, 20190401
				*/
				UniquePolicyTypeInstance.uniqueInstanceValue = 1
				AND CLT00001.Date_Insert >= CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
					'-','')
					AS INT
				);

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.PolicyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'UpdateExistingPolicy',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Policy WITH (TABLOCKX)
				SET
					/*policyId*/
					/*isoClaimId*/
					Policy.insuranceProviderCompanyCode = SOURCE.insuranceProviderCompanyCode,
					Policy.insuranceProviderOfficeCode = SOURCE.insuranceProviderOfficeCode,
					Policy.originalPolicyNumber = SOURCE.originalPolicyNumber,
					Policy.policyTypeCode = SOURCE.policyTypeCode,
					Policy.policyTypeDescription = SOURCE.policyTypeDescription,
					Policy.originalPolicyInceptionDate = SOURCE.originalPolicyInceptionDate,
					Policy.originalPolicyExperiationDate = SOURCE.originalPolicyExperiationDate,
					/*isActive,*/
					/*dateInserted*/
					Policy.deltaDate = @dateInserted
			FROM
				#PolicyData AS SOURCE
			WHERE
				SOURCE.existingPolicyId IS NOT NULL
				AND Policy.isActive = CAST(1 AS BIT)
				AND Policy.policyId = SOURCE.existingPolicyId
				AND 
				(
					/*policyId*/
					/*isoClaimId*/
					Policy.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(Policy.insuranceProviderCompanyCode,'') <> ISNULL(SOURCE.insuranceProviderCompanyCode,'')
					OR ISNULL(Policy.insuranceProviderOfficeCode,'') <> ISNULL(SOURCE.insuranceProviderOfficeCode,'')
					OR ISNULL(Policy.originalPolicyNumber,'') <> ISNULL(SOURCE.originalPolicyNumber,'')
					OR ISNULL(Policy.policyTypeCode,'') <> ISNULL(SOURCE.policyTypeCode,'')
					OR ISNULL(Policy.policyTypeDescription,'') <> ISNULL(SOURCE.policyTypeDescription,'')
					OR ISNULL(Policy.originalPolicyInceptionDate,'') <> ISNULL(SOURCE.originalPolicyInceptionDate,'')
					OR ISNULL(Policy.originalPolicyExperiationDate,'') <> ISNULL(SOURCE.originalPolicyExperiationDate,'')
					/*isActive,*/
					/*dateInserted*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.PolicyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'InsertNewPolicyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Policy WITH (TABLOCKX)
			(
				/*policyId*/
				isoClaimId,
				insuranceProviderCompanyCode,
				insuranceProviderOfficeCode,
				originalPolicyNumber,
				policyTypeCode,
				policyTypeDescription,
				originalPolicyInceptionDate,
				originalPolicyExperiationDate,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				SOURCE.isoClaimId,
				SOURCE.insuranceProviderCompanyCode,
				SOURCE.insuranceProviderOfficeCode,
				SOURCE.originalPolicyNumber,
				SOURCE.policyTypeCode,
				SOURCE.policyTypeDescription,
				SOURCE.originalPolicyInceptionDate,
				SOURCE.originalPolicyExperiationDate,
				CAST(1 AS BIT) AS isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#PolicyData AS SOURCE
			WHERE
				SOURCE.existingPolicyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.PolicyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'DeprecateOrphanISOClaimData_pol',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.Policy WITH (TABLOCKX)
				SET
					Policy.isActive = CAST(0 AS BIT),
					Policy.deltaDate = @dateInserted
			FROM
				#PolicyData AS SOURCE
			WHERE
				Policy.isoClaimId = SOURCE.isoClaimId
				AND Policy.isActive = CAST(1 AS BIT)
				AND Policy.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.PolicyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();

		/*Log Activity*/
		INSERT INTO dbo.PolicyActivityLog
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
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertFMPolicy; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-03-11
Author: Robert David Warner
Description: Mechanism for data-refresh of the Claim Table.
			20190826 - fixed bug related to claimUnderSIUInvestigation RDW
			
			Performance: This SP only upcerts FM claims (for performance reasons) -- (NOTE: unconfirmed-2020-02-13 (RDW))
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertClaim
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.ClaimActivityLog
				WHERE
					ClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND ClaimActivityLog.isSuccessful = 1
					AND ClaimActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(ClaimActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.ClaimActivityLog
			WHERE
				ClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND ClaimActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureClaimDataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingClaim.claimId AS existingClaimId,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.I_ALLCLM)),'') AS VARCHAR(11)) AS isoClaimId,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.N_CLM)),'') AS VARCHAR(30)) AS originalClaimNumber,
				CAST(ExistingLocationOfLoss.addressId AS BIGINT) AS locationOfLossAddressId,
				CAST(ExistingMappedPolicy.policyId AS BIGINT) AS policyId,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.C_CLM_SRCE)),'') AS CHAR(1)) AS claimSearchSourceSystem,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.C_RPT_SRCE)),'') AS CHAR(1)) AS claimEntryMethod,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_VOID)) = 'X'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isVoidedByInsuranceCarrier,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.T_LOSS_DSC)),'') AS VARCHAR(50)) AS lossDescription,
				CAST(NULLIF(RTRIM(LTRIM(CLT0001A.T_LOSS_DSC_EXT)),'') AS VARCHAR(200)) AS lossDescriptionExtended,
				/*Deprecated20190328*//*CAST(NULLIF(RTRIM(LTRIM(CLT0001A.C_CAT)),'') AS CHAR(1)) AS catastropheId,*/
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_PROP)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchProperty,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_AUTO)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchAuto,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_CSLTY)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchCasualty,
				CASE
					WHEN
						RTRIM(LTRIM(CLT0001A.F_APD)) = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimSearchAPD,
				CASE
					WHEN
						COALESCE(
							NULLIF(LTRIM(RTRIM(CLT0001A.M_SIU_CO)),''),
							NULLIF(LTRIM(RTRIM(CLT0001A.M_FUL_NM_SIU)),''),
							NULLIF(
								RIGHT(
									'000' + LTRIM(
										RTRIM(
											CAST(
												CLT0001A.N_AREA_WK_SIU AS CHAR(3)
											)
										)
									),
									3
								),
								'000'
							)
							+ NULLIF(
								RIGHT(
									'0000000' + LTRIM(
										RTRIM(
											CAST(
												CLT0001A.N_TEL_WK_SIU AS CHAR(7)
											)
										)
									),
									7
								),
								'0000000'
							),
							NULLIF(
								RIGHT(
									'000' + LTRIM(
										RTRIM(
											CAST(
												CLT0001A.N_AREA_CELL_SIU AS CHAR(3)
											)
										)
									),
									3
								),
								'000'
							)
							+ NULLIF(
								RIGHT(
									'0000000' + LTRIM(
										RTRIM(
											CAST(
												CLT0001A.N_TEL_CELL_SIU AS CHAR(7)
											)
										)
									),
									7
								),
								'0000000'
							)
						) IS NOT NULL
						OR ClaimUnderSIUInvestigation.isUnderSIUInvestigation = 1
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isClaimUnderSIUInvestigation,
				NULLIF(LTRIM(RTRIM(CLT0001A.M_SIU_CO)),'') AS siuCompanyName,
				NULLIF(LTRIM(RTRIM(CLT0001A.M_FUL_NM_SIU)),'') AS siuRepresentativeFullName,
				NULLIF(
					RIGHT(
						'000' + LTRIM(
							RTRIM(
								CAST(
									CLT0001A.N_AREA_WK_SIU AS CHAR(3)
								)
							)
						),
						3
					),
					'000'
				)
				+ NULLIF(
					RIGHT(
						'0000000' + LTRIM(
							RTRIM(
								CAST(
									CLT0001A.N_TEL_WK_SIU AS CHAR(7)
								)
							)
						),
						7
					),
					'0000000'
				) AS siuWorkPhoneNumber,
				NULLIF(
					RIGHT(
						'000' + LTRIM(
							RTRIM(
								CAST(
									CLT0001A.N_AREA_CELL_SIU AS CHAR(3)
								)
							)
						),
						3
					),
					'000'
				)
				+ NULLIF(
					RIGHT(
						'0000000' + LTRIM(
							RTRIM(
								CAST(
									CLT0001A.N_TEL_CELL_SIU AS CHAR(7)
								)
							)
						),
						7
					),
					'0000000'
				) AS siuCellPhoneNumber,
				CASE
					WHEN
						CLT0001A.D_OCUR IS NULL
					THEN
						CAST(NULL AS DATETIME2(0))
					ELSE
						CASE
							WHEN
								/*ISNULL(
									NULLIF(
										LTRIM(
											RTRIM(
												CLT0001A.H_OCUR
											)
										),
										''
									),
									'NULL'
								) NOT LIKE '[0-9][0-9][0-9][0-9]'
								*/
								ISNULL(
									CLT0001A.H_OCUR,
									'NULL'
								) NOT LIKE '[0-2][0-9][0-9][0-9]'
							THEN
								/*TODO: eventually add cases for the 1689/500,000,000+ rows that don't match the HHMM pattern*/
								CAST(CLT0001A.D_OCUR AS DATETIME2(0))
							ELSE
								CASE
									WHEN
										ISNULL(CLT0001A.F_AM_PM,'DefaultBehavior') = 'P'
										AND
										(
											(
												CAST(
													LEFT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												) + 12 <= 23
											)
											OR
											(
												CAST(
													LEFT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												) + 12 = 24
												AND CAST(
													RIGHT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												) = 0
											)
										)
									THEN
										DATEADD(
											HOUR,
											CAST(
												LEFT(
													CLT0001A.H_OCUR,
													2
												)
												AS SMALLINT
											) + 12,
											DATEADD(
												MINUTE,
												CAST(
													RIGHT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												),
												CAST(CLT0001A.D_OCUR AS DATETIME2(0))
											)
										)
									ELSE
										/*AM or NULL-AMPM, with HHMM*/
										DATEADD(
											HOUR,
												CAST(
													LEFT(
														CLT0001A.H_OCUR,
														2
													)
													AS SMALLINT
												),
												DATEADD(
													MINUTE,
														CAST(
															RIGHT(
																CLT0001A.H_OCUR,
																2
															)
															AS SMALLINT
														)
														,
														CAST(CLT0001A.D_OCUR AS DATETIME2(0))
												)
										)
								END
						END
				END AS dateOfLoss,
				CASE
					WHEN
						LEN(CLT0001A.D_INS_CO_RCV) = 10
					THEN
						CAST(
							REPLACE(CLT0001A.D_INS_CO_RCV,'-','')
							AS DATETIME2(0)
						)
					ELSE
						CAST(NULL AS DATETIME2(0))
				END AS insuranceCompanyReceivedDate,
				CASE
					WHEN
						LEN(CLT0001A.D_RCV) = 26
					THEN
						CAST(
							SUBSTRING(CLT0001A.D_RCV,1,10)
							+ ' '
							+ REPLACE((SUBSTRING(CLT0001A.D_RCV,12,8)),'.',':')
							+ (SUBSTRING(CLT0001A.D_RCV,20,8))
							AS DATETIME2(0)
						)
					ELSE
						CAST(NULL AS DATETIME2(0))
				END AS systemDateReceived
				/*isActive*/
				/*dateInserted*/
				/*deltaDate*/
				INTO #ClaimData
			FROM
				dbo.FireMarshalDriver WITH (NOLOCK)
				INNER JOIN ClaimSearch_Prod.dbo.CLT0001A WITH (NOLOCK)
					ON FireMarshalDriver.isoClaimId = CLT0001A.I_ALLCLM
				LEFT OUTER JOIN dbo.V_ActiveClaim AS ExistingClaim WITH (NOLOCK)
					ON CLT0001A.I_ALLCLM = ExistingClaim.isoClaimId
				LEFT OUTER JOIN
				(
					SELECT
						CLT00004.I_ALLCLM,
						CAST(1 AS BIT) AS isUnderSIUInvestigation,
						ROW_NUMBER() OVER(
							PARTITION BY
								CLT00004.I_ALLCLM
							ORDER BY
								CLT00004.I_ALLCLM
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver AS INNERFireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
							ON INNERFireMarshalDriver.isoClaimId = CLT00004.I_ALLCLM
					WHERE
						CLT00004.F_SIU_INVST = 'Y'
				) AS ClaimUnderSIUInvestigation
					ON CLT0001A.I_ALLCLM = ClaimUnderSIUInvestigation.I_ALLCLM
				LEFT OUTER JOIN dbo.V_ActivePolicy AS ExistingMappedPolicy WITH (NOLOCK)
					ON CLT0001A.I_ALLCLM = ExistingMappedPolicy.isoClaimId
				INNER JOIN dbo.V_ActiveLocationOfLoss AS ExistingLocationOfLoss WITH (NOLOCK)
					ON CLT0001A.I_ALLCLM = ExistingLocationOfLoss.isoClaimId
			WHERE
				/*Deprecating due to performance costs, and current profile state. RDW 20190306:
					NULLIF(LTRIM(RTRIM(CLT0001A.I_ALLCLM)),'') IS NOT NULL
					*additionally: current count for CLT00001.Date_Insert IS NULL is 0, 20190401
				*/
				ISNULL(ClaimUnderSIUInvestigation.uniqueInstanceValue,1) = 1
				AND CLT0001A.Date_Insert >= CAST(
					REPLACE(
						CAST(
							@dateFilterParam
							AS VARCHAR(10)
						),
					'-','')
					AS INT
				);

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'UpdateExistingClaim',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Claim WITH (TABLOCKX)
				SET
					/*Claim.claimId*/
					/*Claim.isoClaimId*/
					Claim.originalClaimNumber = SOURCE.originalClaimNumber,
					Claim.locationOfLossAddressId = Source.locationOfLossAddressId,
					Claim.policyId = Source.policyId,
					Claim.claimSearchSourceSystem = SOURCE.claimSearchSourceSystem,
					Claim.claimEntryMethod = SOURCE.claimEntryMethod,
					Claim.isVoidedByInsuranceCarrier = SOURCE.isVoidedByInsuranceCarrier,
					Claim.lossDescription = SOURCE.lossDescription,
					Claim.lossDescriptionExtended = SOURCE.lossDescriptionExtended,
					Claim.isClaimSearchProperty = SOURCE.isClaimSearchProperty,
					Claim.isClaimSearchAuto = SOURCE.isClaimSearchAuto,
					Claim.isClaimSearchCasualty = SOURCE.isClaimSearchCasualty,
					Claim.isClaimSearchAPD = SOURCE.isClaimSearchAPD,
					Claim.isClaimUnderSIUInvestigation = Source.isClaimUnderSIUInvestigation,
					Claim.siuCompanyName = Source.siuCompanyName,
					Claim.siuRepresentativeFullName = Source.siuRepresentativeFullName,
					Claim.siuWorkPhoneNumber = Source.siuWorkPhoneNumber,
					Claim.siuCellPhoneNumber = Source.siuCellPhoneNumber,
					Claim.dateOfLoss = SOURCE.dateOfLoss,
					Claim.insuranceCompanyReceivedDate = SOURCE.insuranceCompanyReceivedDate,
					Claim.systemDateReceived = SOURCE.systemDateReceived,
					/*Claim.isActive*/
					/*dateInserted*/
					Claim.deltaDate = @dateInserted
			FROM
				#ClaimData AS SOURCE
			WHERE
				SOURCE.existingClaimId IS NOT NULL
				AND Claim.isActive = CAST(1 AS BIT)
				AND Claim.ClaimId = SOURCE.existingClaimId
				AND 
				(
					/*Claim.claimId*/
					/*Claim.isoClaimId*/
					Claim.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(Claim.originalClaimNumber,'') <> ISNULL(SOURCE.originalClaimNumber,'')
					OR ISNULL(Claim.locationOfLossAddressId,-1) <> ISNULL(SOURCE.locationOfLossAddressId,-1)
					OR ISNULL(Claim.policyId,-1) <> ISNULL(SOURCE.policyId,-1)
					OR ISNULL(Claim.claimSearchSourceSystem,'') <> ISNULL(SOURCE.claimSearchSourceSystem,'')
					OR ISNULL(Claim.claimEntryMethod,'') <> ISNULL(SOURCE.claimEntryMethod,'')
					OR ISNULL(Claim.isVoidedByInsuranceCarrier,'') <> ISNULL(SOURCE.isVoidedByInsuranceCarrier,'')
					OR ISNULL(Claim.lossDescription,'') <> ISNULL(SOURCE.lossDescription,'')
					OR ISNULL(Claim.lossDescriptionExtended,'') <> ISNULL(SOURCE.lossDescriptionExtended,'')
					OR ISNULL(Claim.isClaimSearchProperty,0) <> ISNULL(SOURCE.isClaimSearchProperty,0)
					OR ISNULL(Claim.isClaimSearchAuto,0) <> ISNULL(SOURCE.isClaimSearchAuto,0)
					OR ISNULL(Claim.isClaimSearchCasualty,0) <> ISNULL(SOURCE.isClaimSearchCasualty,0)
					OR ISNULL(Claim.isClaimSearchAPD,0) <> ISNULL(SOURCE.isClaimSearchAPD,0)
					OR ISNULL(Claim.isClaimUnderSIUInvestigation,0) <> ISNULL(Source.isClaimUnderSIUInvestigation,0)
					OR ISNULL(Claim.siuCompanyName,'~~~') <> ISNULL(Source.siuCompanyName,'~~~')
					OR ISNULL(Claim.siuRepresentativeFullName,'~~~') <> ISNULL(Source.siuRepresentativeFullName,'~~~')
					OR ISNULL(Claim.siuWorkPhoneNumber,'0000000000') <> ISNULL(Source.siuWorkPhoneNumber,'0000000000')
					OR ISNULL(Claim.siuCellPhoneNumber,'0000000000') <> ISNULL(Source.siuCellPhoneNumber,'0000000000')
					OR ISNULL(Claim.dateOfLoss,'19000101') <> ISNULL(SOURCE.dateOfLoss,'19000101')
					OR ISNULL(Claim.insuranceCompanyReceivedDate,'19000101') <> ISNULL(SOURCE.insuranceCompanyReceivedDate,'19000101')
					OR ISNULL(Claim.systemDateReceived,'19000101') <> ISNULL(SOURCE.systemDateReceived,'19000101')
					/*Claim.isActive*/
					/*Claim.dateInserted*/
					/*Claim.deltaDate*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'InsertNewClaimData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.Claim WITH (TABLOCKX)
			(
				/*claimId,*/
				isoClaimId,
				originalClaimNumber,
				locationOfLossAddressId,
				policyId,
				claimSearchSourceSystem,
				claimEntryMethod,
				isVoidedByInsuranceCarrier,
				lossDescription,
				lossDescriptionExtended,
				isClaimSearchProperty,
				isClaimSearchAuto,
				isClaimSearchCasualty,
				isClaimSearchAPD,
				isClaimUnderSIUInvestigation,
				siuCompanyName,
				siuRepresentativeFullName,
				siuWorkPhoneNumber,
				siuCellPhoneNumber,
				dateOfLoss,
				insuranceCompanyReceivedDate,
				systemDateReceived,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				/*SOURCE.claimId,*/
				SOURCE.isoClaimId,
				SOURCE.originalClaimNumber,
				SOURCE.locationOfLossAddressId,
				SOURCE.policyId,
				SOURCE.claimSearchSourceSystem,
				SOURCE.claimEntryMethod,
				SOURCE.isVoidedByInsuranceCarrier,
				SOURCE.lossDescription,
				SOURCE.lossDescriptionExtended,
				SOURCE.isClaimSearchProperty,
				SOURCE.isClaimSearchAuto,
				SOURCE.isClaimSearchCasualty,
				SOURCE.isClaimSearchAPD,
				Source.isClaimUnderSIUInvestigation,
				Source.siuCompanyName,
				Source.siuRepresentativeFullName,
				Source.siuWorkPhoneNumber,
				Source.siuCellPhoneNumber,
				SOURCE.dateOfLoss,
				SOURCE.insuranceCompanyReceivedDate,
				SOURCE.systemDateReceived,
				CAST(1 AS BIT) isActive,
				@dateInserted dateInserted,
				@dateInserted deltaDate
			FROM
				#ClaimData AS SOURCE
			WHERE
				SOURCE.existingClaimId IS NULL;
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'DeprecateOrphanISOClaimData_claim',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.Claim WITH (TABLOCKX)
				SET
					/*Claim.claimId*/
					/*Claim.isoClaimId*/
					Claim.isActive = CAST(0 AS BIT),
					Claim.deltaDate = @dateInserted
			FROM
				#ClaimData AS SOURCE
			WHERE
				Claim.isoClaimId = SOURCE.isoClaimId
				AND Claim.isActive = CAST(1 AS BIT)
				AND Claim.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();

		/*Log Activity*/
		INSERT INTO dbo.ClaimActivityLog
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
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertFMClaim; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-08
Author: Robert David Warner
Description: Mechanism for data-refresh of the InvolvedParty Table.
				Also inserts new associations into the IPACRMap.

			Performance:
				Use of TempTable index (adds 6 seconds, but significantly reduces runtime of subsequent steps
				Use of WindowfunctionPartition for Duplicate removal (35% performance gain over Distinct).
				Explore LOCK_ESCALATION at the partition level 
					set the LOCK_ESCALATION option of the ALTER TABLE statement to AUTO.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: Some of the transforms on the phone number/area-code values in IP are probably unnecessary.
				however it's a small/non-existant improvement.
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertInvolvedParty
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.InvolvedPartyActivityLog
				WHERE
					InvolvedPartyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND InvolvedPartyActivityLog.isSuccessful = 1
					AND InvolvedPartyActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(InvolvedPartyActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.InvolvedPartyActivityLog WITH (NOLOCK)
			WHERE
				InvolvedPartyActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND InvolvedPartyActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CreateSupportDataTempTable',
				@stepStartDateTime = GETDATE();
			
			/*NOTE: potentially update this step if/when IP Claim-Role changes source*/	
			SELECT
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartyId,
				DuplicateDataSetPerformanceHackMelissaNameMap.isBusiness,
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartyRoleCode,
				DuplicateDataSetPerformanceHackMelissaNameMap.taxIdentificationNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.taxIdentificationNumberLastFour,
				DuplicateDataSetPerformanceHackMelissaNameMap.socialSecurityNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.socialSecurityNumberLastFour,
				DuplicateDataSetPerformanceHackMelissaNameMap.hICNObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseNumberObfuscated,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseNumberLast3,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseClass,
				DuplicateDataSetPerformanceHackMelissaNameMap.driversLicenseState,
				DuplicateDataSetPerformanceHackMelissaNameMap.genderCode,
				DuplicateDataSetPerformanceHackMelissaNameMap.passportID,
				DuplicateDataSetPerformanceHackMelissaNameMap.professionalMedicalLicense,
				DuplicateDataSetPerformanceHackMelissaNameMap.isUnderSiuInvestigation,
				DuplicateDataSetPerformanceHackMelissaNameMap.isLawEnforcementAction,
				DuplicateDataSetPerformanceHackMelissaNameMap.isReportedToFraudBureau,
				DuplicateDataSetPerformanceHackMelissaNameMap.isFraudReported,
				DuplicateDataSetPerformanceHackMelissaNameMap.dateOfBirth,
				DuplicateDataSetPerformanceHackMelissaNameMap.fullName,
				DuplicateDataSetPerformanceHackMelissaNameMap.firstName,
				DuplicateDataSetPerformanceHackMelissaNameMap.middleName,
				DuplicateDataSetPerformanceHackMelissaNameMap.lastName,
				DuplicateDataSetPerformanceHackMelissaNameMap.suffix,
				DuplicateDataSetPerformanceHackMelissaNameMap.businessArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.businessTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.cellArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.cellTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.faxArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.faxTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.homeArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.homeTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.pagerArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.pagerTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.otherArea,
				DuplicateDataSetPerformanceHackMelissaNameMap.otherTel,
				DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId,
				DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartySequenceId
				INTO #ScrubbedNameData
			FROM
				(
					SELECT
						ExistingInvolvedParty.involvedPartyId,
						NULLIF(CAST(LTRIM(RTRIM(CLT00004.I_ALLCLM)) AS VARCHAR(11)),'') AS isoClaimId,
						CAST(CLT00004.I_NM_ADR AS INT) AS involvedPartySequenceId,
						ROW_NUMBER() OVER(
							PARTITION BY
								CLT00004.I_ALLCLM,
								CLT00004.I_NM_ADR
							ORDER BY
								CLT00004.Date_Insert DESC
						) AS uniqueInstanceValue,
						CASE
							WHEN
								CLT00004.C_NM_TYP = 'B'
							THEN
								CAST(1 AS BIT)
							ELSE
								CAST(0 AS BIT)
						END AS isBusiness,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_ROLE)),'') AS VARCHAR(2)) AS involvedPartyRoleCode,
						CAST(NULLIF(LTRIM(RTRIM(CLT00035.N_TIN)),'') AS VARCHAR(36)) AS taxIdentificationNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0035A.TIN_4)),'') AS CHAR(4)) AS taxIdentificationNumberLastFour,
						CAST(NULLIF(LTRIM(RTRIM(CLT00007.N_SSN)),'') AS VARCHAR(36)) AS socialSecurityNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0007A.SSN_4)),'') AS VARCHAR(4)) AS socialSecurityNumberLastFour,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.T_HICN_MEDCR)),'') AS VARCHAR(36)) AS hICNObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT00008.N_DRV_LIC)),'') AS VARCHAR(52)) AS driversLicenseNumberObfuscated,
						CAST(NULLIF(LTRIM(RTRIM(CLT0008A.N_DRV_LIC)),'') AS VARCHAR(3)) AS driversLicenseNumberLast3,
						CAST(NULLIF(LTRIM(RTRIM(CLT0008A.C_DRV_LIC_CLASS)),'') AS VARCHAR(3)) AS driversLicenseClass,
						CAST(NULLIF(LTRIM(RTRIM(CLT00008.C_ST_ALPH)),'') AS VARCHAR(2)) AS driversLicenseState,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.C_GEND)),'') AS CHAR(2)) AS genderCode,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.N_PSPRT)),'') AS VARCHAR(9)) passportID,
						CAST(NULLIF(LTRIM(RTRIM(CLT00010.N_PROF_MED_LIC)),'') AS VARCHAR(20)) professionalMedicalLicense,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_SIU_INVST
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_SIU_INVST
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isUnderSiuInvestigation,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_ENF_ACTN
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_ENF_ACTN
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isLawEnforcementAction,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_BUR_RPT
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_BUR_RPT
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)
							*/
							ELSE
								CAST(0 AS BIT)
						END AS isReportedToFraudBureau,
						CASE
							WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_OCUR
									)
								) = 'Y'
							THEN
								CAST(1 AS BIT)
							/*WHEN
								LTRIM(
									RTRIM(
										CLT00004.F_FRAUD_OCUR
									)
								) = 'N'
							THEN
								CAST(0 AS BIT)
							ELSE
								CAST(NULL AS BIT)*/
							ELSE
								CAST(0 AS BIT)
						END AS isFraudReported,
						CLT00004.D_BRTH AS dateOfBirth,
						CAST(NULLIF(LTRIM(RTRIM(CLT00004.M_FUL_NM)),'') AS VARCHAR(70)) AS fullName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_FirstName1*/NULL)),'') AS VARCHAR(100)) AS firstName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_MiddleName1*/NULL)),'') AS VARCHAR(100)) AS middleName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_LastName1*/NULL)),'') AS VARCHAR(100)) AS lastName,
						CAST(NULLIF(LTRIM(RTRIM(/*CS_Lookup_Unique_Names_Melissa_Output.MD_Suffix1*/NULL)),'') AS VARCHAR(50)) suffix,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[B] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[B])-6,
								7,
								''
							),
						3) AS businessArea,
						RIGHT(CAST(PhoneNumbersPivoted.[B] AS VARCHAR(10)),7) AS businessTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[C] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[C])-6,
								7,
								''
							),
						3) AS cellArea,
						RIGHT(CAST(PhoneNumbersPivoted.[C] AS VARCHAR(10)),7) AS cellTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[F] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[F])-6,
								7,
								''
							),
						3) AS faxArea,
						RIGHT(CAST(PhoneNumbersPivoted.[F] AS VARCHAR(10)),7) AS faxTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[H] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[H])-6,
								7,
								''
							),
						3) AS homeArea,
						RIGHT(CAST(PhoneNumbersPivoted.[H] AS VARCHAR(10)),7) AS homeTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[P] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[P])-6,
								7,
								''
							),
						3) AS pagerArea,
						RIGHT(CAST(PhoneNumbersPivoted.[P] AS VARCHAR(10)),7) AS pagerTel,
						RIGHT(
							'000' 
							+ STUFF(
								CAST(PhoneNumbersPivoted.[*] AS VARCHAR(10)),
								LEN(PhoneNumbersPivoted.[*])-6,
								7,
								''
							),
						3) AS otherArea,
						RIGHT(CAST(PhoneNumbersPivoted.[*] AS VARCHAR(10)),7) AS otherTel
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = CLT00004.I_ALLCLM
						/*DEVNOTE: Deprecated; not using scrubed value at present and significant performance hit*//*
						LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
							ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
						LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Names_Melissa_Output WITH (NOLOCK)
							ON CS_Lookup_EntityIDs.SubjectKey = CS_Lookup_Unique_Names_Melissa_Output.SubjectKey*/
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00007 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00007.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00007.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0007A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0007A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0007A.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00008 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00008.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00008.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0008A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0008A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0008A.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00010 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00010.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00010.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT00035 WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT00035.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT00035.I_NM_ADR
						LEFT OUTER JOIN ClaimSearch_Prod.dbo.CLT0035A WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = CLT0035A.I_ALLCLM
								AND CLT00004.I_NM_ADR = CLT0035A.I_NM_ADR
						LEFT OUTER JOIN (
							SELECT
								CLT00009.I_ALLCLM,
								CLT00009.I_NM_ADR,
								CAST(
									CAST(
										RIGHT('00000' + CAST(CLT00009.N_AREA AS VARCHAR(3)),3)
										+ RIGHT('00000' + CAST(CLT00009.N_TEL AS VARCHAR(7)),7)
									AS CHAR(10))
								 AS BIGINT) AS phoneNumberValue,
								ISNULL(CLT00009.C_TEL_TYP, '*') AS phoneNumberType
							FROM
								dbo.FireMarshalDriver AS INNERFM_ExtractFile WITH (NOLOCK)
								INNER JOIN ClaimSearch_Prod.dbo.CLT00009 WITH (NOLOCK)
									ON INNERFM_ExtractFile.isoClaimId = CLT00009.I_ALLCLM
							) PhoneNumbers PIVOT
							(
								SUM(phoneNumberValue)
								FOR phoneNumberType IN
								(
									[B],
									[C],
									[F],
									[H],
									[P],
									[*]
								)
						) AS PhoneNumbersPivoted
							ON CLT00004.I_ALLCLM = PhoneNumbersPivoted.I_ALLCLM
								AND CLT00004.I_NM_ADR = PhoneNumbersPivoted.I_NM_ADR
						LEFT OUTER JOIN dbo.InvolvedParty AS ExistingInvolvedParty WITH (NOLOCK)
							ON CLT00004.I_ALLCLM = ExistingInvolvedParty.isoClaimId
								AND CLT00004.I_NM_ADR = ExistingInvolvedParty.involvedPartySequenceId
								AND ExistingInvolvedParty.isActive = CAST(1 AS BIT)
					WHERE
						/*Deprecating due to performance costs, and current profile state. RDW 20190306:
							NULLIF(LTRIM(RTRIM(CLT00004.I_ALLCLM)),'') IS NOT NULL
							AND CLT00004.I_NM_ADR IS NOT NULL
						*/
						CLT00004.Date_Insert >= CAST(
							REPLACE(
								CAST(
									@dateFilterParam
									AS VARCHAR(10)
								),
							'-','')
							AS INT
						)
				) AS DuplicateDataSetPerformanceHackMelissaNameMap
			WHERE
				DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;	

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'AddIndexToSupportDataTempTable',
				@stepStartDateTime = GETDATE();
			
			CREATE UNIQUE CLUSTERED INDEX PK_ScrubbedNameData_isoClaimId_IPSequenceId
				ON #ScrubbedNameData (isoClaimId, involvedPartySequenceId);
			CREATE NONCLUSTERED INDEX NIX_ScrubbedNameData_involvedPartyId
				ON #ScrubbedNameData (involvedPartyId);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;	

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'CaptureNonAliasedInvolvedPartyDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId AS existingInvolvedPartyId,
				/*NULL AS isAliasOfInvolvedPartyId,*/
				/*NULL AS isServiceProviderOfInvolvedPartyId,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId,
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel
				/*isActive,*/
				/*dateInserted,*/
				/*deltaDate*/
				INTO #FMNonAliasedInvolvedPartyData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 4,
				@stepDescription = 'UpdateFMNonAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
					InvolvedParty.deltaDate = @dateInserted
				
			FROM
				#FMNonAliasedInvolvedPartyData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NOT NULL
				AND SOURCE.isActive = CAST(1 AS BIT)
				AND InvolvedParty.involvedPartyId = SOURCE.existingInvolvedPartyId
				AND 
				(
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
					/*InvolvedParty.deltaDate*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 5,
				@stepDescription = 'InsertNewFMNonAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isoClaimId,
				involvedPartySequenceId,
				isBusiness,
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				NULL AS isAliasOfInvolvedPartyId,
				NULL AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				SOURCE.isBusiness,
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				CAST(1 AS BIT) isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#FMNonAliasedInvolvedPartyData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 6,
				@stepDescription = 'CaptureAliasedInvolvedPartyToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				#ScrubbedNameData.involvedPartyId AS existingInvolvedPartyId,
				AliasInvolvedParty.involvedPartyId AS isAliasOfInvolvedPartyId,
				/*NULL AS isServiceProviderOfInvolvedPartyId,*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId,
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel
				/*isActive*/
				/*dateInserted*/
				/*deltaDate*/
				INTO #FMAliasedInvolvedPartyData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON DuplicateDataSetPerformanceHackAliases.isoClaimId = AliasInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId = AliasInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 7,
				@stepDescription = 'UpdateFMAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = NULL,
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
					InvolvedParty.deltaDate = @dateInserted
			FROM
				#FMAliasedInvolvedPartyData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NOT NULL
				AND InvolvedParty.involvedPartyId = SOURCE.existingInvolvedPartyId
				AND
				(
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.deltaDate = @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 8,
				@stepDescription = 'InsertNewFMAliasedInvolvedPartyData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isoClaimId,
				involvedPartySequenceId,
				isBusiness,
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
				NULL AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				SOURCE.isBusiness,
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				CAST(1 AS BIT) isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#FMAliasedInvolvedPartyData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 9,
				@stepDescription = 'CaptureNonAliasedServiceProviderDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId AS existingInvolvedPartyId,
				/*NULL isAliasOfInvolvedPartyId*/
				COALESCE(ServiceProviderInvolvedParty.involvedPartyId, DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId) AS isServiceProviderOfInvolvedPartyId, /*COALESECE to protect validity of IP*/
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId,
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel
				/*isActive*/
				/*dateInserted*/
				/*deltaDate*/
				INTO #FMNonAliasedServiceProviderData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS ServiceProviderInvolvedParty WITH (NOLOCK)
					ON DuplicateDataSetPerformanceHackSP.isoClaimId = ServiceProviderInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId = ServiceProviderInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NULL /*could realy be any non-nullable column; we're looking for the absence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
			/*Performance Consideration:
				Potentially, created a Filtered Unique Index on the TempTable for
			*/
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 10,
				@stepDescription = 'UpdateFMNonAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.isAliasOfInvolvedPartyId = NULL,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
					InvolvedParty.deltaDate = @dateInserted
			FROM
				#FMNonAliasedServiceProviderData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NOT NULL
				AND InvolvedParty.involvedPartyId = SOURCE.existingInvolvedPartyId
				AND
				(
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.deltaDate = @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(NULL,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
					/*InvolvedParty.deltaDate*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 11,
				@stepDescription = 'InsertNewFMNonAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isoClaimId,
				involvedPartySequenceId,
				isBusiness,
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				NULL AS isAliasOfInvolvedPartyId,
				SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				SOURCE.isBusiness,
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				CAST(1 AS BIT) isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#FMNonAliasedServiceProviderData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 12,
				@stepDescription = 'CaptureFMAliasedServiceProviderPartyDataToImport',
				@stepStartDateTime = GETDATE();
				
			SELECT
				#ScrubbedNameData.involvedPartyId AS existingInvolvedPartyId,
				AliasInvolvedParty.involvedPartyId AS isAliasOfInvolvedPartyId,
				ServiceProviderInvolvedParty.involvedPartyId AS isServiceProviderOfInvolvedPartyId,
				#ScrubbedNameData.isoClaimId,
				#ScrubbedNameData.involvedPartySequenceId,
				#ScrubbedNameData.isBusiness,
				#ScrubbedNameData.involvedPartyRoleCode,
				#ScrubbedNameData.taxIdentificationNumberObfuscated,
				#ScrubbedNameData.taxIdentificationNumberLastFour,
				#ScrubbedNameData.socialSecurityNumberObfuscated,
				#ScrubbedNameData.socialSecurityNumberLastFour,
				#ScrubbedNameData.hICNObfuscated,
				#ScrubbedNameData.driversLicenseNumberObfuscated,
				#ScrubbedNameData.driversLicenseNumberLast3,
				#ScrubbedNameData.driversLicenseClass,
				#ScrubbedNameData.driversLicenseState,
				#ScrubbedNameData.genderCode,
				#ScrubbedNameData.passportID,
				#ScrubbedNameData.professionalMedicalLicense,
				#ScrubbedNameData.isUnderSiuInvestigation,
				#ScrubbedNameData.isLawEnforcementAction,
				#ScrubbedNameData.isReportedToFraudBureau,
				#ScrubbedNameData.isFraudReported,
				#ScrubbedNameData.dateOfBirth,
				#ScrubbedNameData.fullName,
				#ScrubbedNameData.firstName,
				#ScrubbedNameData.middleName,
				#ScrubbedNameData.lastName,
				#ScrubbedNameData.suffix,
				#ScrubbedNameData.businessArea,
				#ScrubbedNameData.businessTel,
				#ScrubbedNameData.cellArea,
				#ScrubbedNameData.cellTel,
				#ScrubbedNameData.faxArea,
				#ScrubbedNameData.faxTel,
				#ScrubbedNameData.homeArea,
				#ScrubbedNameData.homeTel,
				#ScrubbedNameData.pagerArea,
				#ScrubbedNameData.pagerTel,
				#ScrubbedNameData.otherArea,
				#ScrubbedNameData.otherTel
				/*isActive,*/
				/*dateInserted,*/
				/*deltaDate,*/
				INTO #FMAliasedServiceProviderData
			FROM
				#ScrubbedNameData
				LEFT OUTER JOIN (
					SELECT
						Aliases.I_ALLCLM AS isoClaimId,
						Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
						Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								Aliases.I_ALLCLM,
								Aliases.I_NM_ADR_AKA
							ORDER BY
								Aliases.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = Aliases.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackAliases
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackAliases.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackAliases.aliasInvolvedPartySequenceId
				LEFT OUTER JOIN (
					SELECT
						ServicesProviders.I_ALLCLM AS isoClaimId,
						ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
						ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
						ROW_NUMBER() OVER (
							PARTITION BY
								ServicesProviders.I_ALLCLM,
								ServicesProviders.I_NM_ADR_SVC_PRVD
							ORDER BY
								ServicesProviders.Date_Insert
						) AS uniqueInstanceValue
					FROM
						dbo.FireMarshalDriver WITH (NOLOCK)
						INNER JOIN ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = ServicesProviders.I_ALLCLM
				) AS DuplicateDataSetPerformanceHackSP
					ON #ScrubbedNameData.isoClaimId = DuplicateDataSetPerformanceHackSP.isoClaimId
						AND #ScrubbedNameData.involvedPartySequenceId = DuplicateDataSetPerformanceHackSP.sPInvolvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS AliasInvolvedParty
					ON DuplicateDataSetPerformanceHackAliases.isoClaimId = AliasInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId = AliasInvolvedParty.involvedPartySequenceId
				LEFT OUTER JOIN dbo.InvolvedParty AS ServiceProviderInvolvedParty
					ON DuplicateDataSetPerformanceHackSP.isoClaimId = ServiceProviderInvolvedParty.isoClaimId
						AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId = ServiceProviderInvolvedParty.involvedPartySequenceId
			WHERE
				DuplicateDataSetPerformanceHackAliases.nonAliasInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackAliases.uniqueInstanceValue,1) = 1
				AND DuplicateDataSetPerformanceHackSP.nonSPInvolvedPartySequenceId IS NOT NULL /*could realy be any non-nullable column; we're looking for the presence of any row altogether*/
				AND ISNULL(DuplicateDataSetPerformanceHackSP.uniqueInstanceValue,1) = 1;
				
			/*Performance Consideration:
				Potentially, created a Filtered Unique Index on the TempTable for
			*/
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 13,
				@stepDescription = 'UpdateFMAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.isAliasOfInvolvedPartyId = SOURCE.isAliasOfInvolvedPartyId,
					InvolvedParty.isServiceProviderOfInvolvedPartyId = SOURCE.isServiceProviderOfInvolvedPartyId,
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					InvolvedParty.isBusiness = SOURCE.isBusiness,
					InvolvedParty.taxIdentificationNumberObfuscated = SOURCE.taxIdentificationNumberObfuscated,
					InvolvedParty.taxIdentificationNumberLastFour = SOURCE.taxIdentificationNumberLastFour,
					InvolvedParty.socialSecurityNumberObfuscated = SOURCE.socialSecurityNumberObfuscated,
					InvolvedParty.socialSecurityNumberLastFour = SOURCE.socialSecurityNumberLastFour,
					InvolvedParty.hICNObfuscated = SOURCE.hICNObfuscated,
					InvolvedParty.driversLicenseNumberObfuscated = SOURCE.driversLicenseNumberObfuscated,
					InvolvedParty.driversLicenseNumberLast3 = SOURCE.driversLicenseNumberLast3,
					InvolvedParty.driversLicenseClass = SOURCE.driversLicenseClass,
					InvolvedParty.driversLicenseState = SOURCE.driversLicenseState,
					InvolvedParty.genderCode = SOURCE.genderCode,
					InvolvedParty.passportID = SOURCE.passportID,
					InvolvedParty.professionalMedicalLicense = SOURCE.professionalMedicalLicense,
					InvolvedParty.isUnderSiuInvestigation = SOURCE.isUnderSiuInvestigation,
					InvolvedParty.isLawEnforcementAction = SOURCE.isLawEnforcementAction,
					InvolvedParty.isReportedToFraudBureau = SOURCE.isReportedToFraudBureau,
					InvolvedParty.isFraudReported = SOURCE.isFraudReported,
					InvolvedParty.dateOfBirth = SOURCE.dateOfBirth,
					InvolvedParty.fullName = SOURCE.fullName,
					InvolvedParty.firstName = SOURCE.firstName,
					InvolvedParty.middleName = SOURCE.middleName,
					InvolvedParty.lastName = SOURCE.lastName,
					InvolvedParty.suffix = SOURCE.suffix,
					InvolvedParty.businessArea = SOURCE.businessArea,
					InvolvedParty.businessTel = SOURCE.businessTel,
					InvolvedParty.cellArea = SOURCE.cellArea,
					InvolvedParty.cellTel = SOURCE.cellTel,
					InvolvedParty.faxArea = SOURCE.faxArea,
					InvolvedParty.faxTel = SOURCE.faxTel,
					InvolvedParty.homeArea = SOURCE.homeArea,
					InvolvedParty.homeTel = SOURCE.homeTel,
					InvolvedParty.pagerArea = SOURCE.pagerArea,
					InvolvedParty.pagerTel = SOURCE.pagerTel,
					InvolvedParty.otherArea = SOURCE.otherArea,
					InvolvedParty.otherTel = SOURCE.otherTel,
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
					InvolvedParty.deltaDate = @dateInserted
			FROM
				#FMAliasedServiceProviderData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NOT NULL
				AND InvolvedParty.involvedPartyId = SOURCE.existingInvolvedPartyId
				AND
				(
					/*InvolvedParty.involvedPartyId*/
					InvolvedParty.deltaDate = @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					OR ISNULL(InvolvedParty.isAliasOfInvolvedPartyId,'') <> ISNULL(SOURCE.isAliasOfInvolvedPartyId,'')
					OR ISNULL(InvolvedParty.isServiceProviderOfInvolvedPartyId,'') <> ISNULL(SOURCE.isServiceProviderOfInvolvedPartyId,'')
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					OR ISNULL(InvolvedParty.isBusiness,'') <> ISNULL(SOURCE.isBusiness,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberObfuscated,'') <> ISNULL(SOURCE.taxIdentificationNumberObfuscated,'')
					OR ISNULL(InvolvedParty.taxIdentificationNumberLastFour,'') <> ISNULL(SOURCE.taxIdentificationNumberLastFour,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberObfuscated,'') <> ISNULL(SOURCE.socialSecurityNumberObfuscated,'')
					OR ISNULL(InvolvedParty.socialSecurityNumberLastFour,'') <> ISNULL(SOURCE.socialSecurityNumberLastFour,'')
					OR ISNULL(InvolvedParty.hICNObfuscated,'') <> ISNULL(SOURCE.hICNObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberObfuscated,'') <> ISNULL(SOURCE.driversLicenseNumberObfuscated,'')
					OR ISNULL(InvolvedParty.driversLicenseNumberLast3,'') <> ISNULL(SOURCE.driversLicenseNumberLast3,'')
					OR ISNULL(InvolvedParty.driversLicenseClass,'') <> ISNULL(SOURCE.driversLicenseClass,'')
					OR ISNULL(InvolvedParty.driversLicenseState,'') <> ISNULL(SOURCE.driversLicenseState,'')
					OR ISNULL(InvolvedParty.genderCode,'') <> ISNULL(SOURCE.genderCode,'')
					OR ISNULL(InvolvedParty.passportID,'') <> ISNULL(SOURCE.passportID,'')
					OR ISNULL(InvolvedParty.professionalMedicalLicense,'') <> ISNULL(SOURCE.professionalMedicalLicense,'')
					OR ISNULL(InvolvedParty.isUnderSiuInvestigation,'') <> ISNULL(SOURCE.isUnderSiuInvestigation,'')
					OR ISNULL(InvolvedParty.isLawEnforcementAction,'') <> ISNULL(SOURCE.isLawEnforcementAction,'')
					OR ISNULL(InvolvedParty.isReportedToFraudBureau,'') <> ISNULL(SOURCE.isReportedToFraudBureau,'')
					OR ISNULL(InvolvedParty.isFraudReported,'') <> ISNULL(SOURCE.isFraudReported,'')
					OR ISNULL(InvolvedParty.dateOfBirth,'') <> ISNULL(SOURCE.dateOfBirth,'')
					OR ISNULL(InvolvedParty.fullName,'') <> ISNULL(SOURCE.fullName,'')
					OR ISNULL(InvolvedParty.firstName,'') <> ISNULL(SOURCE.firstName,'')
					OR ISNULL(InvolvedParty.middleName,'') <> ISNULL(SOURCE.middleName,'')
					OR ISNULL(InvolvedParty.lastName,'') <> ISNULL(SOURCE.lastName,'')
					OR ISNULL(InvolvedParty.suffix,'') <> ISNULL(SOURCE.suffix,'')
					OR ISNULL(InvolvedParty.businessArea,'') <> ISNULL(SOURCE.businessArea,'')
					OR ISNULL(InvolvedParty.businessTel,'') <> ISNULL(SOURCE.businessTel,'')
					OR ISNULL(InvolvedParty.cellArea,'') <> ISNULL(SOURCE.cellArea,'')
					OR ISNULL(InvolvedParty.cellTel,'') <> ISNULL(SOURCE.cellTel,'')
					OR ISNULL(InvolvedParty.faxArea,'') <> ISNULL(SOURCE.faxArea,'')
					OR ISNULL(InvolvedParty.faxTel,'') <> ISNULL(SOURCE.faxTel,'')
					OR ISNULL(InvolvedParty.homeArea,'') <> ISNULL(SOURCE.homeArea,'')
					OR ISNULL(InvolvedParty.homeTel,'') <> ISNULL(SOURCE.homeTel,'')
					OR ISNULL(InvolvedParty.pagerArea,'') <> ISNULL(SOURCE.pagerArea,'')
					OR ISNULL(InvolvedParty.pagerTel,'') <> ISNULL(SOURCE.pagerTel,'')
					OR ISNULL(InvolvedParty.otherArea,'') <> ISNULL(SOURCE.otherArea,'')
					OR ISNULL(InvolvedParty.otherTel,'') <> ISNULL(SOURCE.otherTel,'')
					/*InvolvedParty.isActive*/
					/*InvolvedParty.dateInserted*/
					/*InvolvedParty.deltaDate*/
				);
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 14,
				@stepDescription = 'InsertNewFMAliasedServiceProviderData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedParty WITH (TABLOCKX)
			(
				/*involvedPartyId,*/
				isAliasOfInvolvedPartyId,
				isServiceProviderOfInvolvedPartyId,
				isoClaimId,
				involvedPartySequenceId,
				isBusiness,
				taxIdentificationNumberObfuscated,
				taxIdentificationNumberLastFour,
				socialSecurityNumberObfuscated,
				socialSecurityNumberLastFour,
				hICNObfuscated,
				driversLicenseNumberObfuscated,
				driversLicenseNumberLast3,
				driversLicenseClass,
				driversLicenseState,
				genderCode,
				passportID,
				professionalMedicalLicense,
				isUnderSiuInvestigation,
				isLawEnforcementAction,
				isReportedToFraudBureau,
				isFraudReported,
				dateOfBirth,
				fullName,
				firstName,
				middleName,
				lastName,
				suffix,
				businessArea,
				businessTel,
				cellArea,
				cellTel,
				faxArea,
				faxTel,
				homeArea,
				homeTel,
				pagerArea,
				pagerTel,
				otherArea,
				otherTel,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				SOURCE.isAliasOfInvolvedPartyId AS isAliasOfInvolvedPartyId,
				SOURCE.isServiceProviderOfInvolvedPartyId AS isServiceProviderOfInvolvedPartyId,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				SOURCE.isBusiness,
				SOURCE.taxIdentificationNumberObfuscated,
				SOURCE.taxIdentificationNumberLastFour,
				SOURCE.socialSecurityNumberObfuscated,
				SOURCE.socialSecurityNumberLastFour,
				SOURCE.hICNObfuscated,
				SOURCE.driversLicenseNumberObfuscated,
				SOURCE.driversLicenseNumberLast3,
				SOURCE.driversLicenseClass,
				SOURCE.driversLicenseState,
				SOURCE.genderCode,
				SOURCE.passportID,
				SOURCE.professionalMedicalLicense,
				SOURCE.isUnderSiuInvestigation,
				SOURCE.isLawEnforcementAction,
				SOURCE.isReportedToFraudBureau,
				SOURCE.isFraudReported,
				SOURCE.dateOfBirth,
				SOURCE.fullName,
				SOURCE.firstName,
				SOURCE.middleName,
				SOURCE.lastName,
				SOURCE.suffix,
				SOURCE.businessArea,
				SOURCE.businessTel,
				SOURCE.cellArea,
				SOURCE.cellTel,
				SOURCE.faxArea,
				SOURCE.faxTel,
				SOURCE.homeArea,
				SOURCE.homeTel,
				SOURCE.pagerArea,
				SOURCE.pagerTel,
				SOURCE.otherArea,
				SOURCE.otherTel,
				CAST(1 AS BIT) isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#FMAliasedServiceProviderData AS SOURCE
			WHERE
				SOURCE.existingInvolvedPartyId IS NULL;
			--OPTION (RECOMPILE);

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 15,
				@stepDescription = 'DeprecateOrphanISOClaimData_IP',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.InvolvedParty WITH (TABLOCKX)
				SET
					/*InvolvedParty.isoClaimId*/
					/*InvolvedParty.involvedPartySequenceId*/
					InvolvedParty.isActive = CAST(0 AS BIT),
					InvolvedParty.deltaDate = @dateInserted
			FROM
				#ScrubbedNameData AS SOURCE
			WHERE
				InvolvedParty.isoClaimId = SOURCE.isoClaimId
				AND InvolvedParty.isActive = CAST(1 AS BIT)
				AND InvolvedParty.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;


			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 16,
				@stepDescription = 'CreateIPAMDataTempTable',
				@stepStartDateTime = GETDATE();

			SELECT
				ExistingInvolvedPartyAddressMap.involvedPartyAddressRollMapId AS existingIPARMapId,
				PotentialDuplicateReferencePerformanceHack.claimId,
				PotentialDuplicateReferencePerformanceHack.involvedPartyId,
				PotentialDuplicateReferencePerformanceHack.isoClaimId,
				PotentialDuplicateReferencePerformanceHack.involvedPartySequenceId,
				PotentialDuplicateReferencePerformanceHack.nonLocationOfLossAddressId
				/*CAST(1 AS BIT) AS isActive,*/
				/*@dateInserted AS dateInserted,*/
				/*@dateInserted AS deltaDate,*/
				INTO #IPAMData
			FROM
				(
					SELECT
						ExistingActiveClaim.claimId,
						ExistingActiveInvolvedParty.involvedPartyId,
						#ScrubbedNameData.isoClaimId,
						#ScrubbedNameData.involvedPartySequenceId,
						ExistingActiveNonLOL.addressId AS nonLocationOfLossAddressId,
						ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK') AS claimRoleCode,
						ROW_NUMBER() OVER(
							PARTITION BY
								#ScrubbedNameData.isoClaimId,
								#ScrubbedNameData.involvedPartySequenceId,
								ExistingActiveNonLOL.addressId,
								ISNULL(#ScrubbedNameData.involvedPartyRoleCode,'UK')
							ORDER BY
								ExistingActiveInvolvedParty.deltaDate
						) AS uniqueInstanceValue
						
					FROM
						#ScrubbedNameData
						INNER JOIN dbo.V_ActiveClaim AS ExistingActiveClaim
							ON #ScrubbedNameData.isoClaimId = ExistingActiveClaim.isoClaimId
						INNER JOIN dbo.InvolvedParty AS ExistingActiveInvolvedParty
							ON #ScrubbedNameData.isoClaimId = ExistingInvolvedParty.isoClaimId
								AND #ScrubbedNameData.involvedPartySequenceId = ExistingInvolvedParty.involvedPartySequenceId
						INNER JOIN dbo.V_ActiveNonLocationOfLoss AS ExistingActiveNonLOL
							ON #ScrubbedNameData.isoClaimId = ExistingActiveNonLOL.isoClaimId
								AND #ScrubbedNameData.involvedPartySequenceId = ExistingActiveNonLOL.involvedPartySequenceId
						WHERE
							InvolvedParty.isActive = CAST(1 AS BIT)
				) AS PotentialDuplicateReferencePerformanceHack
				LEFT OUTER JOIN dbo.V_ActiveIPAddressMap AS ExistingInvolvedPartyAddressMap
					ON PotentialDuplicateReferencePerformanceHack.isoClaimId = ExistingInvolvedPartyAddressMap.isoClaimId
						AND PotentialDuplicateReferencePerformanceHack.involvedPartySequenceId = ExistingInvolvedPartyAddressMap.involvedPartySequenceId
						AND PotentialDuplicateReferencePerformanceHack.nonLocationOfLossAddressId = ExistingInvolvedPartyAddressMap.nonLocationOfLossAddressId
						AND PotentialDuplicateReferencePerformanceHack.claimRoleCode = ExistingInvolvedPartyAddressMap.claimRoleCode
			WHERE
				PotentialDuplicateReferencePerformanceHack.uniqueInstanceValue = 1;
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 17,
				@stepDescription = 'UpdateIPAMData',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.InvolvedPartyAddressMap
				SET
					/*InvolvedPartyAddressMap.involvedPartyAddressRollMapId*/
					/*InvolvedPartyAddressMap.claimId*/
					/*InvolvedPartyAddressMap.involvedPartyId*/
					/*InvolvedPartyAddressMap.isoClaimId*/
					/*InvolvedPartyAddressMap.involvedPartySequenceId*/
					/*InvolvedPartyAddressMap.nonLocationOfLossAddressId*/
					/*InvolvedPartyAddressMap.claimRoleCode*/
					/*InvolvedPartyAddressMap.isActive*/
					InvolvedPartyAddressMap.dateInserted = @dateInserted
			FROM
				#IPAMData AS SOURCE
			WHERE
				SOURCE.existingIPAMClaimId IS NOT NULL
				AND InvolvedPartyAddressMap.involvedPartyAddressRollMapId = SOURCE.existingIPARMapId
				AND InvolvedPartyAddressMap.isActive = CAST(1 AS BIT);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
				
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 18,
				@stepDescription = 'InsertNewIPAddressMapping(s)',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.InvolvedPartyAddressMap
			(
				/*involvedPartyAddressRollMapId,*/
				claimId,
				involvedPartyId,
				isoClaimId,
				involvedPartySequenceId,
				nonLocationOfLossAddressId,
				claimRoleCode,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				/*involvedPartyAddressRollMapId*/
				SOURCE.claimId,
				SOURCE.involvedPartyId,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				SOURCE.nonLocationOfLossAddressId,
				SOURCE.claimRoleCode,
				CAST(1 AS BIT) AS isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
			FROM
				#IPAMData AS SOURCE
			WHERE
				SOURCE.existingIPAMClaimId IS NULL;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'DeprecateOrphanISOClaimData_IARMap',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.InvolvedPartyAddressMap WITH (TABLOCKX)
				SET
					InvolvedPartyAddressMap.isActive = CAST(0 AS BIT),
					InvolvedPartyAddressMap.deltaDate = @dateInserted
			FROM
				#IPAMData AS SOURCE
			WHERE
				InvolvedPartyAddressMap.isoClaimId = SOURCE.isoClaimId
				AND InvolvedPartyAddressMap.isActive = CAST(1 AS BIT)
				AND InvolvedPartyAddressMap.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.InvolvedPartyActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Log Activity*/
			INSERT INTO dbo.IPAddressMapActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();

		/*Log Activity*/
		INSERT INTO dbo.InvolvedPartyActivityLog
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
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertInvolvedParty; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-06-06
Author: Robert David Warner and Julia Lawrence
Description: Mechanism for data-refresh of the ElementalClaim Table.

***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-20
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.

			Performance: No current notes.
************************************************/
ALTER PROCEDURE dbo.hsp_UpdateInsertElementalClaim
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		DECLARE @internalTransactionCount TINYINT = 0;
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.ElementalClaimActivityLog
				WHERE
					ElementalClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND ElementalClaimActivityLog.isSuccessful = 1
					AND ElementalClaimActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			/*Set Logging Variables for execution*/
			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(ElementalClaimActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.ElementalClaimActivityLog
			WHERE
				ElementalClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND ElementalClaimActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'Capture Elemental Claim DataToImport',
				@stepStartDateTime = GETDATE();

			SELECT
				MAX(ExistingElementalClaimRecord.elementalClaimId) AS existingElementalClaimId,
				V_ActiveIPAddressMap.claimId,
				V_ActiveIPAddressMap.involvedPartyId,
				MAX(V_ActiveAdjuster.adjusterId) AS adjusterId,
				CAST(CLT00014.C_LOSS_TYP AS CHAR(4)) AS lossTypeCode,
				CAST(Dim_Coverage_Type.T_CVG_TYP AS VARCHAR(42)) AS coverageTypeDescription,
				CAST(CLT00014.C_CVG_TYP AS CHAR(4)) AS coverageTypeCode,
				CAST(Dim_Loss_Type.T_LOSS_TYP AS VARCHAR(42)) AS lossTypeDescription,
				MAX(CAST(CLT00014.D_CLM_CLOSE AS DATE)) AS dateClaimClosed, /*D_CLM_CLOSE*/
				CAST(CLT00014.C_CLM_STUS AS VARCHAR(3)) AS coverageStatus, /*C_CLM_STUS*/
				CLT00014.C_CVG_TYP AS cLT14CoverageType,
				CAST(
					NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
						SUM(
							CLT00017.A_BLDG_PD
							+ CLT00017.A_CNNT_PD
							+ CLT00017.A_STK_PD
							+ CLT00017.A_USE_PD
							+ CLT00017.A_OTH_PD
						),
						0
					)
					AS MONEY
				) AS cLT17SettlementAmount,
				CAST(
					NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
						SUM(
							CLT00014.A_STTLMT
						),
						0
					)
					AS MONEY
				) AS cLT14SettlementAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00017.A_BLDG_EST_LOSS + CLT00017.A_CNTT_EST_LOSS + CLT00017.A_STK_EST_LOSS + CLT00017.A_USE_EST_LOSS + CLT00017.A_OTH_EST_LOSS
							),
							0
						)
						AS MONEY
					) AS cLT17estimatedLossAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00014.A_EST_LOSS
							),
							0
						)
						AS MONEY
					) AS cLT14estimatedLossAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00017.A_BLDG_RSRV + CLT00017.A_CNNT_RSRV + CLT00017.A_STK_RSRV + CLT00017.A_USE_RSRV + CLT00017.A_OTH_RSRV
							),
							0
						)
						AS MONEY
					) AS cLT17reserveAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00014.A_RSRV
							),
							0
						)
						AS MONEY
					) AS cLT14reserveAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00017.A_BLDG_TL_INS + CLT00017.A_CNTT_TL_INS + CLT00017.A_STK_TL_INS + CLT00017.A_USE_TL_INS + CLT00017.A_OTH_TL_INS
							),
							0
						)
						AS MONEY
					) AS cLT17totalInsuredAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00014.A_INDM
							),
							0
						)
						AS MONEY
					) AS cLT14totalInsuredAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00017.A_BLDG_POL + CLT00017.A_CNTT_POL + CLT00017.A_STK_POL + CLT00017.A_USE_POL + CLT00017.A_OTH_POL
							),
							0
						)
						AS MONEY
					) AS cLT17policyAmount,
					CAST(
						NULLIF(/*DO NOT REMOVE NULLIF. Breaks COALESCE further down!*/
							SUM(
								CLT00014.A_CVG_TL
							),
							0
						)
						AS MONEY
					) AS cLT14policyAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_RPLCMT_VAL + CLT00017.A_CNTT_RPLCMT_VAL + CLT00017.A_STK_RPLCMT_VAL + CLT00017.A_USE_RPLCMT_VAL+ CLT00017.A_OTH_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)	
				) AS replacementAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_ACTL_VAL + CLT00017.A_CNTT_ACTL_VAL + CLT00017.A_STK_ACTL_VAL + CLT00017.A_USE_ACTL_VAL + CLT00017.A_OTH_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS actualCashAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingPolicyAmount, /*A_BLDG_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingTotalInsuredAmount, /*A_BLDG_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingReplacementAmount, /*A_BLDG_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingActualCashAmount, /*A_BLDG_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingEstimatedLossAmount, /*A_BLDG_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentPolicyAmount, /*A_CNTT_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentTotalInsuredAmount, /*A_CNTT_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentReplacementAmount, /*A_CNTT_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentActualCashAmount, /*A_CNTT_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNTT_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentEstimatedLossAmount, /*A_CNTT_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockPolicyAmount, /*A_STK_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockTotalInsuredAmount, /*A_STK_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockReplacementAmount, /*A_STK_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockActualCashAmount, /*A_STK_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockEstimatedLossAmount, /*A_STK_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUsePolicyAmount, /*A_USE_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseTotalInsuredAmount, /*A_USE_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseReplacementAmount, /*A_USE_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseActualCashAmount, /*A_USE_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseEstimatedLossAmount,
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_POL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherPolicyAmount, /*A_OTH_POL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_TL_INS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherTotalInsuredAmount, /*A_OTH_TL_INS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_RPLCMT_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherReplacementAmount, /*A_OTH_RPLCMT_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_ACTL_VAL AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherActualCashAmount, /*A_OTH_ACTL_VAL*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_EST_LOSS AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherEstimatedLossAmount, /*A_OTH_EST_LOSS*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingReserveAmount, /*A_BLDG_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_BLDG_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS buildingPaidAmount, /*A_BLDG_PD*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNNT_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentReserveAmount, /*A_CNNT_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_CNNT_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS contentPaidAmount, /*A_CNNT_PD*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockReserveAmount, /*A_STK_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_STK_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS stockPaidAmount, /*A_STK_PD */
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUseReserve, /*A_USE_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_USE_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS lossOfUsePaid, /*A_USE_PD*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_RSRV AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherReserveAmount, /*A_OTH_RSRV*/
				SUM(
					COALESCE(
						CAST(CLT00017.A_OTH_PD AS MONEY),
						CAST(0 AS MONEY)
					)
				) AS otherPaidAmount, /*A_OTH_PD*/
				MAX(V_ActiveIPAddressMap.isoClaimId) AS isoClaimId,
				MAX(V_ActiveIPAddressMap.involvedPartySequenceId) AS involvedPartySequenceId
				INTO #ElementalClaimData
			FROM
				dbo.FireMarshalDriver
				INNER JOIN dbo.V_ActiveIPAddressMap
					ON FireMarshalDriver.isoClaimId = V_ActiveIPAddressMap.isoClaimId
				INNER JOIN ClaimSearch_Prod.dbo.CLT00014
					ON V_ActiveIPAddressMap.isoClaimId = CLT00014.I_ALLCLM
						AND V_ActiveIPAddressMap.involvedPartySequenceId = CLT00014.I_NM_ADR
				LEFT OUTER JOIN dbo.V_ActiveAdjuster
					ON V_ActiveIPAddressMap.isoClaimId = V_ActiveAdjuster.isoClaimId
						AND V_ActiveIPAddressMap.involvedPartySequenceId = V_ActiveAdjuster.involvedPartySequenceId
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.Dim_Coverage_Type
					ON CLT00014.C_CVG_TYP = Dim_Coverage_Type.C_CVG_TYP
				LEFT OUTER JOIN ClaimSearch_Prod.dbo.Dim_Loss_Type
					ON CLT00014.C_LOSS_TYP = Dim_Loss_Type.C_LOSS_TYP
				LEFT OUTER JOIN (
					/*Notes on DuplicateDataSetPerformanceHack:
						dbo.CLT00017 contains duplicate records (DanR. verified with business that it
						is caused by an error somewhere in source or currentStateProcess 20190409.
							Performance of rowNumber/partition solution is noticeably better than using DISTINCT
					*/
					SELECT
						InnerCLT00017.I_ALLCLM,
						SUM(InnerCLT00017.A_BLDG_POL) AS A_BLDG_POL,
						SUM(InnerCLT00017.A_BLDG_TL_INS) AS A_BLDG_TL_INS,
						SUM(InnerCLT00017.A_BLDG_RPLCMT_VAL) AS A_BLDG_RPLCMT_VAL,
						SUM(InnerCLT00017.A_BLDG_ACTL_VAL) AS A_BLDG_ACTL_VAL,
						SUM(InnerCLT00017.A_BLDG_EST_LOSS) AS A_BLDG_EST_LOSS,
						SUM(InnerCLT00017.A_CNTT_POL) AS A_CNTT_POL,
						SUM(InnerCLT00017.A_CNTT_TL_INS) AS A_CNTT_TL_INS,
						SUM(InnerCLT00017.A_CNTT_RPLCMT_VAL) AS A_CNTT_RPLCMT_VAL,
						SUM(InnerCLT00017.A_CNTT_ACTL_VAL) AS A_CNTT_ACTL_VAL,
						SUM(InnerCLT00017.A_CNTT_EST_LOSS) AS A_CNTT_EST_LOSS,
						SUM(InnerCLT00017.A_STK_POL) AS A_STK_POL,
						SUM(InnerCLT00017.A_STK_TL_INS) AS A_STK_TL_INS,
						SUM(InnerCLT00017.A_STK_RPLCMT_VAL) AS A_STK_RPLCMT_VAL,
						SUM(InnerCLT00017.A_STK_ACTL_VAL) AS A_STK_ACTL_VAL,
						SUM(InnerCLT00017.A_STK_EST_LOSS) AS A_STK_EST_LOSS,
						SUM(InnerCLT00017.A_USE_POL) AS A_USE_POL,
						SUM(InnerCLT00017.A_USE_TL_INS) AS A_USE_TL_INS,
						SUM(InnerCLT00017.A_USE_RPLCMT_VAL) AS A_USE_RPLCMT_VAL,
						SUM(InnerCLT00017.A_USE_ACTL_VAL) AS A_USE_ACTL_VAL,
						SUM(InnerCLT00017.A_USE_EST_LOSS) AS A_USE_EST_LOSS,
						SUM(InnerCLT00017.A_OTH_POL) AS A_OTH_POL,
						SUM(InnerCLT00017.A_OTH_TL_INS) AS A_OTH_TL_INS,
						SUM(InnerCLT00017.A_OTH_RPLCMT_VAL) AS A_OTH_RPLCMT_VAL,
						SUM(InnerCLT00017.A_OTH_ACTL_VAL) AS A_OTH_ACTL_VAL,
						SUM(InnerCLT00017.A_OTH_EST_LOSS) AS A_OTH_EST_LOSS,
						SUM(InnerCLT00017.A_BLDG_RSRV) AS A_BLDG_RSRV,
						SUM(InnerCLT00017.A_BLDG_PD) AS A_BLDG_PD,
						SUM(InnerCLT00017.A_CNNT_RSRV) AS A_CNNT_RSRV,
						SUM(InnerCLT00017.A_CNNT_PD) AS A_CNNT_PD,
						SUM(InnerCLT00017.A_STK_RSRV) AS A_STK_RSRV,
						SUM(InnerCLT00017.A_STK_PD) AS A_STK_PD,
						SUM(InnerCLT00017.A_USE_RSRV) AS A_USE_RSRV,
						SUM(InnerCLT00017.A_USE_PD) AS A_USE_PD,
						SUM(InnerCLT00017.A_OTH_RSRV) AS A_OTH_RSRV,
						SUM(InnerCLT00017.A_OTH_PD) AS A_OTH_PD,
						MAX(InnerCLT00017.Date_Insert) AS Date_Insert
					FROM
						dbo.FireMarshalDriver AS InnerFireMarshalDriver
						INNER JOIN ClaimSearch_Prod.dbo.CLT00017 AS InnerCLT00017
							ON InnerFireMarshalDriver.isoClaimId = InnerCLT00017.I_ALLCLM
					GROUP BY
						InnerCLT00017.I_ALLCLM
				) AS CLT00017
					ON V_ActiveIPAddressMap.isoClaimId = CLT00017.I_ALLCLM
				LEFT OUTER JOIN dbo.V_ActiveElementalClaim AS ExistingElementalClaimRecord
					ON CLT00014.I_ALLCLM = ExistingElementalClaimRecord.isoClaimId
						AND CLT00014.I_NM_ADR = ExistingElementalClaimRecord.involvedPartySequenceId
						AND CLT00014.C_LOSS_TYP = ExistingElementalClaimRecord.lossTypeCode
						AND CLT00014.C_CVG_TYP = ExistingElementalClaimRecord.coverageTypeCode
			WHERE
				CLT00014.C_LOSS_TYP IS NOT NULL
				AND CLT00014.C_CVG_TYP IS NOT NULL
			--	/*
			--	AND (CLT00014.Date_Insert >= CAST(
			--		REPLACE(
			--			CAST(
			--				@dateFilterParam
			--				AS VARCHAR(10)
			--			),
			--		'-','')
			--		AS INT
			--	)
			--	OR CLT00017.Date_Insert >= CAST(
			--		REPLACE(
			--			CAST(
			--				@dateFilterParam
			--				AS VARCHAR(10)
			--			),
			--		'-','')
			--		AS INT
			--	)
			--)
			--*/
			GROUP BY
				V_ActiveIPAddressMap.claimId,
				V_ActiveIPAddressMap.involvedPartyId,
				CLT00014.C_CVG_TYP,
				CAST(CLT00014.C_LOSS_TYP AS CHAR(4)),
				CAST(Dim_Loss_Type.T_LOSS_TYP AS VARCHAR(42)),
				CAST(CLT00014.C_CVG_TYP AS CHAR(4)),
				CAST(Dim_Coverage_Type.T_CVG_TYP AS VARCHAR(42)),
				CAST(CLT00014.C_CLM_STUS AS VARCHAR(3));
				
			/*Performance Consideration:
				Potentially, created a Filtered Unique Index on the TempTable for
			*/

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'UpdatePropElementalClaimData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.ElementalClaim WITH (TABLOCKX)
				SET
					/*ElementalClaim.elementalClaimId = SOURCE.elementalClaimId,*/
					ElementalClaim.claimId = SOURCE.claimId,
					ElementalClaim.involvedPartyId = SOURCE.involvedPartyId,
					/*ElementalClaim.isoClaimId = SOURCE.isoClaimId*/
					/*ElementalClaim.involvedPartySequenceId = SOURCE.involvedPartySequenceId,*/
					ElementalClaim.adjusterId = SOURCE.adjusterId,
					/*ElementalClaim.lossTypeCode = SOURCE.lossTypeCode,*/
					ElementalClaim.lossTypeDescription = SOURCE.lossTypeDescription,
					/*ElementalClaim.coverageTypeCode = SOURCE.coverageTypeCode,*/
					ElementalClaim.coverageTypeDescription = SOURCE.coverageTypeDescription,
					ElementalClaim.dateClaimClosed = SOURCE.dateClaimClosed,
					ElementalClaim.coverageStatus = SOURCE.coverageStatus,
					ElementalClaim.settlementAmount = COALESCE(
						SOURCE.cLT17SettlementAmount,
						SOURCE.cLT14SettlementAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.estimatedLossAmount = COALESCE(
						SOURCE.cLT17estimatedLossAmount,
						SOURCE.cLT14estimatedLossAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.reserveAmount = COALESCE(
						SOURCE.cLT17reserveAmount,
						SOURCE.cLT14reserveAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.totalInsuredAmount = COALESCE(
						SOURCE.cLT17totalInsuredAmount,
						SOURCE.cLT14totalInsuredAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.policyAmount = COALESCE(
						SOURCE.cLT17policyAmount,
						SOURCE.cLT14policyAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.replacementAmount = SOURCE.replacementAmount,
					ElementalClaim.actualCashAmount = SOURCE.actualCashAmount,
					ElementalClaim.buildingPolicyAmount = SOURCE.buildingPolicyAmount,
					ElementalClaim.buildingTotalInsuredAmount = SOURCE.buildingTotalInsuredAmount,
					ElementalClaim.buildingReplacementAmount = SOURCE.buildingReplacementAmount,
					ElementalClaim.buildingActualCashAmount = SOURCE.buildingActualCashAmount,
					ElementalClaim.buildingEstimatedLossAmount = SOURCE.buildingEstimatedLossAmount,
					ElementalClaim.contentPolicyAmount = SOURCE.contentPolicyAmount,
					ElementalClaim.contentTotalInsuredAmount = SOURCE.contentTotalInsuredAmount,
					ElementalClaim.contentReplacementAmount = SOURCE.contentReplacementAmount,
					ElementalClaim.contentActualCashAmount = SOURCE.contentActualCashAmount,
					ElementalClaim.contentEstimatedLossAmount = SOURCE.contentEstimatedLossAmount,
					ElementalClaim.stockPolicyAmount = SOURCE.stockPolicyAmount,
					ElementalClaim.stockTotalInsuredAmount = SOURCE.stockTotalInsuredAmount,
					ElementalClaim.stockReplacementAmount = SOURCE.stockReplacementAmount,
					ElementalClaim.stockActualCashAmount = SOURCE.stockActualCashAmount,
					ElementalClaim.stockEstimatedLossAmount = SOURCE.stockEstimatedLossAmount,
					ElementalClaim.lossOfUsePolicyAmount = SOURCE.lossOfUsePolicyAmount,
					ElementalClaim.lossOfUseTotalInsuredAmount = SOURCE.lossOfUseTotalInsuredAmount,
					ElementalClaim.lossOfUseReplacementAmount = SOURCE.lossOfUseReplacementAmount,
					ElementalClaim.lossOfUseActualCashAmount = SOURCE.lossOfUseActualCashAmount,
					ElementalClaim.lossOfUseEstimatedLossAmount = SOURCE.lossOfUseEstimatedLossAmount,
					ElementalClaim.otherPolicyAmount = SOURCE.otherPolicyAmount,
					ElementalClaim.otherTotalInsuredAmount = SOURCE.otherTotalInsuredAmount,
					ElementalClaim.otherReplacementAmount = SOURCE.otherReplacementAmount,
					ElementalClaim.otherActualCashAmount = SOURCE.otherActualCashAmount,
					ElementalClaim.otherEstimatedLossAmount = SOURCE.otherEstimatedLossAmount,
					ElementalClaim.buildingReserveAmount = SOURCE.buildingReserveAmount,
					ElementalClaim.buildingPaidAmount = SOURCE.buildingPaidAmount,
					ElementalClaim.contentReserveAmount = SOURCE.contentReserveAmount,
					ElementalClaim.contentPaidAmount = SOURCE.contentPaidAmount,
					ElementalClaim.stockReserveAmount = SOURCE.stockReserveAmount,
					ElementalClaim.stockPaidAmount = SOURCE.stockPaidAmount,
					ElementalClaim.lossOfUseReserve = SOURCE.lossOfUseReserve,
					ElementalClaim.lossOfUsePaid = SOURCE.lossOfUsePaid,
					ElementalClaim.otherReserveAmount = SOURCE.otherReserveAmount,
					ElementalClaim.otherPaidAmount = SOURCE.otherPaidAmount,
					/*ElementalClaim.isActive*/
					/*ElementalClaim.dateInserted*/
					ElementalClaim.deltaDate = @dateInserted
				
			FROM
				#ElementalClaimData AS SOURCE
			WHERE
				SOURCE.existingElementalClaimId IS NOT NULL
				AND ISNULL(SOURCE.cLT14CoverageType, 'NotProp') = 'PROP'
				AND ElementalClaim.elementalClaimId = SOURCE.existingElementalClaimId
				AND 
				(
					ElementalClaim.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					/*elementalClaimId*/
					/*claimId*/
					/*involvedPartyId*/
					/*isoClaimId*/
					/*involvedPartySequenceId*/
					OR ISNULL(ElementalClaim.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
					/*lossTypeCode*/
					OR ISNULL(ElementalClaim.lossTypeDescription,'~~~') <> ISNULL(SOURCE.lossTypeDescription,'~~~')
					/*coverageTypeCode*/
					OR ISNULL(ElementalClaim.coverageTypeDescription,'~~~') <> ISNULL(SOURCE.coverageTypeDescription,'~~~')
					OR ISNULL(ElementalClaim.dateClaimClosed,'99990101') <> ISNULL(SOURCE.dateClaimClosed,'99990101')
					OR ISNULL(ElementalClaim.coverageStatus,'~~~') <> ISNULL(SOURCE.coverageStatus,'~~~')
					OR ISNULL(ElementalClaim.settlementAmount,-1) <> COALESCE(
						SOURCE.cLT17SettlementAmount,
						SOURCE.cLT14SettlementAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.estimatedLossAmount <> COALESCE(
						SOURCE.cLT17estimatedLossAmount,
						SOURCE.cLT14estimatedLossAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.reserveAmount <> COALESCE(
						SOURCE.cLT17reserveAmount,
						SOURCE.cLT14reserveAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.totalInsuredAmount <> COALESCE(
						SOURCE.cLT17totalInsuredAmount,
						SOURCE.cLT14totalInsuredAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.policyAmount <> COALESCE(
						SOURCE.cLT17policyAmount,
						SOURCE.cLT14policyAmount,
						CAST(-1 AS MONEY)
					)
					OR ISNULL(ElementalClaim.replacementAmount,-1) <> ISNULL(SOURCE.replacementAmount,-1)
					OR ISNULL(ElementalClaim.actualCashAmount,-1) <> ISNULL(SOURCE.actualCashAmount,-1)
					OR ISNULL(ElementalClaim.buildingPolicyAmount,-1) <> ISNULL(SOURCE.buildingPolicyAmount,-1)
					OR ISNULL(ElementalClaim.buildingTotalInsuredAmount,-1) <> ISNULL(SOURCE.buildingTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.buildingReplacementAmount,-1) <> ISNULL(SOURCE.buildingReplacementAmount,-1)
					OR ISNULL(ElementalClaim.buildingActualCashAmount,-1) <> ISNULL(SOURCE.buildingActualCashAmount,-1)
					OR ISNULL(ElementalClaim.buildingEstimatedLossAmount,-1) <> ISNULL(SOURCE.buildingEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.contentPolicyAmount,-1) <> ISNULL(SOURCE.contentPolicyAmount,-1)
					OR ISNULL(ElementalClaim.contentTotalInsuredAmount,-1) <> ISNULL(SOURCE.contentTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.contentReplacementAmount,-1) <> ISNULL(SOURCE.contentReplacementAmount,-1)
					OR ISNULL(ElementalClaim.contentActualCashAmount,-1) <> ISNULL(SOURCE.contentActualCashAmount,-1)
					OR ISNULL(ElementalClaim.contentEstimatedLossAmount,-1) <> ISNULL(SOURCE.contentEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.stockPolicyAmount,-1) <> ISNULL(SOURCE.stockPolicyAmount,-1)
					OR ISNULL(ElementalClaim.stockTotalInsuredAmount,-1) <> ISNULL(SOURCE.stockTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.stockReplacementAmount,-1) <> ISNULL(SOURCE.stockReplacementAmount,-1)
					OR ISNULL(ElementalClaim.stockActualCashAmount,-1) <> ISNULL(SOURCE.stockActualCashAmount,-1)
					OR ISNULL(ElementalClaim.stockEstimatedLossAmount,-1) <> ISNULL(SOURCE.stockEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUsePolicyAmount,-1) <> ISNULL(SOURCE.lossOfUsePolicyAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseTotalInsuredAmount,-1) <> ISNULL(SOURCE.lossOfUseTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseReplacementAmount,-1) <> ISNULL(SOURCE.lossOfUseReplacementAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseActualCashAmount,-1) <> ISNULL(SOURCE.lossOfUseActualCashAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseEstimatedLossAmount,-1) <> ISNULL(SOURCE.lossOfUseEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.otherPolicyAmount,-1) <> ISNULL(SOURCE.otherPolicyAmount,-1)
					OR ISNULL(ElementalClaim.otherTotalInsuredAmount,-1) <> ISNULL(SOURCE.otherTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.otherReplacementAmount,-1) <> ISNULL(SOURCE.otherReplacementAmount,-1)
					OR ISNULL(ElementalClaim.otherActualCashAmount,-1) <> ISNULL(SOURCE.otherActualCashAmount,-1)
					OR ISNULL(ElementalClaim.otherEstimatedLossAmount,-1) <> ISNULL(SOURCE.otherEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.buildingReserveAmount,-1) <> ISNULL(SOURCE.buildingReserveAmount,-1)
					OR ISNULL(ElementalClaim.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
					OR ISNULL(ElementalClaim.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
					OR ISNULL(ElementalClaim.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
					OR ISNULL(ElementalClaim.stockReserveAmount,-1) <> ISNULL(SOURCE.stockReserveAmount,-1)
					OR ISNULL(ElementalClaim.stockPaidAmount,-1) <> ISNULL(SOURCE.stockPaidAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseReserve,-1) <> ISNULL(SOURCE.lossOfUseReserve,-1)
					OR ISNULL(ElementalClaim.lossOfUsePaid,-1) <> ISNULL(SOURCE.lossOfUsePaid,-1)
					OR ISNULL(ElementalClaim.otherReserveAmount,-1) <> ISNULL(SOURCE.otherReserveAmount,-1)
					OR ISNULL(ElementalClaim.otherPaidAmount,-1) <> ISNULL(SOURCE.otherPaidAmount,-1)
					/*isActive*/
					/*ElementalClaim.dateInserted*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'UpdateNonPropElementalClaimData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.ElementalClaim WITH (TABLOCKX)
				SET
					/*ElementalClaim.elementalClaimId*/
					ElementalClaim.claimId = SOURCE.claimId,
					ElementalClaim.involvedPartyId = SOURCE.involvedPartyId,
					/*ElementalClaim.isoClaimId*/
					/*ElementalClaim.involvedPartySequenceId*/
					ElementalClaim.adjusterId = SOURCE.adjusterId,
					/*ElementalClaim.lossTypeCode = SOURCE.lossTypeCode,*/
					ElementalClaim.lossTypeDescription = SOURCE.lossTypeDescription,
					/*ElementalClaim.coverageTypeCode = SOURCE.coverageTypeCode,*/
					ElementalClaim.coverageTypeDescription = SOURCE.coverageTypeDescription,
					ElementalClaim.dateClaimClosed = SOURCE.dateClaimClosed,
					ElementalClaim.coverageStatus = SOURCE.coverageStatus,
					ElementalClaim.settlementAmount = COALESCE(
						SOURCE.cLT14SettlementAmount,
						SOURCE.cLT17SettlementAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.estimatedLossAmount = COALESCE(
						SOURCE.cLT14estimatedLossAmount,
						SOURCE.cLT17estimatedLossAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.reserveAmount = COALESCE(
						SOURCE.cLT14reserveAmount,
						SOURCE.cLT17reserveAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.totalInsuredAmount = COALESCE(
						SOURCE.cLT14totalInsuredAmount,
						SOURCE.cLT17totalInsuredAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.policyAmount = COALESCE(
						SOURCE.cLT14policyAmount,
						SOURCE.cLT17policyAmount,
						CAST(0 AS MONEY)
					),
					ElementalClaim.replacementAmount = SOURCE.replacementAmount,
					ElementalClaim.actualCashAmount = SOURCE.actualCashAmount,
					ElementalClaim.buildingPolicyAmount = SOURCE.buildingPolicyAmount,
					ElementalClaim.buildingTotalInsuredAmount = SOURCE.buildingTotalInsuredAmount,
					ElementalClaim.buildingReplacementAmount = SOURCE.buildingReplacementAmount,
					ElementalClaim.buildingActualCashAmount = SOURCE.buildingActualCashAmount,
					ElementalClaim.buildingEstimatedLossAmount = SOURCE.buildingEstimatedLossAmount,
					ElementalClaim.contentPolicyAmount = SOURCE.contentPolicyAmount,
					ElementalClaim.contentTotalInsuredAmount = SOURCE.contentTotalInsuredAmount,
					ElementalClaim.contentReplacementAmount = SOURCE.contentReplacementAmount,
					ElementalClaim.contentActualCashAmount = SOURCE.contentActualCashAmount,
					ElementalClaim.contentEstimatedLossAmount = SOURCE.contentEstimatedLossAmount,
					ElementalClaim.stockPolicyAmount = SOURCE.stockPolicyAmount,
					ElementalClaim.stockTotalInsuredAmount = SOURCE.stockTotalInsuredAmount,
					ElementalClaim.stockReplacementAmount = SOURCE.stockReplacementAmount,
					ElementalClaim.stockActualCashAmount = SOURCE.stockActualCashAmount,
					ElementalClaim.stockEstimatedLossAmount = SOURCE.stockEstimatedLossAmount,
					ElementalClaim.lossOfUsePolicyAmount = SOURCE.lossOfUsePolicyAmount,
					ElementalClaim.lossOfUseTotalInsuredAmount = SOURCE.lossOfUseTotalInsuredAmount,
					ElementalClaim.lossOfUseReplacementAmount = SOURCE.lossOfUseReplacementAmount,
					ElementalClaim.lossOfUseActualCashAmount = SOURCE.lossOfUseActualCashAmount,
					ElementalClaim.lossOfUseEstimatedLossAmount = SOURCE.lossOfUseEstimatedLossAmount,
					ElementalClaim.otherPolicyAmount = SOURCE.otherPolicyAmount,
					ElementalClaim.otherTotalInsuredAmount = SOURCE.otherTotalInsuredAmount,
					ElementalClaim.otherReplacementAmount = SOURCE.otherReplacementAmount,
					ElementalClaim.otherActualCashAmount = SOURCE.otherActualCashAmount,
					ElementalClaim.otherEstimatedLossAmount = SOURCE.otherEstimatedLossAmount,
					ElementalClaim.buildingReserveAmount = SOURCE.buildingReserveAmount,
					ElementalClaim.buildingPaidAmount = SOURCE.buildingPaidAmount,
					ElementalClaim.contentReserveAmount = SOURCE.contentReserveAmount,
					ElementalClaim.contentPaidAmount = SOURCE.contentPaidAmount,
					ElementalClaim.stockReserveAmount = SOURCE.stockReserveAmount,
					ElementalClaim.stockPaidAmount = SOURCE.stockPaidAmount,
					ElementalClaim.lossOfUseReserve = SOURCE.lossOfUseReserve,
					ElementalClaim.lossOfUsePaid = SOURCE.lossOfUsePaid,
					ElementalClaim.otherReserveAmount = SOURCE.otherReserveAmount,
					ElementalClaim.otherPaidAmount = SOURCE.otherPaidAmount,
					/*ElementalClaim.isActive*/
					/*ElementalClaim.dateInserted*/
					ElementalClaim.deltaDate = @dateInserted
			FROM
				#ElementalClaimData AS SOURCE
			WHERE
				SOURCE.existingElementalClaimId IS NOT NULL
				AND ISNULL(SOURCE.cLT14CoverageType, 'NotProp') <> 'PROP'
				AND ElementalClaim.elementalClaimId = SOURCE.existingElementalClaimId
				AND 
				(
					ElementalClaim.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
					/*elementalClaimId*/
					/*claimId*/
					/*involvedPartyId*/
					/*isoClaimId*/
					/*involvedPartySequenceId*/
					OR ISNULL(ElementalClaim.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
					/*lossTypeCode*/
					OR ISNULL(ElementalClaim.lossTypeDescription,'~~~') <> ISNULL(SOURCE.lossTypeDescription,'~~~')
					/*coverageTypeCode*/
					OR ISNULL(ElementalClaim.coverageTypeDescription,'~~~') <> ISNULL(SOURCE.coverageTypeDescription,'~~~')
					OR ISNULL(ElementalClaim.dateClaimClosed,'99990101') <> ISNULL(SOURCE.dateClaimClosed,'99990101')
					OR ISNULL(ElementalClaim.coverageStatus,'~~~') <> ISNULL(SOURCE.coverageStatus,'~~~')
					OR ISNULL(ElementalClaim.settlementAmount,-1) <> COALESCE(
						SOURCE.cLT14SettlementAmount,
						SOURCE.cLT17SettlementAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.estimatedLossAmount <> COALESCE(
						SOURCE.cLT14estimatedLossAmount,
						SOURCE.cLT17estimatedLossAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.reserveAmount <> COALESCE(
						SOURCE.cLT14reserveAmount,
						SOURCE.cLT17reserveAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.totalInsuredAmount <> COALESCE(
						SOURCE.cLT14totalInsuredAmount,
						SOURCE.cLT17totalInsuredAmount,
						CAST(-1 AS MONEY)
					)
					OR ElementalClaim.policyAmount <> COALESCE(
						SOURCE.cLT14policyAmount,
						SOURCE.cLT17policyAmount,
						CAST(-1 AS MONEY)
					)
					OR ISNULL(ElementalClaim.replacementAmount,-1) <> ISNULL(SOURCE.replacementAmount,-1)
					OR ISNULL(ElementalClaim.actualCashAmount,-1) <> ISNULL(SOURCE.actualCashAmount,-1)
					OR ISNULL(ElementalClaim.buildingPolicyAmount,-1) <> ISNULL(SOURCE.buildingPolicyAmount,-1)
					OR ISNULL(ElementalClaim.buildingTotalInsuredAmount,-1) <> ISNULL(SOURCE.buildingTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.buildingReplacementAmount,-1) <> ISNULL(SOURCE.buildingReplacementAmount,-1)
					OR ISNULL(ElementalClaim.buildingActualCashAmount,-1) <> ISNULL(SOURCE.buildingActualCashAmount,-1)
					OR ISNULL(ElementalClaim.buildingEstimatedLossAmount,-1) <> ISNULL(SOURCE.buildingEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.contentPolicyAmount,-1) <> ISNULL(SOURCE.contentPolicyAmount,-1)
					OR ISNULL(ElementalClaim.contentTotalInsuredAmount,-1) <> ISNULL(SOURCE.contentTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.contentReplacementAmount,-1) <> ISNULL(SOURCE.contentReplacementAmount,-1)
					OR ISNULL(ElementalClaim.contentActualCashAmount,-1) <> ISNULL(SOURCE.contentActualCashAmount,-1)
					OR ISNULL(ElementalClaim.contentEstimatedLossAmount,-1) <> ISNULL(SOURCE.contentEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.stockPolicyAmount,-1) <> ISNULL(SOURCE.stockPolicyAmount,-1)
					OR ISNULL(ElementalClaim.stockTotalInsuredAmount,-1) <> ISNULL(SOURCE.stockTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.stockReplacementAmount,-1) <> ISNULL(SOURCE.stockReplacementAmount,-1)
					OR ISNULL(ElementalClaim.stockActualCashAmount,-1) <> ISNULL(SOURCE.stockActualCashAmount,-1)
					OR ISNULL(ElementalClaim.stockEstimatedLossAmount,-1) <> ISNULL(SOURCE.stockEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUsePolicyAmount,-1) <> ISNULL(SOURCE.lossOfUsePolicyAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseTotalInsuredAmount,-1) <> ISNULL(SOURCE.lossOfUseTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseReplacementAmount,-1) <> ISNULL(SOURCE.lossOfUseReplacementAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseActualCashAmount,-1) <> ISNULL(SOURCE.lossOfUseActualCashAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseEstimatedLossAmount,-1) <> ISNULL(SOURCE.lossOfUseEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.otherPolicyAmount,-1) <> ISNULL(SOURCE.otherPolicyAmount,-1)
					OR ISNULL(ElementalClaim.otherTotalInsuredAmount,-1) <> ISNULL(SOURCE.otherTotalInsuredAmount,-1)
					OR ISNULL(ElementalClaim.otherReplacementAmount,-1) <> ISNULL(SOURCE.otherReplacementAmount,-1)
					OR ISNULL(ElementalClaim.otherActualCashAmount,-1) <> ISNULL(SOURCE.otherActualCashAmount,-1)
					OR ISNULL(ElementalClaim.otherEstimatedLossAmount,-1) <> ISNULL(SOURCE.otherEstimatedLossAmount,-1)
					OR ISNULL(ElementalClaim.buildingReserveAmount,-1) <> ISNULL(SOURCE.buildingReserveAmount,-1)
					OR ISNULL(ElementalClaim.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
					OR ISNULL(ElementalClaim.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
					OR ISNULL(ElementalClaim.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
					OR ISNULL(ElementalClaim.stockReserveAmount,-1) <> ISNULL(SOURCE.stockReserveAmount,-1)
					OR ISNULL(ElementalClaim.stockPaidAmount,-1) <> ISNULL(SOURCE.stockPaidAmount,-1)
					OR ISNULL(ElementalClaim.lossOfUseReserve,-1) <> ISNULL(SOURCE.lossOfUseReserve,-1)
					OR ISNULL(ElementalClaim.lossOfUsePaid,-1) <> ISNULL(SOURCE.lossOfUsePaid,-1)
					OR ISNULL(ElementalClaim.otherReserveAmount,-1) <> ISNULL(SOURCE.otherReserveAmount,-1)
					OR ISNULL(ElementalClaim.otherPaidAmount,-1) <> ISNULL(SOURCE.otherPaidAmount,-1)
					/*isActive*/
					/*ElementalClaim.dateInserted*/
				);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 4,
				@stepDescription = 'InsertNewElementalClaimData',
				@stepStartDateTime = GETDATE();

			INSERT INTO dbo.ElementalClaim WITH (TABLOCKX)
			(
				/*elementalClaimId,*/
				claimId,
				involvedPartyId,
				isoClaimId,
				involvedPartySequenceId,
				adjusterId,
				lossTypeCode,
				lossTypeDescription,
				coverageTypeCode,
				coverageTypeDescription,
				dateClaimClosed,
				coverageStatus,
				settlementAmount,
				estimatedLossAmount,
				reserveAmount,
				totalInsuredAmount,
				policyAmount,
				replacementAmount,
				actualCashAmount,
				buildingPolicyAmount,
				buildingTotalInsuredAmount,
				buildingReplacementAmount,
				buildingActualCashAmount,
				buildingEstimatedLossAmount,
				contentPolicyAmount,
				contentTotalInsuredAmount,
				contentReplacementAmount,
				contentActualCashAmount,
				contentEstimatedLossAmount,
				stockPolicyAmount,
				stockTotalInsuredAmount,
				stockReplacementAmount,
				stockActualCashAmount,
				stockEstimatedLossAmount,
				lossOfUsePolicyAmount,
				lossOfUseTotalInsuredAmount,
				lossOfUseReplacementAmount,
				lossOfUseActualCashAmount,
				lossOfUseEstimatedLossAmount,
				otherPolicyAmount,
				otherTotalInsuredAmount,
				otherReplacementAmount,
				otherActualCashAmount,
				otherEstimatedLossAmount,
				buildingReserveAmount,
				buildingPaidAmount,
				contentReserveAmount,
				contentPaidAmount,
				stockReserveAmount,
				stockPaidAmount,
				lossOfUseReserve,
				lossOfUsePaid,
				otherReserveAmount,
				otherPaidAmount,
				isActive,
				dateInserted,
				deltaDate
			)
			SELECT
				/*SOURCE.existingElementalClaimId,*/
				SOURCE.claimId,
				SOURCE.involvedPartyId,
				SOURCE.isoClaimId,
				SOURCE.involvedPartySequenceId,
				SOURCE.adjusterId,
				SOURCE.lossTypeCode,
				SOURCE.lossTypeDescription,
				SOURCE.coverageTypeCode,
				SOURCE.coverageTypeDescription,
				SOURCE.dateClaimClosed,
				SOURCE.coverageStatus,
				CASE
					WHEN
						ISNULL(SOURCE.cLT14CoverageType, 'NotProp') = 'PROP'
					THEN
						COALESCE(
							SOURCE.cLT17SettlementAmount,
							SOURCE.cLT14SettlementAmount,
							CAST(0 AS MONEY)
						)
					ELSE
						COALESCE(
							SOURCE.cLT14SettlementAmount,
							SOURCE.cLT17SettlementAmount,
							CAST(0 AS MONEY)
						)
				END,
				CASE
					WHEN
						ISNULL(SOURCE.cLT14CoverageType, 'NotProp') = 'PROP'
					THEN
						COALESCE(
							SOURCE.cLT17estimatedLossAmount,
							SOURCE.cLT14estimatedLossAmount,
							CAST(0 AS MONEY)
						)
					ELSE
						COALESCE(
							SOURCE.cLT14estimatedLossAmount,
							SOURCE.cLT17estimatedLossAmount,
							CAST(0 AS MONEY)
						)
				END,
				CASE
					WHEN
						ISNULL(SOURCE.cLT14CoverageType, 'NotProp') = 'PROP'
					THEN
						COALESCE(
							SOURCE.cLT17reserveAmount,
							SOURCE.cLT14reserveAmount,
							CAST(0 AS MONEY)
						)
					ELSE
						COALESCE(
							SOURCE.cLT14reserveAmount,
							SOURCE.cLT17reserveAmount,
							CAST(0 AS MONEY)
						)
				END,
				CASE
					WHEN
						ISNULL(SOURCE.cLT14CoverageType, 'NotProp') = 'PROP'
					THEN
						COALESCE(
							SOURCE.cLT17totalInsuredAmount,
							SOURCE.cLT14totalInsuredAmount,
							CAST(0 AS MONEY)
						)
					ELSE
						COALESCE(
							SOURCE.cLT14totalInsuredAmount,
							SOURCE.cLT17totalInsuredAmount,
							CAST(0 AS MONEY)
						)
				END,
				CASE
					WHEN
						ISNULL(SOURCE.cLT14CoverageType, 'NotProp') = 'PROP'
					THEN
						COALESCE(
							SOURCE.cLT17policyAmount,
							SOURCE.cLT14policyAmount,
							CAST(0 AS MONEY)
						)
					ELSE
						COALESCE(
							SOURCE.cLT14policyAmount,
							SOURCE.cLT17policyAmount,
							CAST(0 AS MONEY)
						)
				END,
				SOURCE.replacementAmount,
				SOURCE.actualCashAmount,
				SOURCE.buildingPolicyAmount,
				SOURCE.buildingTotalInsuredAmount,
				SOURCE.buildingReplacementAmount,
				SOURCE.buildingActualCashAmount,
				SOURCE.buildingEstimatedLossAmount,
				SOURCE.contentPolicyAmount,
				SOURCE.contentTotalInsuredAmount,
				SOURCE.contentReplacementAmount,
				SOURCE.contentActualCashAmount,
				SOURCE.contentEstimatedLossAmount,
				SOURCE.stockPolicyAmount,
				SOURCE.stockTotalInsuredAmount,
				SOURCE.stockReplacementAmount,
				SOURCE.stockActualCashAmount,
				SOURCE.stockEstimatedLossAmount,
				SOURCE.lossOfUsePolicyAmount,
				SOURCE.lossOfUseTotalInsuredAmount,
				SOURCE.lossOfUseReplacementAmount,
				SOURCE.lossOfUseActualCashAmount,
				SOURCE.lossOfUseEstimatedLossAmount,
				SOURCE.otherPolicyAmount,
				SOURCE.otherTotalInsuredAmount,
				SOURCE.otherReplacementAmount,
				SOURCE.otherActualCashAmount,
				SOURCE.otherEstimatedLossAmount,
				SOURCE.buildingReserveAmount,
				SOURCE.buildingPaidAmount,
				SOURCE.contentReserveAmount,
				SOURCE.contentPaidAmount,
				SOURCE.stockReserveAmount,
				SOURCE.stockPaidAmount,
				SOURCE.lossOfUseReserve,
				SOURCE.lossOfUsePaid,
				SOURCE.otherReserveAmount,
				SOURCE.otherPaidAmount,
				CAST(1 AS BIT) AS isActive,
				@dateInserted AS dateInserted,
				@dateInserted AS deltaDate
				
			FROM
				#ElementalClaimData AS SOURCE
			WHERE
				SOURCE.existingElementalClaimId IS NULL;
			--OPTION (RECOMPILE);
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'DeprecateOrphanISOClaimData_elementalClaim',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.ElementalClaim WITH (TABLOCKX)
				SET
					ElementalClaim.isActive = CAST(0 AS BIT),
					ElementalClaim.deltaDate = @dateInserted
			FROM
				#LocationOfLossData AS SOURCE
			WHERE
				ElementalClaim.isoClaimId = SOURCE.isoClaimId
				AND ElementalClaim.isActive = CAST(1 AS BIT)
				AND ElementalClaim.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.ElementalClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();

		/*Log Activity*/
		INSERT INTO dbo.ElementalClaimActivityLog
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
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateElementalClaim; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 20190220
Author: Dan Ravaglia and Robert David Warner
Description: Mechanism for data-refresh of Pending FM Claim(s).
			
			!!!NOTE: State-LossType Combination Fitlers exist in this script.
				See Step1.
			
			Performance: Consider CROSSAPLY vs.
				second TempTable join for Exception-identification

***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/ 
ALTER PROCEDURE dbo.hsp_UpdateInsertFMPendingClaim
	@dateFilterParam DATETIME2(0) = NULL,
	@dailyLoadOverride BIT = 0
AS
BEGIN
	BEGIN TRY
		/*Current @dailyLoadOverride-Wrapper required due to how multi-execute scheduling of ETL jobs is currently implimented*/
		IF(
			@dailyLoadOverride = 1
			OR NOT EXISTS
			(
				SELECT NULL
				FROM dbo.FMPendingClaimActivityLog
				WHERE
					FMPendingClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND FMPendingClaimActivityLog.isSuccessful = 1
					AND FMPendingClaimActivityLog.executionDateTime > DATEADD(HOUR,-12,GETDATE())
			)
		)
		BEGIN
			DECLARE @internalTransactionCount TINYINT = 0;
			IF (@@TRANCOUNT = 0)
			BEGIN
				BEGIN TRANSACTION;
				SET @internalTransactionCount = 1;
			END
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
				@stepId TINYINT,
				@stepDescription VARCHAR(1000),
				@stepStartDateTime DATETIME2(0),
				@stepEndDateTime DATETIME2(0),
				@recordsAffected BIGINT,
				@isSuccessful BIT,
				@stepExecutionNotes VARCHAR(1000);

			SELECT
				@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
				(
					COALESCE
					(
						@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
						MAX(FMPendingClaimActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
						CAST('2014-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				)
			FROM
				dbo.FMPendingClaimActivityLog
			WHERE
				FMPendingClaimActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND FMPendingClaimActivityLog.isSuccessful = 1;
			SET @sourceDateTime = @dateFilterParam;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureFMPendingClaimDataToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				V_ActiveElementalClaim.elementalClaimId,
				CAST(ISNULL(ExistingPendingClaim.uniqueInstanceValue,1) AS TINYINT) AS uniqueInstanceValue, 
				V_ActiveClaim.claimId,
				V_ActiveClaim.isoClaimId AS isoFileNumber,
				CAST('Pending' AS VARCHAR(25)) AS reportingStatus,
				(CAST(DATENAME(MONTH, FireMarshalController.projectedGenerationDate) AS VARCHAR(255)) + ' ' + CAST(YEAR(FireMarshalController.projectedGenerationDate) AS CHAR(4))) AS fireMarshallStatus,
				/*fireMarshallDate*/
				FireMarshalController.projectedGenerationDate,
				/*The following code block supports Propogating "any closed elemental-claim status" to the entire claim.
					Deprecated 20190712, per conversation with business leaders (Stephen Adams).
					Search for a similar description tag or for "ClaimOpenCloseAggregation" to uncomment supporting code- RDW*//*
					CASE
						WHEN
							COALESCE(ClaimOpenCloseAggregation.instancesOfClosedClaim,0) = 0
						THEN
							CAST(1 AS BIT)
						ELSE
							CAST(0 AS BIT)
					END AS claimIsOpen,
				*/
				CASE
					WHEN
						V_ActiveElementalClaim.dateClaimClosed IS NOT NULL
						OR V_ActiveElementalClaim.coverageStatus IN('C', 'CWP')
					THEN
						0
					ELSE
						1
				END AS claimIsOpen,
				V_ActiveClaim.systemDateReceived AS dateSubmittedToIso,
				V_ActiveClaim.originalClaimNumber,
				V_ActivePolicy.originalPolicyNumber,
				V_ActivePolicy.insuranceProviderOfficeCode,
				V_ActivePolicy.insuranceProviderCompanyCode,
				CompanyHeirarchy.Customer_lvl0 + ' (' + V_ActivePolicy.insuranceProviderCompanyCode + ')' AS companyName,
				CompanyHeirarchy.lvl1 AS affiliate1Code,
				CompanyHeirarchy.Customer_lvl1 + ' (' + CompanyHeirarchy.lvl1 + ')' AS affiliate1Name,
				CompanyHeirarchy.lvl2 AS affiliate2Code,
				CompanyHeirarchy.Customer_lvl2 + ' (' + CompanyHeirarchy.lvl2 + ')' AS affiliate2Name,
				CompanyHeirarchy.lvl3 AS groupCode,
				CompanyHeirarchy.Customer_lvl3 + ' (' + CompanyHeirarchy.lvl3 + ')' AS groupName,
				
				V_ActiveLocationOfLoss.originalAddressLine1 AS lossAddressLine1,
				V_ActiveLocationOfLoss.originalAddressLine2 AS lossAddressLine2,
				V_ActiveLocationOfLoss.originalCityName AS lossCityName,
				V_ActiveLocationOfLoss.originalStateCode AS lossStateCode,
				Lookup_States.State_Name AS lossStateName,
				V_ActiveLocationOfLoss.originalZipCode AS lossZipCode,
				V_ActiveLocationOfLoss.scrubbedCountyName AS lossGeoCounty,
				V_ActiveLocationOfLoss.latitude AS lossLatitude,
				V_ActiveLocationOfLoss.longitude AS lossLongitude,
				V_ActiveClaim.lossDescription,
				V_ActiveClaim.lossDescriptionExtended,
				V_ActiveClaim.dateOfLoss,
				
				V_ActiveElementalClaim.lossTypeCode,
				V_ActiveElementalClaim.lossTypeDescription,
				V_ActivePolicy.policyTypeCode,
				V_ActivePolicy.policyTypeDescription,
				V_ActiveElementalClaim.coverageTypeCode,
				V_ActiveElementalClaim.coverageTypeDescription,

				V_ActiveElementalClaim.estimatedLossAmount,
				V_ActiveElementalClaim.settlementAmount,
				V_ActiveElementalClaim.policyAmount,
				
				V_ActiveElementalClaim.buildingPaidAmount,
				V_ActiveElementalClaim.contentReserveAmount,
				V_ActiveElementalClaim.contentPaidAmount,
				CASE
					WHEN
						ISNULL(DuplicateRemovalFlagPartition.F_INCEND_FIRE,'0') = 'Y'
					THEN
						CAST(1 AS BIT)
					ELSE
						CAST(0 AS BIT)
				END AS isIncendiaryFire,
				
				V_ActiveClaim.isClaimUnderSIUInvestigation,
				V_ActiveClaim.siuCompanyName,
				V_ActiveClaim.siuRepresentativeFullName,
				V_ActiveClaim.siuWorkPhoneNumber,
				V_ActiveClaim.siuCellPhoneNumber,
				
				V_ActiveElementalClaim.involvedPartyId,
				InvolvedParty.fullName AS involvedPartyFullName,
				V_ActiveElementalClaim.adjusterId,
				V_ActiveElementalClaim.involvedPartySequenceId,
				
				V_ActiveElementalClaim.reserveAmount,
				V_ActiveElementalClaim.totalInsuredAmount,
				V_ActiveElementalClaim.replacementAmount,

				V_ActiveElementalClaim.actualCashAmount,
				V_ActiveElementalClaim.buildingPolicyAmount,
				V_ActiveElementalClaim.buildingTotalInsuredAmount,
				V_ActiveElementalClaim.buildingReplacementAmount,
				V_ActiveElementalClaim.buildingActualCashAmount,
				V_ActiveElementalClaim.buildingEstimatedLossAmount,
				V_ActiveElementalClaim.contentPolicyAmount,
				V_ActiveElementalClaim.contentTotalInsuredAmount,
				V_ActiveElementalClaim.contentReplacementAmount,
				V_ActiveElementalClaim.contentActualCashAmount,
				V_ActiveElementalClaim.contentEstimatedLossAmount,
				V_ActiveElementalClaim.stockPolicyAmount,
				V_ActiveElementalClaim.stockTotalInsuredAmount,
				V_ActiveElementalClaim.stockReplacementAmount,
				V_ActiveElementalClaim.stockActualCashAmount,
				V_ActiveElementalClaim.stockEstimatedLossAmount,
				V_ActiveElementalClaim.lossOfUsePolicyAmount,
				V_ActiveElementalClaim.lossOfUseTotalInsuredAmount,
				V_ActiveElementalClaim.lossOfUseReplacementAmount,
				V_ActiveElementalClaim.lossOfUseActualCashAmount,
				V_ActiveElementalClaim.lossOfUseEstimatedLossAmount,
				V_ActiveElementalClaim.otherPolicyAmount,
				V_ActiveElementalClaim.otherTotalInsuredAmount,
				V_ActiveElementalClaim.otherReplacementAmount,
				V_ActiveElementalClaim.otherActualCashAmount,
				V_ActiveElementalClaim.otherEstimatedLossAmount,
				V_ActiveElementalClaim.buildingReserveAmount,

				V_ActiveElementalClaim.stockReserveAmount,
				V_ActiveElementalClaim.stockPaidAmount,
				V_ActiveElementalClaim.lossOfUseReserve,
				V_ActiveElementalClaim.lossOfUsePaid,
				V_ActiveElementalClaim.otherReserveAmount,
				V_ActiveElementalClaim.otherPaidAmount,

				/*isActive*/
				/*isCurrent*/
				/*dateInserted*/
				V_ActiveLocationOfLoss.scrubbedCountyFIPS AS lossGeoCountyFipsCode
				INTO #PendingFMClaimDataToInsert
			FROM
				dbo.FireMarshalController
				INNER JOIN [ClaimSearch_Prod].dbo.Lookup_States WITH (NOLOCK)
					ON FireMarshalController.fmStateCode = Lookup_States.State_Abb
				INNER JOIN dbo.V_ActiveLocationOfLoss WITH (NOLOCK)
					ON FireMarshalController.fmStateCode = V_ActiveLocationOfLoss.originalStateCode
				INNER JOIN dbo.V_ActiveClaim WITH (NOLOCK)
					ON V_ActiveClaim.isoClaimId = V_ActiveLocationOfLoss.isoClaimId
				INNER JOIN dbo.V_ActivePolicy WITH (NOLOCK)
					ON V_ActiveClaim.isoClaimId = V_ActivePolicy.isoClaimId
				INNER JOIN dbo.V_ActiveElementalClaim WITH (NOLOCK)
					ON V_ActiveClaim.claimId = V_ActiveElementalClaim.claimId
				INNER JOIN dbo.InvolvedParty
					ON V_ActiveElementalClaim.involvedPartyId = InvolvedParty.involvedPartyId
				INNER JOIN ClaimSearch_Prod.dbo.V_MM_Hierarchy AS CompanyHeirarchy WITH (NOLOCK)
					ON V_ActivePolicy.insuranceProviderCompanyCode = CompanyHeirarchy.lvl0
				/*fire indicator, from 18, join on I_ALLCLM, colName=F_INCEND_FIRE*/
				LEFT OUTER JOIN (
					SELECT
						CLT00018.I_ALLCLM,
						CLT00018.F_INCEND_FIRE,
						ROW_NUMBER() OVER(
							PARTITION BY
									CLT00018.I_ALLCLM
							ORDER BY
									CLT00018.Date_Insert DESC
						) AS incendiaryFireUniqueInstanceValue
					FROM
						dbo.FireMarshalDriver
						INNER JOIN [ClaimSearch_Prod].dbo.CLT00018 WITH (NOLOCK)
							ON FireMarshalDriver.isoClaimId = CLT00018.I_ALLCLM
				) DuplicateRemovalFlagPartition
					ON V_ActiveClaim.isoClaimId = DuplicateRemovalFlagPartition.I_ALLCLM
				/*The following code block supports Propogating "any closed elemental-claim status" to the entire claim.
					Deprecated 20190712, per conversation with business leaders (Stephen Adams). - RDW*//*
					LEFT OUTER JOIN(
						SELECT
							InnerActiveElementalClaim.claimId,
							SUM
							(
								CASE
									WHEN
										InnerActiveElementalClaim.dateClaimClosed IS NOT NULL
										OR InnerActiveElementalClaim.coverageStatus IN('C', 'CWP')
									THEN
										1
									ELSE
										0
								END
							) AS instancesOfClosedClaim
						FROM
							dbo.V_ActiveElementalClaim AS InnerActiveElementalClaim WITH (NOLOCK)
						GROUP BY
							InnerActiveElementalClaim.claimId
					) AS ClaimOpenCloseAggregation
						ON V_ActiveElementalClaim.claimId = ClaimOpenCloseAggregation.claimId
				*/
				/*DevNote: The following LO-join against the FireMarshalClaimSendHistory object
					is paired with an IS-NULL filter; this is to ensure that duplicate claim-
					-representation	on the dashboard is prevented*/
				LEFT OUTER JOIN dbo.V_ActiveFireMarshalClaimSendHistory
					ON V_ActiveElementalClaim.isoClaimId = V_ActiveFireMarshalClaimSendHistory.isoFileNumber
					AND V_ActiveFireMarshalClaimSendHistory.reportingStatus = 'Sent'
				LEFT OUTER JOIN dbo.FireMarshalPendingClaim AS ExistingPendingClaim
					ON V_ActiveElementalClaim.elementalClaimId = ExistingPendingClaim.elementalClaimId
			WHERE
				FireMarshalController.endDate IS NULL
				AND FireMarshalController.fmStateStatusCode = 'A'
				AND FireMarshalController.projectedGenerationDate IS NOT NULL
				AND ISNULL(ExistingPendingClaim.isCurrent,1) = 1
				AND V_ActiveFireMarshalClaimSendHistory.claimId IS NULL
				AND ISNULL(DuplicateRemovalFlagPartition.incendiaryFireUniqueInstanceValue,1) = 1
				AND (
					V_ActiveElementalClaim.lossTypeCode = 'FIRE'
					OR (
						V_ActiveElementalClaim.lossTypeCode = 'EXPL'
						AND V_ActiveLocationOfLoss.originalStateCode IN ('KY', 'FL')
					)
					OR (
						V_ActiveElementalClaim.lossTypeCode = 'LGHT'
						AND V_ActiveLocationOfLoss.originalStateCode = 'KY'
					)
				);

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'IdentifyClaimQualificationExceptions',
				@stepStartDateTime = GETDATE();
			
			/*Proper logic for FMClaimQualificationRequirementSetBehavior is NOT FULLY IMPLIMENTED;
				currently only works for 'GA' and 'KS'
			*/
			SELECT
				ValidatedFMStatus.elementalClaimId,
				ValidatedFMStatus.uniqueInstanceValue,
				CASE
					WHEN
						/*LEFT(ValidatedFMStatus.fireMarshallStatusValue,7) = 'Pending'*/
						RIGHT(ValidatedFMStatus.fireMarshallStatusValue,4) LIKE '[0-9][0-9][0-9][0-9]'
					THEN
						'Pending'
					ELSE
						'Exception'
				END  AS reportingStatus,
				ValidatedFMStatus.fireMarshallStatusValue AS fireMarshallStatus
				INTO #PendingClaimWithException
			FROM
				(
					SELECT
						InnerPendingFMClaimDataToInsert.elementalClaimid,
						InnerPendingFMClaimDataToInsert.uniqueInstanceValue,
						InnerPendingFMClaimDataToInsert.lossStateCode,
						CASE
							WHEN 
								InnerPendingFMClaimDataToInsert.lossStateCode = 'GA'
							THEN
								CASE
									WHEN
										InnerPendingFMClaimDataToInsert.claimIsOpen = 1
									THEN
										CASE
											WHEN
												ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) = 0
											THEN
												CAST('Settlement Amount Missing' AS VARCHAR(255))
											ELSE
												InnerPendingFMClaimDataToInsert.fireMarshallStatus	
										END
									ELSE
										InnerPendingFMClaimDataToInsert.fireMarshallStatus
								END
							WHEN
								InnerPendingFMClaimDataToInsert.lossStateCode = 'KS'
							THEN
								CASE
									WHEN
										ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescription)),'Fire') IN (
											'Fire',
											'',
											'blank'
										)
										AND ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescriptionExtended)),'Fire') IN (
											'Fire',
											'',
											'blank'
										)
										AND (
											ISNULL(InnerPendingFMClaimDataToInsert.estimatedLossAmount,0) <= 0
											AND ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) <= 0
										)
									THEN
										CAST('Loss Description Invalid and Estimated and/or Settlement Amount Missing' AS VARCHAR(255))
									WHEN
										ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescription)),'Fire') IN (
											'Fire',
											'',
											'blank'
										)
										AND ISNULL(LTRIM(RTRIM(InnerPendingFMClaimDataToInsert.lossDescriptionExtended)),'Fire') IN (
											'Fire',
											'',
											'blank'
										)
									THEN
										CAST('Loss Description Invalid' AS VARCHAR(255))
									WHEN
										ISNULL(InnerPendingFMClaimDataToInsert.estimatedLossAmount,0) <= 0
										AND ISNULL(InnerPendingFMClaimDataToInsert.settlementAmount,0) <= 0
									THEN
										CAST('Estimated and/or Settlement Amount Missing' AS VARCHAR(255))
									ELSE
										InnerPendingFMClaimDataToInsert.fireMarshallStatus	
								END
							ELSE
								/*'Status temporarily unavailable, pending Development Update'*/
								InnerPendingFMClaimDataToInsert.fireMarshallStatus
						END AS fireMarshallStatusValue
					FROM
						#PendingFMClaimDataToInsert AS InnerPendingFMClaimDataToInsert
				) AS ValidatedFMStatus
				INNER JOIN dbo.FireMarshalController
					ON ValidatedFMStatus.lossStateCode = FireMarshalController.fmStateCode
			WHERE
				FireMarshalController.endDate IS NULL
				AND FireMarshalController.fmStateStatusCode = 'A'
				AND FireMarshalController.projectedGenerationDate IS NOT NULL
				AND FireMarshalController.fmQualificationRequirmentSetId NOT IN
				(
					0,4
				);

			UPDATE #PendingFMClaimDataToInsert
				SET
					#PendingFMClaimDataToInsert.reportingStatus = #PendingClaimWithException.reportingStatus,
					#PendingFMClaimDataToInsert.fireMarshallStatus = #PendingClaimWithException.fireMarshallStatus
			FROM
				#PendingClaimWithException
			WHERE
				#PendingFMClaimDataToInsert.elementalClaimId = #PendingClaimWithException.elementalClaimId
				AND #PendingFMClaimDataToInsert.uniqueInstanceValue = #PendingClaimWithException.uniqueInstanceValue
				AND #PendingClaimWithException.reportingStatus = 'Exception'
				AND #PendingFMClaimDataToInsert.fireMarshallStatus <> ISNULL(#PendingClaimWithException.fireMarshallStatus,'~~~~')
			
			/*Hide KS Closed-claim Exceptions. Alternatively we can only CLOSED KS claims that meet other conditions (IE: are not exception etc.)*/
			DELETE FROM #PendingFMClaimDataToInsert
			WHERE
				#PendingFMClaimDataToInsert.lossStateCode = 'KS'
				AND #PendingFMClaimDataToInsert.claimIsOpen = 0
				AND #PendingFMClaimDataToInsert.reportingStatus = 'Exception';
			
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 3,
				@stepDescription = 'Update Existing Pending Claims',
				@stepStartDateTime = GETDATE();
			
			UPDATE dbo.FireMarshalPendingClaim
			SET
				FireMarshalPendingClaim.isCurrent = 0
			OUTPUT
				SOURCE.elementalClaimId,
				SOURCE.uniqueInstanceValue+1, /*incriment the uniqueInstanceValue*/
				SOURCE.claimId,
				SOURCE.isoFileNumber,
				SOURCE.reportingStatus,
				SOURCE.fireMarshallStatus,
				@dateInserted AS fireMarshallDate,
				SOURCE.claimIsOpen,
				SOURCE.dateSubmittedToIso,
				SOURCE.originalClaimNumber,
				SOURCE.originalPolicyNumber,
				SOURCE.insuranceProviderOfficeCode,
				SOURCE.insuranceProviderCompanyCode,
				SOURCE.companyName,
				SOURCE.affiliate1Code,
				SOURCE.affiliate1Name,
				SOURCE.affiliate2Code,
				SOURCE.affiliate2Name,
				SOURCE.groupCode,
				SOURCE.groupName,
				SOURCE.lossAddressLine1,
				SOURCE.lossAddressLine2,
				SOURCE.lossCityName,
				SOURCE.lossStateCode,
				SOURCE.lossStateName,
				SOURCE.lossZipCode,
				SOURCE.lossGeoCounty,
				SOURCE.lossLatitude,
				SOURCE.lossLongitude,
				SOURCE.lossDescription,
				SOURCE.lossDescriptionExtended,
				SOURCE.dateOfLoss,
				SOURCE.lossTypeCode,
				SOURCE.lossTypeDescription,
				SOURCE.policyTypeCode,
				SOURCE.policyTypeDescription,
				SOURCE.coverageTypeCode,
				SOURCE.coverageTypeDescription,
				SOURCE.estimatedLossAmount,
				SOURCE.settlementAmount,
				SOURCE.policyAmount,
				SOURCE.buildingPaidAmount,
				SOURCE.contentReserveAmount,
				SOURCE.contentPaidAmount,
				SOURCE.isIncendiaryFire,
				SOURCE.isClaimUnderSIUInvestigation,
				SOURCE.siuCompanyName,
				SOURCE.siuRepresentativeFullName,
				SOURCE.siuWorkPhoneNumber,
				SOURCE.siuCellPhoneNumber,
				SOURCE.involvedPartyId,
				SOURCE.involvedPartyFullName,
				SOURCE.adjusterId,
				SOURCE.involvedPartySequenceId,
				SOURCE.reserveAmount,
				SOURCE.totalInsuredAmount,
				SOURCE.replacementAmount,
				SOURCE.actualCashAmount,
				SOURCE.buildingPolicyAmount,
				SOURCE.buildingTotalInsuredAmount,
				SOURCE.buildingReplacementAmount,
				SOURCE.buildingActualCashAmount,
				SOURCE.buildingEstimatedLossAmount,
				SOURCE.contentPolicyAmount,
				SOURCE.contentTotalInsuredAmount,
				SOURCE.contentReplacementAmount,
				SOURCE.contentActualCashAmount,
				SOURCE.contentEstimatedLossAmount,
				SOURCE.stockPolicyAmount,
				SOURCE.stockTotalInsuredAmount,
				SOURCE.stockReplacementAmount,
				SOURCE.stockActualCashAmount,
				SOURCE.stockEstimatedLossAmount,
				SOURCE.lossOfUsePolicyAmount,
				SOURCE.lossOfUseTotalInsuredAmount,
				SOURCE.lossOfUseReplacementAmount,
				SOURCE.lossOfUseActualCashAmount,
				SOURCE.lossOfUseEstimatedLossAmount,
				SOURCE.otherPolicyAmount,
				SOURCE.otherTotalInsuredAmount,
				SOURCE.otherReplacementAmount,
				SOURCE.otherActualCashAmount,
				SOURCE.otherEstimatedLossAmount,
				SOURCE.buildingReserveAmount,
				SOURCE.stockReserveAmount,
				SOURCE.stockPaidAmount,
				SOURCE.lossOfUseReserve,
				SOURCE.lossOfUsePaid,
				SOURCE.otherReserveAmount,
				SOURCE.otherPaidAmount,
				1 AS isActive,
				1 AS isCurrent,
				@dateInserted AS dateInserted,
				SOURCE.lossGeoCountyFipsCode
				INTO dbo.FireMarshalPendingClaim
			FROM
				#PendingFMClaimDataToInsert AS SOURCE
			WHERE
				FireMarshalPendingClaim.elementalClaimId = SOURCE.elementalClaimId
					AND Source.uniqueInstanceValue = FireMarshalPendingClaim.uniqueInstanceValue
				AND
				(
					FireMarshalPendingClaim.claimId <> SOURCE.claimId
					OR ISNULL(FireMarshalPendingClaim.isoFileNumber,'~~~') <> ISNULL(SOURCE.isoFileNumber,'~~~')
					OR ISNULL(FireMarshalPendingClaim.reportingStatus,'~~~') <> ISNULL(SOURCE.reportingStatus,'~~~')
					OR ISNULL(FireMarshalPendingClaim.fireMarshallStatus,'~~~') <> ISNULL(SOURCE.fireMarshallStatus,'~~~')
					OR  FireMarshalPendingClaim.claimIsOpen <> SOURCE.claimIsOpen
					OR CAST(ISNULL(FireMarshalPendingClaim.dateSubmittedToIso,'99990101')AS DATE) <> CAST(ISNULL(SOURCE.dateSubmittedToIso,'99990101') AS DATE)
					OR ISNULL(FireMarshalPendingClaim.originalClaimNumber,'~~~') <> ISNULL(SOURCE.originalClaimNumber,'~~~')
					OR FireMarshalPendingClaim.originalPolicyNumber <> SOURCE.originalPolicyNumber
					OR FireMarshalPendingClaim.insuranceProviderOfficeCode <> SOURCE.insuranceProviderOfficeCode
					OR FireMarshalPendingClaim.insuranceProviderCompanyCode <> SOURCE.insuranceProviderCompanyCode
					OR FireMarshalPendingClaim.companyName <> SOURCE.companyName
					OR FireMarshalPendingClaim.affiliate1Code <> SOURCE.affiliate1Code
					OR FireMarshalPendingClaim.affiliate1Name <> SOURCE.affiliate1Name
					OR FireMarshalPendingClaim.affiliate2Code <> SOURCE.affiliate2Code
					OR FireMarshalPendingClaim.affiliate2Name <> SOURCE.affiliate2Name
					OR FireMarshalPendingClaim.groupCode <> SOURCE.groupCode
					OR FireMarshalPendingClaim.groupName <> SOURCE.groupName
					OR ISNULL(FireMarshalPendingClaim.lossAddressLine1,'~~~') <> ISNULL(SOURCE.lossAddressLine1,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossAddressLine2,'~~~') <> ISNULL(SOURCE.lossAddressLine2,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossCityName,'~~~') <> ISNULL(SOURCE.lossCityName,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossStateCode,'~~') <> ISNULL(SOURCE.lossStateCode,'~~')
					OR ISNULL(FireMarshalPendingClaim.lossStateName,'~~~') <> ISNULL(SOURCE.lossStateName,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossZipCode,'~~~~~') <> ISNULL(SOURCE.lossZipCode,'~~~~~')
					OR ISNULL(FireMarshalPendingClaim.lossGeoCounty,'~~~') <> ISNULL(SOURCE.lossGeoCounty,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossLatitude,'~~~') <> ISNULL(SOURCE.lossLatitude,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossLongitude,'~~~') <> ISNULL(SOURCE.lossLongitude,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossDescription,'~~~') <> ISNULL(SOURCE.lossDescription,'~~~')
					OR ISNULL(FireMarshalPendingClaim.lossDescriptionExtended,'~~~') <> ISNULL(SOURCE.lossDescriptionExtended,'~~~')
					OR CAST(ISNULL(FireMarshalPendingClaim.dateOfLoss,'99990101') AS DATE) <> CAST(ISNULL(SOURCE.dateOfLoss,'99990101') AS DATE)
					OR ISNULL(FireMarshalPendingClaim.lossTypeCode,'~~~~') <> ISNULL(SOURCE.lossTypeCode,'~~~~')
					OR ISNULL(FireMarshalPendingClaim.lossTypeDescription,'~~~') <> ISNULL(SOURCE.lossTypeDescription,'~~~')
					OR ISNULL(FireMarshalPendingClaim.policyTypeCode,'~~~~') <> ISNULL(SOURCE.policyTypeCode,'~~~~')
					OR ISNULL(FireMarshalPendingClaim.policyTypeDescription,'~~~') <> ISNULL(SOURCE.policyTypeDescription,'~~~')
					OR ISNULL(FireMarshalPendingClaim.coverageTypeCode,'~~~~') <> ISNULL(SOURCE.coverageTypeCode,'~~~~')
					OR ISNULL(FireMarshalPendingClaim.coverageTypeDescription,'~~~') <> ISNULL(SOURCE.coverageTypeDescription,'~~~')
					OR ISNULL(FireMarshalPendingClaim.estimatedLossAmount,-1) <> ISNULL(SOURCE.estimatedLossAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.settlementAmount,-1) <> ISNULL(SOURCE.settlementAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.policyAmount,-1) <> ISNULL(SOURCE.policyAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.buildingPaidAmount,-1) <> ISNULL(SOURCE.buildingPaidAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.contentReserveAmount,-1) <> ISNULL(SOURCE.contentReserveAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.contentPaidAmount,-1) <> ISNULL(SOURCE.contentPaidAmount,-1)
					OR FireMarshalPendingClaim.isIncendiaryFire <> SOURCE.isIncendiaryFire
					OR ISNULL(FireMarshalPendingClaim.isClaimUnderSIUInvestigation,0) <> ISNULL(SOURCE.isClaimUnderSIUInvestigation,0)
					OR ISNULL(FireMarshalPendingClaim.siuCompanyName,'~~~') <> ISNULL(SOURCE.siuCompanyName,'~~~')
					OR ISNULL(FireMarshalPendingClaim.siuRepresentativeFullName,'~~~') <> ISNULL(SOURCE.siuRepresentativeFullName,'~~~')
					OR ISNULL(FireMarshalPendingClaim.siuWorkPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuWorkPhoneNumber,'~~~~~~~~~~')
					OR ISNULL(FireMarshalPendingClaim.siuCellPhoneNumber,'~~~~~~~~~~') <> ISNULL(SOURCE.siuCellPhoneNumber,'~~~~~~~~~~')
					OR FireMarshalPendingClaim.involvedPartyId <> SOURCE.involvedPartyId
					OR ISNULL(FireMarshalPendingClaim.involvedPartyFullName,'~~~') <> ISNULL(SOURCE.involvedPartyFullName,'~~~')
					OR ISNULL(FireMarshalPendingClaim.adjusterId,-1) <> ISNULL(SOURCE.adjusterId,-1)
					OR ISNULL(FireMarshalPendingClaim.involvedPartySequenceId,-1) <> ISNULL(SOURCE.involvedPartySequenceId,-1)
					OR ISNULL(FireMarshalPendingClaim.reserveAmount,-1) <> ISNULL(SOURCE.reserveAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.totalInsuredAmount,-1) <> ISNULL(SOURCE.totalInsuredAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.replacementAmount,-1) <> ISNULL(SOURCE.replacementAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.actualCashAmount,-1) <> ISNULL(SOURCE.actualCashAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.buildingPolicyAmount,-1) <> ISNULL(SOURCE.buildingPolicyAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.buildingTotalInsuredAmount,-1) <> ISNULL(SOURCE.buildingTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.buildingReplacementAmount,-1) <> ISNULL(SOURCE.buildingReplacementAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.buildingActualCashAmount,-1) <> ISNULL(SOURCE.buildingActualCashAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.buildingEstimatedLossAmount,-1) <> ISNULL(SOURCE.buildingEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.contentPolicyAmount,-1) <> ISNULL(SOURCE.contentPolicyAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.contentTotalInsuredAmount,-1) <> ISNULL(SOURCE.contentTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.contentReplacementAmount,-1) <> ISNULL(SOURCE.contentReplacementAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.contentActualCashAmount,-1) <> ISNULL(SOURCE.contentActualCashAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.contentEstimatedLossAmount,-1) <> ISNULL(SOURCE.contentEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.stockPolicyAmount,-1) <> ISNULL(SOURCE.stockPolicyAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.stockTotalInsuredAmount,-1) <> ISNULL(SOURCE.stockTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.stockReplacementAmount,-1) <> ISNULL(SOURCE.stockReplacementAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.stockActualCashAmount,-1) <> ISNULL(SOURCE.stockActualCashAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.stockEstimatedLossAmount,-1) <> ISNULL(SOURCE.stockEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.lossOfUsePolicyAmount,-1) <> ISNULL(SOURCE.lossOfUsePolicyAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.lossOfUseTotalInsuredAmount,-1) <> ISNULL(SOURCE.lossOfUseTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.lossOfUseReplacementAmount,-1) <> ISNULL(SOURCE.lossOfUseReplacementAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.lossOfUseActualCashAmount,-1) <> ISNULL(SOURCE.lossOfUseActualCashAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.lossOfUseEstimatedLossAmount,-1) <> ISNULL(SOURCE.lossOfUseEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.otherPolicyAmount,-1) <> ISNULL(SOURCE.otherPolicyAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.otherTotalInsuredAmount,-1) <> ISNULL(SOURCE.otherTotalInsuredAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.otherReplacementAmount,-1) <> ISNULL(SOURCE.otherReplacementAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.otherActualCashAmount,-1) <> ISNULL(SOURCE.otherActualCashAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.otherEstimatedLossAmount,-1) <> ISNULL(SOURCE.otherEstimatedLossAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.buildingReserveAmount,-1) <> ISNULL(SOURCE.buildingReserveAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.stockReserveAmount,-1) <> ISNULL(SOURCE.stockReserveAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.stockPaidAmount,-1) <> ISNULL(SOURCE.stockPaidAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.lossOfUseReserve,-1) <> ISNULL(SOURCE.lossOfUseReserve,-1)
					OR ISNULL(FireMarshalPendingClaim.lossOfUsePaid,-1) <> ISNULL(SOURCE.lossOfUsePaid,-1)
					OR ISNULL(FireMarshalPendingClaim.otherReserveAmount,-1) <> ISNULL(SOURCE.otherReserveAmount,-1)
					OR ISNULL(FireMarshalPendingClaim.otherPaidAmount,-1) <> ISNULL(SOURCE.otherPaidAmount,-1)
					OR FireMarshalPendingClaim.isActive <> 1
					OR ISNULL(FireMarshalPendingClaim.lossGeoCountyFipsCode,'~~~~~') <> ISNULL(SOURCE.lossGeoCountyFipsCode,'~~~~~')
				);
			
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 4,
				@stepDescription = 'Insert New Pending Claims',
				@stepStartDateTime = GETDATE();
			
			INSERT INTO dbo.FireMarshalPendingClaim
			(
				elementalClaimId,
				uniqueInstanceValue,
				claimId,
				isoFileNumber,
				reportingStatus,
				fireMarshallStatus,
				fireMarshallDate,
				claimIsOpen,
				dateSubmittedToIso,
				originalClaimNumber,
				originalPolicyNumber,
				insuranceProviderOfficeCode,
				insuranceProviderCompanyCode,
				companyName,
				affiliate1Code,
				affiliate1Name,
				affiliate2Code,
				affiliate2Name,
				groupCode,
				groupName,
				lossAddressLine1,
				lossAddressLine2,
				lossCityName,
				lossStateCode,
				lossStateName,
				lossZipCode,
				lossGeoCounty,
				lossLatitude,
				lossLongitude,
				lossDescription,
				lossDescriptionExtended,
				dateOfLoss,
				lossTypeCode,
				lossTypeDescription,
				policyTypeCode,
				policyTypeDescription,
				coverageTypeCode,
				coverageTypeDescription,
				estimatedLossAmount,
				settlementAmount,
				policyAmount,
				buildingPaidAmount,
				contentReserveAmount,
				contentPaidAmount,
				isIncendiaryFire,
				isClaimUnderSIUInvestigation,
				siuCompanyName,
				siuRepresentativeFullName,
				siuWorkPhoneNumber,
				siuCellPhoneNumber,
				involvedPartyId,
				involvedPartyFullName,
				adjusterId,
				involvedPartySequenceId,
				reserveAmount,
				totalInsuredAmount,
				replacementAmount,
				actualCashAmount,
				buildingPolicyAmount,
				buildingTotalInsuredAmount,
				buildingReplacementAmount,
				buildingActualCashAmount,
				buildingEstimatedLossAmount,
				contentPolicyAmount,
				contentTotalInsuredAmount,
				contentReplacementAmount,
				contentActualCashAmount,
				contentEstimatedLossAmount,
				stockPolicyAmount,
				stockTotalInsuredAmount,
				stockReplacementAmount,
				stockActualCashAmount,
				stockEstimatedLossAmount,
				lossOfUsePolicyAmount,
				lossOfUseTotalInsuredAmount,
				lossOfUseReplacementAmount,
				lossOfUseActualCashAmount,
				lossOfUseEstimatedLossAmount,
				otherPolicyAmount,
				otherTotalInsuredAmount,
				otherReplacementAmount,
				otherActualCashAmount,
				otherEstimatedLossAmount,
				buildingReserveAmount,
				stockReserveAmount,
				stockPaidAmount,
				lossOfUseReserve,
				lossOfUsePaid,
				otherReserveAmount,
				otherPaidAmount,
				isActive,
				isCurrent,
				dateInserted,
				lossGeoCountyFipsCode
			)
			SELECT
				#PendingFMClaimDataToInsert.elementalClaimId,
				#PendingFMClaimDataToInsert.uniqueInstanceValue,
				#PendingFMClaimDataToInsert.claimId,
				#PendingFMClaimDataToInsert.isoFileNumber,
				#PendingFMClaimDataToInsert.reportingStatus,
				#PendingFMClaimDataToInsert.fireMarshallStatus,
				@dateInserted AS fireMarshallDate,
				#PendingFMClaimDataToInsert.claimIsOpen,
				#PendingFMClaimDataToInsert.dateSubmittedToIso,
				#PendingFMClaimDataToInsert.originalClaimNumber,
				#PendingFMClaimDataToInsert.originalPolicyNumber,
				#PendingFMClaimDataToInsert.insuranceProviderOfficeCode,
				#PendingFMClaimDataToInsert.insuranceProviderCompanyCode,
				#PendingFMClaimDataToInsert.companyName,
				#PendingFMClaimDataToInsert.affiliate1Code,
				#PendingFMClaimDataToInsert.affiliate1Name,
				#PendingFMClaimDataToInsert.affiliate2Code,
				#PendingFMClaimDataToInsert.affiliate2Name,
				#PendingFMClaimDataToInsert.groupCode,
				#PendingFMClaimDataToInsert.groupName,
				#PendingFMClaimDataToInsert.lossAddressLine1,
				#PendingFMClaimDataToInsert.lossAddressLine2,
				#PendingFMClaimDataToInsert.lossCityName,
				#PendingFMClaimDataToInsert.lossStateCode,
				#PendingFMClaimDataToInsert.lossStateName,
				#PendingFMClaimDataToInsert.lossZipCode,
				#PendingFMClaimDataToInsert.lossGeoCounty,
				#PendingFMClaimDataToInsert.lossLatitude,
				#PendingFMClaimDataToInsert.lossLongitude,
				#PendingFMClaimDataToInsert.lossDescription,
				#PendingFMClaimDataToInsert.lossDescriptionExtended,
				#PendingFMClaimDataToInsert.dateOfLoss,
				#PendingFMClaimDataToInsert.lossTypeCode,
				#PendingFMClaimDataToInsert.lossTypeDescription,
				#PendingFMClaimDataToInsert.policyTypeCode,
				#PendingFMClaimDataToInsert.policyTypeDescription,
				#PendingFMClaimDataToInsert.coverageTypeCode,
				#PendingFMClaimDataToInsert.coverageTypeDescription,
				#PendingFMClaimDataToInsert.estimatedLossAmount,
				#PendingFMClaimDataToInsert.settlementAmount,
				#PendingFMClaimDataToInsert.policyAmount,
				#PendingFMClaimDataToInsert.buildingPaidAmount,
				#PendingFMClaimDataToInsert.contentReserveAmount,
				#PendingFMClaimDataToInsert.contentPaidAmount,
				#PendingFMClaimDataToInsert.isIncendiaryFire,
				#PendingFMClaimDataToInsert.isClaimUnderSIUInvestigation,
				#PendingFMClaimDataToInsert.siuCompanyName,
				#PendingFMClaimDataToInsert.siuRepresentativeFullName,
				#PendingFMClaimDataToInsert.siuWorkPhoneNumber,
				#PendingFMClaimDataToInsert.siuCellPhoneNumber,
				#PendingFMClaimDataToInsert.involvedPartyId,
				#PendingFMClaimDataToInsert.involvedPartyFullName,
				#PendingFMClaimDataToInsert.adjusterId,
				#PendingFMClaimDataToInsert.involvedPartySequenceId,
				#PendingFMClaimDataToInsert.reserveAmount,
				#PendingFMClaimDataToInsert.totalInsuredAmount,
				#PendingFMClaimDataToInsert.replacementAmount,
				#PendingFMClaimDataToInsert.actualCashAmount,
				#PendingFMClaimDataToInsert.buildingPolicyAmount,
				#PendingFMClaimDataToInsert.buildingTotalInsuredAmount,
				#PendingFMClaimDataToInsert.buildingReplacementAmount,
				#PendingFMClaimDataToInsert.buildingActualCashAmount,
				#PendingFMClaimDataToInsert.buildingEstimatedLossAmount,
				#PendingFMClaimDataToInsert.contentPolicyAmount,
				#PendingFMClaimDataToInsert.contentTotalInsuredAmount,
				#PendingFMClaimDataToInsert.contentReplacementAmount,
				#PendingFMClaimDataToInsert.contentActualCashAmount,
				#PendingFMClaimDataToInsert.contentEstimatedLossAmount,
				#PendingFMClaimDataToInsert.stockPolicyAmount,
				#PendingFMClaimDataToInsert.stockTotalInsuredAmount,
				#PendingFMClaimDataToInsert.stockReplacementAmount,
				#PendingFMClaimDataToInsert.stockActualCashAmount,
				#PendingFMClaimDataToInsert.stockEstimatedLossAmount,
				#PendingFMClaimDataToInsert.lossOfUsePolicyAmount,
				#PendingFMClaimDataToInsert.lossOfUseTotalInsuredAmount,
				#PendingFMClaimDataToInsert.lossOfUseReplacementAmount,
				#PendingFMClaimDataToInsert.lossOfUseActualCashAmount,
				#PendingFMClaimDataToInsert.lossOfUseEstimatedLossAmount,
				#PendingFMClaimDataToInsert.otherPolicyAmount,
				#PendingFMClaimDataToInsert.otherTotalInsuredAmount,
				#PendingFMClaimDataToInsert.otherReplacementAmount,
				#PendingFMClaimDataToInsert.otherActualCashAmount,
				#PendingFMClaimDataToInsert.otherEstimatedLossAmount,
				#PendingFMClaimDataToInsert.buildingReserveAmount,
				#PendingFMClaimDataToInsert.stockReserveAmount,
				#PendingFMClaimDataToInsert.stockPaidAmount,
				#PendingFMClaimDataToInsert.lossOfUseReserve,
				#PendingFMClaimDataToInsert.lossOfUsePaid,
				#PendingFMClaimDataToInsert.otherReserveAmount,
				#PendingFMClaimDataToInsert.otherPaidAmount,
				1 AS isActive,
				1 AS isCurrent,
				@dateInserted AS dateInserted,
				#PendingFMClaimDataToInsert.lossGeoCountyFipsCode
			FROM
				#PendingFMClaimDataToInsert
				LEFT OUTER JOIN dbo.FireMarshalPendingClaim
					ON #PendingFMClaimDataToInsert.elementalClaimId = FireMarshalPendingClaim.elementalClaimId
						AND #PendingFMClaimDataToInsert.uniqueInstanceValue = FireMarshalPendingClaim.uniqueInstanceValue
			WHERE
				FireMarshalPendingClaim.elementalClaimId IS NULL;

			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;
				
				
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 100,
				@stepDescription = 'FullHistoryCompare DeActivateClaims',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.FireMarshalPendingClaim
			SET
				FireMarshalPendingClaim.isActive = 0,
				FireMarshalPendingClaim.dateInserted = @dateInserted
			FROM
				dbo.FireMarshalPendingClaim
				LEFT OUTER JOIN #PendingFMClaimDataToInsert
					ON FireMarshalPendingClaim.elementalClaimId = #PendingFMClaimDataToInsert.elementalClaimId
			WHERE
				FireMarshalPendingClaim.isActive = 1
				AND dbo.FireMarshalPendingClaim.isCurrent = 1
				AND #PendingFMClaimDataToInsert.elementalClaimId IS NULL;
		
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FMPendingClaimActivityLog
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
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

			IF (@internalTransactionCount = 1)
			BEGIN
				COMMIT TRANSACTION;
			END
		END;
	END TRY
	BEGIN CATCH
		/*Set Logging Variables for Current Step_End_Fail*/
		IF (@internalTransactionCount = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		END
		
		SELECT
			@stepEndDateTime = GETDATE(),
			@recordsAffected = @@ROWCOUNT,
			@isSuccessful = 0,
			@stepExecutionNotes = 'Error: ' + ERROR_MESSAGE();
		/*Log Activity*/
		INSERT INTO dbo.FMPendingClaimActivityLog
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
			@productCode,
			@sourceDateTime,
			@executionDateTime,
			@stepId,
			@stepDescription,
			@stepStartDateTime,
			@stepEndDateTime,
			@recordsAffected,
			@isSuccessful,
			@stepExecutionNotes;
		
		/*Optional: We can bubble the error up to the calling level.*/
		IF (@internalTransactionCount = 0)
		BEGIN
			DECLARE
				@raisError_message VARCHAR(2045) = /*Constructs an intuative error message*/
					'Error: in Step'
					+ CAST(@stepId AS VARCHAR(3))
					+ ' ('
					+ @stepDescription
					+ ') '
					+ 'of hsp_UpdateInsertFireMarshalDriver; ErrorMsg: '
					+ ERROR_MESSAGE(),
				@errorSeverity INT,
				@errorState INT;
			SELECT
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE();
			RAISERROR(@raisError_message,@errorSeverity,@errorState);
		END
	END CATCH
	
END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO







PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Jan 30 2020  3:32PM			*
*****************************************
COMMIT TRANSACTION

*/