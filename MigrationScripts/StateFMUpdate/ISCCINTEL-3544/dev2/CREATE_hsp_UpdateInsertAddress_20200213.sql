SET NOEXEC OFF;

USE ClaimSearch_Dev;
--USE ClaimSearch_Prod;

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
Date: 20200213
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
				/*isLocationOfLoss*/,
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
					WHEN /*when the scrub data is "more new" than the claimData, operate on scrub date*/
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
					Address.melissaMappingKey = SOURCE.melissaMappingKey
					/*isActive*/
					/*dateInserted*/
					Address.deltaDate = @dateInserted
			FROM
				#LocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId IS NOT NULL
				AND Address.addressId = SOURCE.existingAddressId
				AND 
				(
					ISNULL(Address.originalAddressLine1,'') <> ISNULL(SOURCE.originalAddressLine1,'')
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
				deltaDate,
			)
			SELECT
				SOURCE.isoClaimId,
				CAST(NULL AS INT) AS involvedPartySequenceId
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
				@stepId = 5,
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
					Address.melissaMappingKey = SOURCE.melissaMappingKey
					Address.deltaDate = @dateInserted
			FROM
				#NonLocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId IS NOT NULL
				AND Address.addressId = SOURCE.existingAddressId
				AND 
				(
					ISNULL(Address.originalAddressLine1,'') <> ISNULL(SOURCE.originalAddressLine1,'')
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
					/*OR ISNULL(Address.scrubbedCountyName,'') <> ISNULL(SOURCE.scrubbedCountyName,'')*/
					/*OR ISNULL(Address.scrubbedCountyFIPS,'') <> ISNULL(SOURCE.scrubbedCountyFIPS,'')*/
					/*OR ISNULL(Address.scrubbedCountryCode,'') <> ISNULL(SOURCE.scrubbedCountryCode,'')*/
					/*OR ISNULL(Address.longitude,'') <> ISNULL(SOURCE.longitude,'')
					OR ISNULL(Address.latitude,'') <> ISNULL(SOURCE.latitude,'')
					OR ISNULL(Address.geoAccuracy,'') <> ISNULL(SOURCE.geoAccuracy,'')
					OR ISNULL(Address.melissaMappingKey,-1) <> ISNULL(SOURCE.melissaMappingKey,-1)*/
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
				@stepId = 100, /*Step 6; however it is the last step so we default that to 100 for padding.*/
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
				SOURCE.involvedPartySequenceId
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

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
COMMIT TRANSACTION
20190114 : 5:27PM
20190122 : 9:52AM
20190122 : 4:50PM
20190130 : 9:54AM
*/