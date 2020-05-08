SET NOEXEC OFF;

--USE ClaimSearch_Prod;
USE ClaimSearch_Dev;

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
Date: 2019-01-08
Author: Robert David Warner
Description: Mechanism for Monthly Generate .
***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-08-22
Author: Robert David Warner
Description: Resolve boolean date error line 60
***********************************************
WorkItem: ISCCINTEL-2438
Date: 2019-11-04
Author: Robert David Warner
Description: Add Support for FM-State DML; including:
	LOC Lat, LOC long,.GeoCounty and GeoCountyFIPS
	
	Performance: NOTE: FULL HISTORY CrossDB Query finished in 34 min.
					Monitor the runtime of the Lat\Long function of this Sproc;
					it may be possible to add this functionality to the regular\daily run.
************************************************/
ALTER PROCEDURE dbo.hsp_FireMarshalSendClaims
	@mustMatchDB2FMProcess BIT = 0,
	@executionDate DATE = NULL
AS
BEGIN
	BEGIN TRY
		SELECT /*Initialization of DEFAULT-NULL executionDate-param*/
			@executionDate = COALESCE(@executionDate,GETDATE());
			
		DECLARE @internalTransactionCount TINYINT = 0,
			@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
			@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
			@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
			@sourceDateTime	DATETIME2(0),
			
			@stepId TINYINT,
			@stepDescription VARCHAR(1000),
			@stepStartDateTime DATETIME2(0),
			@stepEndDateTime DATETIME2(0),
			@recordsAffected BIGINT,
			@isSuccessful BIT,
			@stepExecutionNotes VARCHAR(1000);
				
		IF (@@TRANCOUNT = 0)
		BEGIN
			BEGIN TRANSACTION;
			SET @internalTransactionCount = 1;
		END
		/*Wrapper to allow for daily-execution schedule of otherwise monthly process*//*
		IF(
			NOT EXISTS
			(
				SELECT NULL
				FROM dbo.FireMarshalGenerationLog
				WHERE
					FireMarshalGenerationLog.stepId = 200 /*stepId for secondary-finalStep of UpdateInsert HSP*/
					AND FireMarshalGenerationLog.isSuccessful = 1
					AND MONTH(FireMarshalGenerationLog.executionDateTime) = MONTH(@executionDate)
			)
			AND DAY(@executionDate) < 5
		)
		BEGIN
			SELECT
				@sourceDateTime	= COALESCE(
					MAX(FireMarshalGenerationLog.executionDateTime),
					CAST('20150101' AS DATETIME2(0))
				)
			FROM
				(
					VALUES
						(1)
				) AS AtLeastOneRowHack (unusedValue)
				LEFT OUTER JOIN dbo.FireMarshalGenerationLog
					ON 1 = 1 /*HACK*/
						AND FireMarshalGenerationLog.stepId = 200 /*One Sided Filter*/
						AND FireMarshalGenerationLog.isSuccessful = 1 /*One Sided Filter*/;
			
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureLocationOfLossScrubbedDataToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				ExistingAddress.addressId AS existingAddressId,
				/*DevNotes:
					consider mapping these rows\updates based on:
						isoClaimId
						involvedPartySequenceId
					  since the surrogate-key\uniqueID of addressId is not as static as one would hope\like
					  (it is possible for it to be reverse-updated through the involvedPartySequenceId changing.
						DON'T assume you know what this means. Ask or investigate).
				*/
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
				INTO #LocationOfLossData
			FROM
				dbo.Address AS ExistingAddress WITH (NOLOCK)
				INNER JOIN ClaimSearch_Prod.dbo.CLT00001 WITH (NOLOCK)
					ON ExistingAddress.isoClaimId =  CLT00001.I_ALLCLM
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 WITH (NOLOCK)
					ON CLT00001.ALLCLMROWID = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.ALLCLMROWID
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
					ON CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
			WHERE
				ExistingAddress.isLocationOfLoss = 1
				AND ExistingAddress.dateInserted >= @sourceDateTime;
				
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
				@stepId = 200,
				@stepDescription = 'UpdateExistingLocationOfLossData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Address WITH (TABLOCKX)
				SET
					Address.scrubbedAddressLine1 = SOURCE.scrubbedAddressLine1,
					Address.scrubbedAddressLine2 = SOURCE.scrubbedAddressLine2,
					Address.scrubbedCityName = SOURCE.scrubbedCityName,
					Address.scrubbedStateCode = SOURCE.scrubbedStateCode,
					Address.scrubbedZipCode = SOURCE.scrubbedZipCode,
					Address.scrubbedZipCodeExtended = SOURCE.scrubbedZipCodeExtended,
					Address.scrubbedCountyName = SOURCE.scrubbedCountyName,
					Address.scrubbedCountyFIPS = SOURCE.scrubbedCountyFIPS,
					Address.scrubbedCountryCode = SOURCE.scrubbedCountryCode,
					Address.longitude = SOURCE.longitude,
					Address.latitude = SOURCE.latitude,
					Address.geoAccuracy = SOURCE.geoAccuracy,
					Address.dateInserted = @dateInserted
			FROM
				#LocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId = Address.addressId
				AND Address.isLocationOfLoss = 1
				AND 
				(
					ISNULL(Address.scrubbedAddressLine1,'') <> ISNULL(SOURCE.scrubbedAddressLine1,'')
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
				);
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
		
		/*	Currently, Non-LOL-Data does not require a Lat,Long,County, Etc. Update. 
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 2,
				@stepDescription = 'CaptureNonLocationOfLossDataToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				ExistingAddress.addressId AS existingAddressId,
				/*DevNotes:
					consider mapping these rows\updates based on:
						isoClaimId
						involvedPartySequenceId
					  since the surrogate-key\uniqueID of addressId is not as static as one would hope\like
					  (it is possible for it to be reverse-updated through the involvedPartySequenceId changing.
						DON'T assume you know what this means. Ask or investigate).
				*/
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
				INTO #NonLocationOfLossData
			FROM
				dbo.Address AS ExistingAddress WITH (NOLOCK)
				INNER JOIN [ClaimSearch_Prod].dbo.CLT00004 WITH (NOLOCK)
					ON ExistingAddress.isoClaimId = CLT00004.I_ALLCLM
						AND ExistingAddress.involvedPartySequenceId = CLT00004.I_NM_ADR
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
					ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
				LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
					ON CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey = CS_Lookup_EntityIDs.AddressKey
			WHERE
				ExistingAddress.isLocationOfLoss = 0
				AND ExistingAddress.dateInserted >= @sourceDateTime;
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;
				
			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
				@stepId = 200,
				@stepDescription = 'UpdateExistingNonLocationOfLossData',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Address WITH (TABLOCKX)
				SET
					Address.scrubbedAddressLine1 = SOURCE.scrubbedAddressLine1,
					Address.scrubbedAddressLine2 = SOURCE.scrubbedAddressLine2,
					Address.scrubbedCityName = SOURCE.scrubbedCityName,
					Address.scrubbedStateCode = SOURCE.scrubbedStateCode,
					Address.scrubbedZipCode = SOURCE.scrubbedZipCode,
					Address.scrubbedZipCodeExtended = SOURCE.scrubbedZipCodeExtended,
					Address.scrubbedCountyName = SOURCE.scrubbedCountyName,
					Address.scrubbedCountyFIPS = SOURCE.scrubbedCountyFIPS,
					Address.scrubbedCountryCode = SOURCE.scrubbedCountryCode,
					Address.longitude = SOURCE.longitude,
					Address.latitude = SOURCE.latitude,
					Address.geoAccuracy = SOURCE.geoAccuracy,
					Address.dateInserted = @dateInserted
			FROM
				#NonLocationOfLossData AS SOURCE
			WHERE
				SOURCE.existingAddressId = Address.addressId
				AND 
				(
					ISNULL(Address.scrubbedAddressLine1,'') <> ISNULL(SOURCE.scrubbedAddressLine1,'')
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
				);
			
				
			/*
				It appears as though all of the objects designated for exposing firemarshal data are
				updated at a table level each cycle; which means no additional updates should be necessary
				for this lattitude and longitude update to cycle up.
			*/
				
			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
			
			/*End of Non-LOL-Codeblock*/
			--*/
		END
		ELSE IF(
		--*/
		IF NOT EXISTS
			(
				SELECT NULL
				FROM dbo.FireMarshalGenerationLog
				WHERE
					FireMarshalGenerationLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
					AND FireMarshalGenerationLog.isSuccessful = 1
					AND MONTH(FireMarshalGenerationLog.executionDateTime) = MONTH(@executionDate)
			)
			AND DAY(GETDATE()) >= 5
		)
		BEGIN
			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'Migrate for FM Claims with qualifying ProjectedGenDate',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.FireMarshalPendingClaim
				SET
					FireMarshalPendingClaim.isActive = 0,
					FireMarshalPendingClaim.dateInserted = @dateInserted
				OUTPUT
					deleted.elementalClaimId,
					CASE
						WHEN
							FireMarshalClaimSendHistory.maxHistoricUniqueInstanceValue IS NULL
						THEN
							deleted.uniqueInstanceValue
						ELSE
							FireMarshalClaimSendHistory.maxHistoricUniqueInstanceValue +1
					END AS uniqueInstanceValue,
					deleted.claimId,
					deleted.isoFileNumber,
					'Sent' AS reportingStatus,
					CASE
						WHEN
							@mustMatchDB2FMProcess = 1 
						THEN
							(CAST(DATENAME(MONTH, DuplicateFMExtractPartition.dateInserted) AS VARCHAR(255)) + ' ' + CAST(YEAR(DuplicateFMExtractPartition.dateInserted) AS CHAR(4)))
						ELSE
							(CAST(DATENAME(MONTH, @dateInserted) AS VARCHAR(255)) + ' ' + CAST(YEAR(@dateInserted) AS CHAR(4)))
					END AS fireMarshallStatus,
					CASE
						WHEN
							@mustMatchDB2FMProcess = 1 
						THEN
							DuplicateFMExtractPartition.dateInserted
						ELSE
							@dateInserted
					END AS fireMarshallDate,
					deleted.claimIsOpen,
					deleted.dateSubmittedToIso,
					deleted.originalClaimNumber,
					deleted.originalPolicyNumber,
					deleted.insuranceProviderOfficeCode,
					deleted.insuranceProviderCompanyCode,
					deleted.companyName,
					deleted.affiliate1Code,
					deleted.affiliate1Name,
					deleted.affiliate2Code,
					deleted.affiliate2Name,
					deleted.groupCode,
					deleted.groupName,
					deleted.lossAddressLine1,
					deleted.lossAddressLine2,
					deleted.lossCityName,
					deleted.lossStateCode,
					deleted.lossStateName,
					deleted.lossZipCode,
					deleted.lossGeoCounty,
					deleted.lossLatitude,
					deleted.lossLongitude,
					deleted.lossDescription,
					deleted.lossDescriptionExtended,
					deleted.dateOfLoss,
					deleted.lossTypeCode,
					deleted.lossTypeDescription,
					deleted.policyTypeCode,
					deleted.policyTypeDescription,
					deleted.coverageTypeCode,
					deleted.coverageTypeDescription,
					deleted.estimatedLossAmount,
					deleted.settlementAmount,
					deleted.policyAmount,
					deleted.buildingPaidAmount,
					deleted.contentReserveAmount,
					deleted.contentPaidAmount,
					deleted.isIncendiaryFire,
					deleted.isClaimUnderSIUInvestigation,
					deleted.siuCompanyName,
					deleted.siuRepresentativeFullName,
					deleted.siuWorkPhoneNumber,
					deleted.siuCellPhoneNumber,
					deleted.involvedPartyId,
					deleted.involvedPartyFullName,
					deleted.adjusterId,
					deleted.involvedPartySequenceId,
					deleted.reserveAmount,
					deleted.totalInsuredAmount,
					deleted.replacementAmount,
					deleted.actualCashAmount,
					deleted.buildingPolicyAmount,
					deleted.buildingTotalInsuredAmount,
					deleted.buildingReplacementAmount,
					deleted.buildingActualCashAmount,
					deleted.buildingEstimatedLossAmount,
					deleted.contentPolicyAmount,
					deleted.contentTotalInsuredAmount,
					deleted.contentReplacementAmount,
					deleted.contentActualCashAmount,
					deleted.contentEstimatedLossAmount,
					deleted.stockPolicyAmount,
					deleted.stockTotalInsuredAmount,
					deleted.stockReplacementAmount,
					deleted.stockActualCashAmount,
					deleted.stockEstimatedLossAmount,
					deleted.lossOfUsePolicyAmount,
					deleted.lossOfUseTotalInsuredAmount,
					deleted.lossOfUseReplacementAmount,
					deleted.lossOfUseActualCashAmount,
					deleted.lossOfUseEstimatedLossAmount,
					deleted.otherPolicyAmount,
					deleted.otherTotalInsuredAmount,
					deleted.otherReplacementAmount,
					deleted.otherActualCashAmount,
					deleted.otherEstimatedLossAmount,
					deleted.buildingReserveAmount,
					deleted.stockReserveAmount,
					deleted.stockPaidAmount,
					deleted.lossOfUseReserve,
					deleted.lossOfUsePaid,
					deleted.otherReserveAmount,
					deleted.otherPaidAmount,
					deleted.isActive,
					@dateInserted AS dateInserted,
					deleted.lossGeoCountyFipsCode
				INTO dbo.FireMarshalClaimSendHistory
				FROM
					dbo.FireMarshalPendingClaim
					INNER JOIN dbo.FireMarshalController
						ON FireMarshalPendingClaim.lossStateCode = FireMarshalController.fmStateCode
							AND FireMarshalController.endDate IS NULL
					LEFT OUTER JOIN dbo.V_ActiveFireMarshalClaimSendHistory
						ON V_ActiveFireMarshalClaimSendHistory.elementalClaimId = FireMarshalPendingClaim.elementalClaimId
					LEFT OUTER JOIN(
						SELECT
							INNERFireMarshalClaimSendHistory.elementalClaimId,
							MAX(INNERFireMarshalClaimSendHistory.uniqueInstanceValue) AS maxHistoricUniqueInstanceValue
						FROM							
							dbo.FireMarshalClaimSendHistory AS INNERFireMarshalClaimSendHistory
						GROUP BY
							INNERFireMarshalClaimSendHistory.elementalClaimId
					) AS FireMarshalClaimSendHistory
						ON FireMarshalPendingClaim.elementalClaimId = FireMarshalClaimSendHistory.elementalClaimId
					LEFT OUTER JOIN 
					(
						SELECT
							V_Extract_FM_V1.I_ALLCLM,
							V_Extract_FM_V1.M_FUL_NM,
							CAST(CAST(V_Extract_FM_V1.DATE_INSERT AS CHAR(8))AS DATE) AS dateInserted,
							ROW_NUMBER() OVER (
								PARTITION BY
									V_Extract_FM_V1.I_ALLCLM,
									V_Extract_FM_V1.M_FUL_NM
								ORDER BY
									V_Extract_FM_V1.DATE_INSERT
							) AS uniqueInstanceValue
						FROM
							ClaimSearch_Prod.dbo.V_Extract_FM_V1
					) AS DuplicateFMExtractPartition
						ON FireMarshalPendingClaim.isoFileNumber = DuplicateFMExtractPartition.I_ALLCLM
						AND FireMarshalPendingClaim.involvedPartyFullName = DuplicateFMExtractPartition.M_FUL_NM
				WHERE
					FireMarshalPendingClaim.reportingStatus = 'Pending'
					AND V_ActiveFireMarshalClaimSendHistory.elementalClaimId IS NULL
					AND FireMarshalPendingClaim.isCurrent = 1
					AND FireMarshalPendingClaim.isActive = 1
					AND COALESCE(DuplicateFMExtractPartition.I_ALLCLM,CAST(NULLIF(@mustMatchDB2FMProcess,1) AS VARCHAR(11))) IS NOT NULL
					AND ISNULL(DuplicateFMExtractPartition.uniqueInstanceValue,1) = 1
					AND @dateInserted >= FireMarshalController.projectedGenerationDate

		/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
				@dateInserted,
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
				@stepDescription = 'Update Claim ProjectedGenerationDate, as needed',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.FireMarshalController
				SET
					FireMarshalController.endDate = @dateInserted	
				OUTPUT
					DELETED.fmStateCode,
					DELETED.fmQualificationRequirmentSetId,
					DELETED.fmStateStatusCode,
					DELETED.frequencyCode,
					DATEADD(
						MONTH,
						ProjectedDateToIncrementApply.projectedDateIncrementValue,
						DELETED.projectedGenerationDate
					) /*new projectedGenerationDate*/,
					DELETED.receivesPrint,
					DELETED.receivesFTP,
					DELETED.receivesEmail,
					DELETED.fmContactFirstName,
					DELETED.fmContactMiddleName,
					DELETED.fmContactLastName,
					DELETED.fmContactSuffixName,
					DELETED.fmContactDeptartmentName,
					DELETED.fmContactDivisionName,
					DELETED.fmContactDeliveryAddressLine1,
					DELETED.fmContactDeliveryAddressLine2,
					DELETED.fmContactDeliveryCity,
					DELETED.fmContactDeliveryStateCode,
					DELETED.fmContactZipCode,
					DELETED.fmContactTitleName,
					DELETED.fmContactSalutation,
					@dateInserted,
					DELETED.endDate
				INTO dbo.FireMarshalController
				(
					fmStateCode,
					fmQualificationRequirmentSetId,
					fmStateStatusCode,
					frequencyCode,
					projectedGenerationDate,
					receivesPrint,
					receivesFTP,
					receivesEmail,
					fmContactFirstName,
					fmContactMiddleName,
					fmContactLastName,
					fmContactSuffixName,
					fmContactDeptartmentName,
					fmContactDivisionName,
					fmContactDeliveryAddressLine1,
					fmContactDeliveryAddressLine2,
					fmContactDeliveryCity,
					fmContactDeliveryStateCode,
					fmContactZipCode,
					fmContactTitleName,
					fmContactSalutation,
					dateInserted,
					endDate
				)
			FROM
				dbo.FireMarshalController
				CROSS APPLY 
				(
					SELECT /*BehaviorLogic for daily\weekly\yearly NOT YET IMPLIMENTED*/
						CASE
							WHEN
								FireMarshalController.frequencyCode = 'Q'
								AND MONTH(FireMarshalController.projectedGenerationDate) < 4 
							THEN
								3
							WHEN
								FireMarshalController.frequencyCode = 'Q'
							THEN
								4
							WHEN
								FireMarshalController.frequencyCode = 'M'
							THEN
								1
							ELSE
								1
						END AS projectedDateIncrementValue
				) AS ProjectedDateToIncrementApply
			WHERE
				FireMarshalController.endDate IS NULL
				AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('99990101' AS DATE))<= CAST(@executionDate AS DATE)

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.FireMarshalGenerationLog
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
				@dateInserted,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;

		END;
		IF (@internalTransactionCount = 1)
		BEGIN
			COMMIT TRANSACTION;
		END
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
		INSERT INTO dbo.FireMarshalGenerationLog
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
			@dateInserted,
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
					+ 'of hsp_FireMarshalSendClaims; ErrorMsg: '
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

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION
PRINT 'COMMIT ';COMMIT TRANSACTION

/*
*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Dec  4 2019  2:09PM			*
*****************************************
COMMIT 

*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Dec  4 2019  3:31PM			*
*****************************************
COMMIT 

*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Dec  4 2019  4:22PM			*
*****************************************
COMMIT 

*/