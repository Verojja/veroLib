SET NOEXEC OFF;
BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ?????
Date: 2019-01-08
Author: Robert David Warner && Dan Ravaglia
Description: Mechanism for data-refresh of the Address Table.
			
			Performance:
************************************************/
CREATE PROCEDURE dbo.hsp_UpdateInsertAddress
	@dateFilterParam DATETIME2(0) = NULL
AS
BEGIN
	BEGIN TRANSACTION
		BEGIN TRY
			DECLARE
				@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
				@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
				@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
				@executionDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
					
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
						CAST('2013-08-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
					) AS DATE
				),
				@sourceDateTime = COALESCE
					(
						MAX(AddressActivityLog.executionDateTime),/*use the last successful ExecutionDateTime*/
						GETDATE()/*if the log table is empty (IE: first run), use currentDate*/
					),
				@executionDateTime = GETDATE() /*executionDateTime is always currentDate*/
			FROM
				dbo.AddressActivityLog
			WHERE
				stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
				AND isSuccessful = 1;

			/*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 1,
				@stepDescription = 'CaptureLocationOfLossDataToImport',
				@stepStartDateTime = GETDATE();
			
			SELECT
				ExistingAddress.addressId,
				CAST(1 AS BIT) AS isLocationOfLoss,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.ADR_LN1_V)),'') AS VARCHAR(50))AS originalAddressLine1,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.ADR_LN2_V)),'') AS VARCHAR(50))AS originalAddressLine2,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.CITY_V)),'') AS VARCHAR(25))AS originalCityName,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.[State])),'') AS CHAR(2))AS originalStateCode,
				CAST(NULLIF(LTRIM(RTRIM(CS_Lookup_Unique_Addresses_Melissa_Output.ZIP_V)),'') AS VARCHAR(9))AS originalZipCode,
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
				CAST(CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey AS BIGINT) AS melissaMappingKey,
				CAST(CLT00001.I_ALLCLM AS VARCHAR(11)) AS isoClaimId,
				INTO #LocationOfLossData
			FROM
				[ClaimSearch].dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 WITH (NOLOCK)
				INNER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
					ON CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey
				INNER JOIN [ClaimSearch_Prod].dbo.CLT00001
					ON CLT00001.ALLCLMROWID = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.ALLCLMROWID
				LEFT OUTER JOIN [ClaimSearch_Prod].dbo.V_ActiveLocationOfLoss AS ExistingAddress WITH (NOLOCK)
					ON ExistingAddress.melissaMappingKey = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey
			WHERE
				CS_Lookup_Unique_Addresses_Melissa_Output.Date_Insert >= @dateFilterParam
				AND NULLIF(LTRIM(RTRIM(CLT00001.I_ALLCLM)),'') IS NOT NULL
				AND CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey IS NOT NULL;

			
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
				executionNotes
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
					SOURCE.isLocationOfLoss = Address.isLocationOfLoss,
					SOURCE.involvedPartyId = Address.involvedPartyId,
					SOURCE.insuranceClaimId = Address.insuranceClaimId,
					SOURCE.originalAddressLine1 = Address.originalAddressLine1,
					SOURCE.originalAddressLine2 = Address.originalAddressLine2,
					SOURCE.originalCityName = Address.originalCityName,
					SOURCE.originalStateCode = Address.originalStateCode,
					SOURCE.originalZipCode = Address.originalZipCode,
					SOURCE.scrubbedAddressLine1 = Address.scrubbedAddressLine1,
					SOURCE.scrubbedAddressLine2 = Address.scrubbedAddressLine2,
					SOURCE.scrubbedCityName = Address.scrubbedCityName,
					SOURCE.scrubbedStateCode = Address.scrubbedStateCode,
					SOURCE.scrubbedZipCode = Address.scrubbedZipCode,
					SOURCE.scrubbedZipCodeExtended = Address.scrubbedZipCodeExtended,
					SOURCE.scrubbedCountyName = Address.scrubbedCountyName,
					SOURCE.scrubbedCountyFIPS = Address.scrubbedCountyFIPS,
					SOURCE.scrubbedCountryCode = Address.scrubbedCountryCode,
					SOURCE.longitude = Address.longitude,
					SOURCE.latitude = Address.latitude,
					SOURCE.geoAccuracy = Address.geoAccuracy,
					@dateInserted = Address.dateInserted
			FROM
				#LocationOfLossData AS SOURCE
				INNER JOIN dbo.V_ActiveLocationOfLoss
					ON V_ActiveLocationOfLoss.melissaMappingKey = SOURCE.melissaMappingKey
						AND V_ActiveLocationOfLoss.isoClaimId = SOURCE.isoClaimId

			WHERE
				V_ActiveLocationOfLoss.addressId = Address.addressId
				AND 
				(
					ISNULL(ClaimReference.iSSSNCode,'') <> ISNULL(SOURCE.iSSSNCode,'')
					OR ISNULL(ClaimReference.companySoldToCode,'') <> ISNULL(SOURCE.companySoldToCode,'')
					OR ISNULL(ClaimReference.officeSoldToCode,'') <> ISNULL(SOURCE.officeSoldToCode,'')
					OR ISNULL(ClaimReference.companyShippedToCode,'') <> ISNULL(SOURCE.companyShippedToCode,'')
					OR ISNULL(ClaimReference.officeShippedToCode,'') <> ISNULL(SOURCE.officeShippedToCode,'')
					OR ISNULL(ClaimReference.vendorId,'') <> ISNULL(SOURCE.vendorId,'')
					OR ISNULL(ClaimReference.rsltVendCode,'') <> ISNULL(SOURCE.rsltVendCode,'')
					OR ISNULL(ClaimReference.orderStatus,'') <> ISNULL(SOURCE.orderStatus,'')
					OR ISNULL(ClaimReference.reportType,'') <> ISNULL(SOURCE.reportType,'')
					OR ISNULL(ClaimReference.productTransactionCode,'') <> ISNULL(SOURCE.productTransactionCode,'')
					OR ISNULL(ClaimReference.VendorTransactionID,'') <> ISNULL(SOURCE.VendorTransactionID,'')
					OR ISNULL(ClaimReference.isMatched,'') <> ISNULL(SOURCE.isMatched,'')
					OR ISNULL(ClaimReference.isBilled,'') <> ISNULL(SOURCE.isBilled,'')
					OR ISNULL(ClaimReference.mtroStatusCode,'') <> ISNULL(SOURCE.mtroStatusCode,'')
					OR ISNULL(ClaimReference.vendorTransactionCode,'') <> ISNULL(SOURCE.vendorTransactionCode,'')
					OR ISNULL(ClaimReference.additionalCharge,-.01) <> ISNULL(SOURCE.additionalCharge,-.01)
					OR ISNULL(ClaimReference.otherProductCode,'') <> ISNULL(SOURCE.otherProductCode,'')
					OR ISNULL(ClaimReference.dateBilled,'') <> ISNULL(SOURCE.dateBilled,'')
					OR ISNULL(ClaimReference.dateBilledRun,'') <> ISNULL(SOURCE.dateBilledRun,'')
					OR ISNULL(ClaimReference.dateDeleted,'') <> ISNULL(SOURCE.dateDeleted,'')
					OR ISNULL(ClaimReference.dateSearched,'') <> ISNULL(SOURCE.dateSearched,'')
					OR ISNULL(ClaimReference.dateFilled,'') <> ISNULL(SOURCE.dateFilled,'')
				)
			--OPTION (RECOMPILE);


			COMMIT TRANSACTION;
		END TRY
		BEGIN CATCH
			/*Set Logging Variables for Current Step_End_Fail*/
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
				executionNotes
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
			ROLLBACK TRANSACTION;
			
			/*Optional: We can bubble the error up to the calling level.*/
			DECLARE @RAISEERROR_message VARCHAR(2045) = /*Constructs an intuative error message*/
				'Error: in Step'
				+ CAST(@stepId AS VARCHAR(3))
				+ ' ('
				+ @stepDescription
				+ ') '
				+ 'of hsp_UpdateInsertAddress; ErrorMsg: '
				+ ERROR_MESSAGE();
			RAISERROR(@RAISEERROR_message,-1,-1);
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

*/