
DECLARE
	@dateFilterParam DATETIME2(0) = NULL,
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
			CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
		) AS DATE
	)
FROM
	dbo.AddressActivityLog  WITH (NOLOCK)
WHERE
	AddressActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
	AND AddressActivityLog.isSuccessful = 1;
SET @sourceDateTime = @dateFilterParam;

SELECT
	DuplicateDataSetPerformanceHackMelissaNameMap.addressId,
	CAST(1 AS BIT) AS isLocationOfLoss,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine1,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine2,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalCityName,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalStateCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalZipCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine1,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine2,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCityName,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedStateCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCodeExtended,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyName,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyFIPS,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountryCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.latitude,
	DuplicateDataSetPerformanceHackMelissaNameMap.longitude,
	DuplicateDataSetPerformanceHackMelissaNameMap.geoAccuracy,
	DuplicateDataSetPerformanceHackMelissaNameMap.melissaMappingKey,
	DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId
	--INTO dbo.LocationOfLossDataa
FROM
	(/*Notes on DuplicateDataSetPerformanceHack: dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 contains duplicate records
		performance of rowNumber/partition is noticeably better than using DISTINCT*/
		SELECT
			ExistingAddress.addressId,
			CAST(LTRIM(RTRIM(CLT00001.I_ALLCLM)) AS VARCHAR(11)) AS isoClaimId, 
			ROW_NUMBER() OVER(
				PARTITION BY
					--CLT00001.CLT1AROWID
					CLT00001.ALLCLMROWID
				ORDER BY
					CLT00001.Date_Insert DESC
					/*CS_Lookup_EntityIDs.[YEAR] DESC*/
			) AS uniqueInstanceValue,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR1)),'') AS VARCHAR(50))AS originalAddressLine1,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.T_LOL_STR2)),'') AS VARCHAR(50))AS originalAddressLine2,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.M_LOL_CITY)),'') AS VARCHAR(25))AS originalCityName,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ST_ALPH)),'') AS CHAR(2))AS originalStateCode,
			CAST(NULLIF(LTRIM(RTRIM(CLT00001.C_LOL_ZIP)),'') AS VARCHAR(9))AS originalZipCode,
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
		FROM
			dbo.FireMarshalDriver WITH (NOLOCK)
			--INNER JOIN ClaimSearch_Prod.dbo.CLT0001A  AS CLT00001 WITH (NOLOCK)
			INNER JOIN ClaimSearch_Prod.dbo.CLT00001 WITH (NOLOCK)
				ON FireMarshalDriver.isoClaimId = CLT00001.I_ALLCLM
			LEFT OUTER JOIN dbo.Address AS ExistingAddress WITH (NOLOCK)
				ON ExistingAddress.isoClaimId = CLT00001.I_ALLCLM
			--LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 WITH (NOLOCK)
			--	ON CLT00001.ALLCLMROWID = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.ALLCLMROWID
			--LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
			--	ON CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
		WHERE
			/*Deprecating due to performance costs, and current profile state. RDW 20190328:
				NULLIF(LTRIM(RTRIM(CLT00001.I_ALLCLM)),'') IS NOT NULL
			*/
			CLT00001.Date_Insert >= CAST(
				REPLACE(
					CAST(
						@dateFilterParam
						AS VARCHAR(10)
					),
				'-','')
				AS INT
			)
			--OR CS_Lookup_Unique_Addresses_Melissa_Output.Date_Insert >= CAST(@dateFilterParam AS DATE)
	) AS DuplicateDataSetPerformanceHackMelissaNameMap
WHERE
	DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1;
	
--UPDATE dbo.Address WITH (TABLOCKX)
--	SET
--		Address.originalAddressLine1 = SOURCE.originalAddressLine1,
--		Address.originalAddressLine2 = SOURCE.originalAddressLine2,
--		Address.originalCityName = SOURCE.originalCityName,
--		Address.originalStateCode = SOURCE.originalStateCode,
--		Address.originalZipCode = SOURCE.originalZipCode,
--		Address.scrubbedAddressLine1 = SOURCE.scrubbedAddressLine1,
--		Address.scrubbedAddressLine2 = SOURCE.scrubbedAddressLine2,
--		Address.scrubbedCityName = SOURCE.scrubbedCityName,
--		Address.scrubbedStateCode = SOURCE.scrubbedStateCode,
--		Address.scrubbedZipCode = SOURCE.scrubbedZipCode,
--		Address.scrubbedZipCodeExtended = SOURCE.scrubbedZipCodeExtended,
--		/*Address.scrubbedCountyName = SOURCE.scrubbedCountyName,*/
--		Address.scrubbedCountyFIPS = SOURCE.scrubbedCountyFIPS,
--		Address.scrubbedCountryCode = SOURCE.scrubbedCountryCode,
--		/*Address.longitude = SOURCE.longitude,
--		Address.latitude = SOURCE.latitude,
--		Address.geoAccuracy = SOURCE.geoAccuracy,*/
--		Address.dateInserted = @dateInserted
--FROM
--	dbo.LocationOfLossDataa AS SOURCE
--WHERE
--	SOURCE.addressId IS NOT NULL
--	AND SOURCE.addressId = Address.addressId
--	AND 
--	(
--		ISNULL(Address.originalAddressLine1,'') <> ISNULL(SOURCE.originalAddressLine1,'')
--		OR ISNULL(Address.originalAddressLine2,'') <> ISNULL(SOURCE.originalAddressLine2,'')
--		OR ISNULL(Address.originalCityName,'') <> ISNULL(SOURCE.originalCityName,'')
--		OR ISNULL(Address.originalStateCode,'') <> ISNULL(SOURCE.originalStateCode,'')
--		OR ISNULL(Address.originalZipCode,'') <> ISNULL(SOURCE.originalZipCode,'')
--		OR ISNULL(Address.scrubbedAddressLine1,'') <> ISNULL(SOURCE.scrubbedAddressLine1,'')
--		OR ISNULL(Address.scrubbedAddressLine2,'') <> ISNULL(SOURCE.scrubbedAddressLine2,'')
--		OR ISNULL(Address.scrubbedCityName,'') <> ISNULL(SOURCE.scrubbedCityName,'')
--		OR ISNULL(Address.scrubbedStateCode,'') <> ISNULL(SOURCE.scrubbedStateCode,'')
--		OR ISNULL(Address.scrubbedZipCode,'') <> ISNULL(SOURCE.scrubbedZipCode,'')
--		OR ISNULL(Address.scrubbedZipCodeExtended,'') <> ISNULL(SOURCE.scrubbedZipCodeExtended,'')
--		/*OR ISNULL(Address.scrubbedCountyName,'') <> ISNULL(SOURCE.scrubbedCountyName,'')*/
--		OR ISNULL(Address.scrubbedCountyFIPS,'') <> ISNULL(SOURCE.scrubbedCountyFIPS,'')
--		OR ISNULL(Address.scrubbedCountryCode,'') <> ISNULL(SOURCE.scrubbedCountryCode,'')
--		/*OR ISNULL(Address.longitude,'') <> ISNULL(SOURCE.longitude,'')
--		OR ISNULL(Address.latitude,'') <> ISNULL(SOURCE.latitude,'')
--		OR ISNULL(Address.geoAccuracy,'') <> ISNULL(SOURCE.geoAccuracy,'')*/
--	);

--INSERT INTO dbo.Address WITH (TABLOCKX)
--(
--	isLocationOfLoss,
--	originalAddressLine1,
--	originalAddressLine2,
--	originalCityName,
--	originalStateCode,
--	originalZipCode,
--	scrubbedAddressLine1,
--	scrubbedAddressLine2,
--	scrubbedCityName,
--	scrubbedStateCode,
--	scrubbedZipCode,
--	scrubbedZipCodeExtended,
--	scrubbedCountyName,
--	scrubbedCountyFIPS,
--	scrubbedCountryCode,
--	longitude,
--	latitude,
--	geoAccuracy,
--	isActive,
--	dateInserted,
--	melissaMappingKey,
--	isoClaimId,
--	involvedPartySequenceId
--)
--SELECT
--	SOURCE.isLocationOfLoss,
--	SOURCE.originalAddressLine1,
--	SOURCE.originalAddressLine2,
--	SOURCE.originalCityName,
--	SOURCE.originalStateCode,
--	SOURCE.originalZipCode,
--	SOURCE.scrubbedAddressLine1,
--	SOURCE.scrubbedAddressLine2,
--	SOURCE.scrubbedCityName,
--	SOURCE.scrubbedStateCode,
--	SOURCE.scrubbedZipCode,
--	SOURCE.scrubbedZipCodeExtended,
--	SOURCE.scrubbedCountyName,
--	SOURCE.scrubbedCountyFIPS,
--	SOURCE.scrubbedCountryCode,
--	SOURCE.longitude,
--	SOURCE.latitude,
--	SOURCE.geoAccuracy,
--	1 AS isActive,
--	@dateInserted AS dateInserted,
--	SOURCE.melissaMappingKey,
--	SOURCE.isoClaimId,
--	NULL AS involvedPartySequenceId
--FROM
--	dbo.LocationOfLossDataa AS SOURCE
--WHERE
--	SOURCE.addressId IS NULL;
----OPTION (RECOMPILE);

SELECT
	DuplicateDataSetPerformanceHackMelissaNameMap.addressId,
	CAST(0 AS BIT) AS isLocationOfLoss,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine1,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalAddressLine2,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalCityName,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalStateCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.originalZipCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine1,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedAddressLine2,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCityName,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedStateCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedZipCodeExtended,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyName,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountyFIPS,
	DuplicateDataSetPerformanceHackMelissaNameMap.scrubbedCountryCode,
	DuplicateDataSetPerformanceHackMelissaNameMap.latitude,
	DuplicateDataSetPerformanceHackMelissaNameMap.longitude,
	DuplicateDataSetPerformanceHackMelissaNameMap.geoAccuracy,
	DuplicateDataSetPerformanceHackMelissaNameMap.melissaMappingKey,
	DuplicateDataSetPerformanceHackMelissaNameMap.isoClaimId,
	DuplicateDataSetPerformanceHackMelissaNameMap.involvedPartySequenceId
	--INTO dbo.NonLocationOfLossData
FROM
	(
		SELECT
			ExistingAddress.addressId,
			CAST(LTRIM(RTRIM(CLT00004.I_ALLCLM)) AS VARCHAR(11)) AS isoClaimId,
			CAST(CLT00004.I_NM_ADR AS INT) AS involvedPartySequenceId,
			ROW_NUMBER() OVER(
				PARTITION BY
					CLT00004.I_ALLCLM,
					CLT00004.I_NM_ADR
				ORDER BY
					CLT00004.Date_Insert DESC
					/*CS_Lookup_EntityIDs.[YEAR] DESC*/
			) AS uniqueInstanceValue,
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
		FROM
			dbo.FireMarshalDriver WITH (NOLOCK)
			INNER JOIN ClaimSearch_Prod.dbo.CLT00004 WITH (NOLOCK)
				ON FireMarshalDriver.isoClaimId = CLT00004.I_ALLCLM
			LEFT OUTER JOIN dbo.Address AS ExistingAddress WITH (NOLOCK)
				ON CLT00004.I_ALLCLM  = ExistingAddress.isoClaimId
					AND CLT00004.I_NM_ADR = ExistingAddress.involvedPartySequenceId
			--LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_EntityIDs WITH (NOLOCK)
			--	ON CLT00004.CLMNMROWID = CS_Lookup_EntityIDs.CLMNMROWID
			--LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
			--	ON CS_Lookup_EntityIDs.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
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
			--OR CS_Lookup_Unique_Addresses_Melissa_Output.Date_Insert >= CAST(@dateFilterParam AS DATE)
	) AS DuplicateDataSetPerformanceHackMelissaNameMap
WHERE
	DuplicateDataSetPerformanceHackMelissaNameMap.uniqueInstanceValue = 1;

/*Set Logging Variables for Current Step_Start*/
SELECT
	@stepId = 5,
	@stepDescription = 'UpdateExistingNonLocationOfLossData',
	@stepStartDateTime = GETDATE();

--UPDATE dbo.Address WITH (TABLOCKX)
--	SET
--		Address.originalAddressLine1 = SOURCE.originalAddressLine1,
--		Address.originalAddressLine2 = SOURCE.originalAddressLine2,
--		Address.originalCityName = SOURCE.originalCityName,
--		Address.originalStateCode = SOURCE.originalStateCode,
--		Address.originalZipCode = SOURCE.originalZipCode,
--		Address.scrubbedAddressLine1 = SOURCE.scrubbedAddressLine1,
--		Address.scrubbedAddressLine2 = SOURCE.scrubbedAddressLine2,
--		Address.scrubbedCityName = SOURCE.scrubbedCityName,
--		Address.scrubbedStateCode = SOURCE.scrubbedStateCode,
--		Address.scrubbedZipCode = SOURCE.scrubbedZipCode,
--		Address.scrubbedZipCodeExtended = SOURCE.scrubbedZipCodeExtended,
--		/*Address.scrubbedCountyName = SOURCE.scrubbedCountyName,*/
--		Address.scrubbedCountyFIPS = SOURCE.scrubbedCountyFIPS,
--		Address.scrubbedCountryCode = SOURCE.scrubbedCountryCode,
--		/*Address.longitude = SOURCE.longitude,
--		Address.latitude = SOURCE.latitude,
--		Address.geoAccuracy = SOURCE.geoAccuracy,*/
--		Address.dateInserted = @dateInserted
--FROM
--	dbo.NonLocationOfLossData AS SOURCE
--WHERE
--	SOURCE.addressId IS NOT NULL
--	AND SOURCE.addressId = Address.addressId
--	AND 
--	(
--		ISNULL(Address.originalAddressLine1,'') <> ISNULL(SOURCE.originalAddressLine1,'')
--		OR ISNULL(Address.originalAddressLine2,'') <> ISNULL(SOURCE.originalAddressLine2,'')
--		OR ISNULL(Address.originalCityName,'') <> ISNULL(SOURCE.originalCityName,'')
--		OR ISNULL(Address.originalStateCode,'') <> ISNULL(SOURCE.originalStateCode,'')
--		OR ISNULL(Address.originalZipCode,'') <> ISNULL(SOURCE.originalZipCode,'')
--		OR ISNULL(Address.scrubbedAddressLine1,'') <> ISNULL(SOURCE.scrubbedAddressLine1,'')
--		OR ISNULL(Address.scrubbedAddressLine2,'') <> ISNULL(SOURCE.scrubbedAddressLine2,'')
--		OR ISNULL(Address.scrubbedCityName,'') <> ISNULL(SOURCE.scrubbedCityName,'')
--		OR ISNULL(Address.scrubbedStateCode,'') <> ISNULL(SOURCE.scrubbedStateCode,'')
--		OR ISNULL(Address.scrubbedZipCode,'') <> ISNULL(SOURCE.scrubbedZipCode,'')
--		OR ISNULL(Address.scrubbedZipCodeExtended,'') <> ISNULL(SOURCE.scrubbedZipCodeExtended,'')
--		/*OR ISNULL(Address.scrubbedCountyName,'') <> ISNULL(SOURCE.scrubbedCountyName,'')*/
--		OR ISNULL(Address.scrubbedCountyFIPS,'') <> ISNULL(SOURCE.scrubbedCountyFIPS,'')
--		OR ISNULL(Address.scrubbedCountryCode,'') <> ISNULL(SOURCE.scrubbedCountryCode,'')
--		/*OR ISNULL(Address.longitude,'') <> ISNULL(SOURCE.longitude,'')
--		OR ISNULL(Address.latitude,'') <> ISNULL(SOURCE.latitude,'')
--		OR ISNULL(Address.geoAccuracy,'') <> ISNULL(SOURCE.geoAccuracy,'')*/
--	);

--INSERT INTO dbo.Address WITH (TABLOCKX)
--(
--	isLocationOfLoss,
--	originalAddressLine1,
--	originalAddressLine2,
--	originalCityName,
--	originalStateCode,
--	originalZipCode,
--	scrubbedAddressLine1,
--	scrubbedAddressLine2,
--	scrubbedCityName,
--	scrubbedStateCode,
--	scrubbedZipCode,
--	scrubbedZipCodeExtended,
--	scrubbedCountyName,
--	scrubbedCountyFIPS,
--	scrubbedCountryCode,
--	longitude,
--	latitude,
--	geoAccuracy,
--	isActive,
--	dateInserted,
--	melissaMappingKey,
--	isoClaimId,
--	involvedPartySequenceId
--)
--SELECT
--	SOURCE.isLocationOfLoss,
--	SOURCE.originalAddressLine1,
--	SOURCE.originalAddressLine2,
--	SOURCE.originalCityName,
--	SOURCE.originalStateCode,
--	SOURCE.originalZipCode,
--	SOURCE.scrubbedAddressLine1,
--	SOURCE.scrubbedAddressLine2,
--	SOURCE.scrubbedCityName,
--	SOURCE.scrubbedStateCode,
--	SOURCE.scrubbedZipCode,
--	SOURCE.scrubbedZipCodeExtended,
--	SOURCE.scrubbedCountyName,
--	SOURCE.scrubbedCountyFIPS,
--	SOURCE.scrubbedCountryCode,
--	SOURCE.longitude,
--	SOURCE.latitude,
--	SOURCE.geoAccuracy,
--	1 AS isActive,
--	@dateInserted AS dateInserted,
--	SOURCE.melissaMappingKey,
--	SOURCE.isoClaimId,
--	SOURCE.involvedPartySequenceId
--FROM
--	dbo.NonLocationOfLossData AS SOURCE
--WHERE
--	SOURCE.addressId IS NULL;
--OPTION (RECOMPILE);
