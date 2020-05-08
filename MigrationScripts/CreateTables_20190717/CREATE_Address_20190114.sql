SET NOEXEC OFF;
/*
	todo:
		analyze which county to import
*/
USE ClaimSearch_Dev

BEGIN TRANSACTION

--/*
	--/*
	DROP VIEW dbo.V_ActiveLocationOfLoss
	DROP VIEW dbo.V_ActiveNonLocationOfLoss
	--*/
	--DROP TABLE dbo.AddressActivityLog
	DROP TABLE dbo.Address
--*/

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Generic Address object
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Address
(
	addressId BIGINT IDENTITY (1,1) NOT NULL,
	isLocationOfLoss BIT NOT NULL,

	originalAddressLine1 /*T_ADR_LN1*/ VARCHAR(50) NULL,
	originalAddressLine2 /*T_ADR_LN2*/ VARCHAR(50) NULL,
	originalCityName /*M_CITY*/ VARCHAR(25) NULL,
	originalStateCode /*C_ST_ALPH*/ CHAR(2) NULL,
	originalZipCode /*C_ZIP*/ VARCHAR(9) NULL,
	
	scrubbedAddressLine1 /*T_ADR_LN1*/ VARCHAR(50) NULL,
	scrubbedAddressLine2 /*T_ADR_LN2*/ VARCHAR(50) NULL,
	scrubbedCityName /*M_CITY*/ VARCHAR(25) NULL,
	scrubbedStateCode /*C_ST_ALPH*/ CHAR(2) NULL,
	scrubbedZipCode /*C_ZIP*/ CHAR(5) NULL,
	scrubbedZipCodeExtended /*C_ZIP*/ CHAR(4) NULL,
	scrubbedCountyName /*C_CNTRY*/ VARCHAR(25) NULL,
	scrubbedCountyFIPS CHAR(5) NULL,
	scrubbedCountryCode /*C_CNTRY*/ VARCHAR(3) NULL, 
	latitude VARCHAR(15) NULL,
	longitude VARCHAR(15) NULL,
	/*DevNote: Deprecating; July2011 SpatialFeature WhitePapers SQLServer2012 highlight several performance
		improvements; 5-30x performance depending on operation.
		Possibly Deprecate until local isntance using 2012.
	geolocation AS geography::STPointFromText
	(
		'POINT('
		+ longitude
		+ ' '
		+ latitude
		+ ')',
		 4326
	),
	--*/
	geoAccuracy VARCHAR(15) NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	melissaMappingKey BIGINT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	CONSTRAINT PK_Address_addressId
		PRIMARY KEY CLUSTERED (addressId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_Address_isoClaimId
	ON dbo.Address (isoClaimId)
	INCLUDE (isLocationOfLoss, originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, melissaMappingKey);
	GO
CREATE NONCLUSTERED INDEX NIX_Address_isoClaimId_involvedPartySequenceId
	ON dbo.Address (isoClaimId, involvedPartySequenceId)
	INCLUDE (isLocationOfLoss, originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, melissaMappingKey);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW representing addresses that correlate
				to the covered-incident of insurance claims.
************************************************/
CREATE VIEW dbo.V_ActiveLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		addressId,
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
		/*geolocation,*/
		geoAccuracy,
		dateInserted,
		melissaMappingKey,
		isoClaimId
	FROM
		dbo.Address
	WHERE
		Address.isActive = 1
		AND Address.isLocationOfLoss = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveLocationOfLoss_addressId
	ON dbo.V_ActiveLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_melissaMappingKey
	ON dbo.V_ActiveLocationOfLoss (melissaMappingKey)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
*/
CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_isoClaimId
	ON dbo.V_ActiveLocationOfLoss (isoClaimId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, melissaMappingKey);
/*CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_originalStateCode
	ON dbo.V_ActiveLocationOfLoss (originalStateCode)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW representing addresses that correlate
				to the mailing address for the Insured at the time of a claim.
************************************************/
CREATE VIEW dbo.V_ActiveNonLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		addressId,
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
		/*geolocation,*/
		geoAccuracy,
		dateInserted,
		melissaMappingKey,
		isoClaimId,
		involvedPartySequenceId
	FROM
		dbo.Address
	WHERE
		Address.isActive = 1
		AND Address.isLocationOfLoss = 0
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveNonLocationOfLos_addressId
	ON dbo.V_ActiveNonLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*CREATE NONCLUSTERED INDEX NIX_ActiveNonLocationOfLos_melissaMappingKey
	ON dbo.V_ActiveNonLocationOfLoss (melissaMappingKey)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
*/
CREATE NONCLUSTERED INDEX NIX_ActiveNonLocationOfLos_isoClaimId
	ON dbo.V_ActiveNonLocationOfLoss (isoClaimId, involvedPartySequenceId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, melissaMappingKey);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--/***********************************************
--WorkItem: ISCCINTEL-2316
--Date: 2019-01-07
--Author: Robert David Warner
--Description: Logging table for the Address object
--************************************************/
--CREATE TABLE dbo.AddressActivityLog
--(
--	addressActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
--	productCode VARCHAR(50) NULL,
--	sourceDateTime DATETIME2(0) NOT NULL,
--	executionDateTime DATETIME2(0) NOT NULL,
--	stepId TINYINT NOT NULL,
--	stepDescription VARCHAR(1000) NULL,
--	stepStartDateTime DATETIME2(0) NULL,
--	stepEndDateTime DATETIME2(0) NULL,
--	executionDurationInSeconds AS DATEDIFF(SECOND,stepStartDateTime,stepEndDateTime),
--	recordsAffected BIGINT NULL,
--	isSuccessful BIT NOT NULL,
--	stepExecutionNotes VARCHAR(1000) NULL,
--	CONSTRAINT PK_AddressActivityLog_addressActivityLogId
--		PRIMARY KEY CLUSTERED (addressActivityLogId)
--);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--CREATE NONCLUSTERED INDEX NIX_AddressActivityLog_isSuccessful_stepId_executionDateTime
--	ON dbo.AddressActivityLog (isSuccessful, stepId, executionDateTime);
--GO
--IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
--BEGIN
--	ROLLBACK TRANSACTION;
--	SET NOEXEC ON;
--END
--GO
--EXEC sp_help 'dbo.Address'
--EXEC sp_help 'dbo.V_ActiveNonLocationOfLoss'
--EXEC sp_help 'dbo.V_ActiveLocationOfLoss'
--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*
COMMIT
20190114 : 5:24 PM
*/