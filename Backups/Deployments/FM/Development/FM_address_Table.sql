SET NOEXEC OFF;
/*
	todo:
		analyze which county to import
*/
--USE ClaimSearch_Dev

USE ClaimSearchSB_01

BEGIN TRANSACTION

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ??????
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
	longitude VARCHAR(15) NULL,
	latitude VARCHAR(15) NULL,
	/*July2011 SpatialFeature WhitePapers SQLServer2012 highlight several performance
		improvements; 5-30x performance depending on operation.
		Possibly Deprecate until using 2012.
	--*/
	/*Deprecating:
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
	geoAccuracy VARCHAR(15) NOT NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	melissaMappingKey BIGINT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId TINYINT NULL, /*I_NM_ADR*/
	CONSTRAINT Address_addressId
		PRIMARY KEY CLUSTERED (addressId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
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
CREATE UNIQUE CLUSTERED INDEX PK_V_ActiveLocationOfLoss_addressId
	ON dbo.V_ActiveLocationOfLoss (addressId);
CREATE NONCLUSTERED INDEX NIX_V_ActiveLocationOfLoss_melissaMappingKey_isoClaimId
	ON dbo.V_ActiveLocationOfLoss (melissaMappingKey, isoClaimId)
		INCLUDE (addressId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveNonLOLAddress
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
		Address.isActive = 0
		AND Address.isLocationOfLoss = 0
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_V_ActiveNonLOLAddress_addressId
	ON dbo.V_ActiveNonLOLAddress (addressId)
CREATE NONCLUSTERED INDEX NIX_V_ActiveNonLOLAddress_melissaMappingKey_isoClaimId
	ON dbo.V_ActiveNonLOLAddress (melissaMappingKey, isoClaimId)
		INCLUDE (addressId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE TABLE dbo.AddressActivityLog
(
	addressActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
	productCode VARCHAR(50) NULL,
	sourceDateTime DATETIME2(0) NOT NULL,
	executionDateTime DATETIME2(0) NOT NULL,
	stepId TINYINT NOT NULL,
	stepDescription VARCHAR(1000) NULL,
	stepStartDateTime DATETIME2(0) NULL,
	stepEndDateTime DATETIME2(0) NULL,
	executionDurationInSeconds AS DATEDIFF(SECOND,stepStartDateTime,stepEndDateTime),
	recordsAffected BIGINT NULL,
	isSuccessful BIT NOT NULL,
	stepExecutionNotes VARCHAR(1000) NULL
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
EXEC sp_help 'dbo.Address'
PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
--PRINT 'COMMIT'; COMMIT TRANSACTION;

/*

*/