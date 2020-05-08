SET NOEXEC OFF;
/*
TODO:
	Update naming convention
	Deprecate materialized views
	Deprecate AddressActivityLog
*/

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
	involvedPartySequenceId INT NULL /*I_NM_ADR*/
	/*DevNote: Deprecating Indexs/Constraints for SNOWFLAKE implimentation*//*,CONSTRAINT PK_Address_addressId
		PRIMARY KEY CLUSTERED (addressId)*/
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
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMLocationOfLoss
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
		INNER JOIN dbo.FM_ExtractFile
			ON Address.isoClaimId = FM_ExtractFile.I_ALLCLM
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
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMLocationOfLoss_addressId
	ON dbo.V_ActiveFMLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80);
CREATE NONCLUSTERED INDEX NIX_ActiveFMLocationOfLoss_melissaMappingKey_isoClaimId
	ON dbo.V_ActiveFMLocationOfLoss (melissaMappingKey, isoClaimId);	
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveFMNonLocationOfLoss
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
		INNER JOIN dbo.FM_ExtractFile
			ON Address.isoClaimId = FM_ExtractFile.I_ALLCLM
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
CREATE UNIQUE CLUSTERED INDEX PK_ActiveFMNonLocationOfLoss_addressId
	ON dbo.V_ActiveFMNonLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80)
CREATE NONCLUSTERED INDEX NIX_ActiveFMNonLocationOfLoss_melissaMappingKey_isoClaimId
	ON dbo.V_ActiveFMNonLocationOfLoss (melissaMappingKey, isoClaimId, involvedPartySequenceId);
CREATE NONCLUSTERED INDEX NIX_ActiveFMNonLocationOfLoss_isoClaimId
	ON dbo.V_ActiveFMNonLocationOfLoss (isoClaimId, involvedPartySequenceId);
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
	stepExecutionNotes VARCHAR(1000) NULL,
	CONSTRAINT PK_AddressActivityLog_addressActivityLogId
		PRIMARY KEY CLUSTERED (addressActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_AddressActivityLog_executionDateTime
	ON dbo.AddressActivityLog (executionDateTime)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--EXEC sp_help 'dbo.Address'
--EXEC sp_help 'dbo.V_ActiveFMLocationOfLoss'
--EXEC sp_help 'dbo.V_ActiveFMNonLocationOfLoss'
--EXEC sp_help 'dbo.AddressActivityLog'

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;

/*
COMMIT
20190114 : 5:24 PM
*/