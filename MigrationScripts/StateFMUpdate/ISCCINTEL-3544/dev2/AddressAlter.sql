BEGIN TRANSACTION;
--DROP VIEW dbo.V_ActiveLocationOfLoss
--DROP VIEW dbo.V_ActiveNonLocationOfLoss
--DROP TABLE dbo.Address

/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Generic Address object
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 20200213
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			NOTE: Sanitized\Scrubbed-Address-data for NON-LocationOfLoss is currently not being imported\updated due to performance reasons.
			
			Performance: Significant performance improvements on one-query(with-date-calc)
							over cross-db-query-with-index-added (40 sec vs. 30 min in worst case).
************************************************/
CREATE TABLE dbo.Address
(
	addressId BIGINT IDENTITY (1,1) NOT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
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
	melissaMappingKey BIGINT NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
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
	INCLUDE (involvedPartySequenceId, isLocationOfLoss, originalAddressLine1,
		originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1,
		scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended,
		scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude, longitude,
		geoAccuracy, melissaMappingKey, isActive, dateInserted, deltaDate
	);
	GO
CREATE NONCLUSTERED INDEX NIX_Address_isoClaimId_involvedPartySequenceId
	ON dbo.Address (isoClaimId, involvedPartySequenceId)
	INCLUDE (isLocationOfLoss, originalAddressLine1,
		originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1,
		scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended,
		scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude, longitude,
		geoAccuracy, melissaMappingKey, isActive, dateInserted, deltaDate
	);
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
***********************************************
WorkItem: ISCCINTEL-3544
Date: 20200213
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		addressId,
		isoClaimId,
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
		dateInserted,
		deltaDate
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
CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_isoClaimId
	ON dbo.V_ActiveLocationOfLoss (isoClaimId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode,
		scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode,
		scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude,
		longitude, geoAccuracy, melissaMappingKey, dateInserted, deltaDate
	);
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
***********************************************
WorkItem: ISCCINTEL-3544
Date: 20200213
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveNonLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		addressId,
		isoClaimId,
		involvedPartySequenceId,
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
		dateInserted,
		deltaDate
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
CREATE NONCLUSTERED INDEX NIX_ActiveNonLocationOfLos_isoClaimId
	ON dbo.V_ActiveNonLocationOfLoss (isoClaimId, involvedPartySequenceId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode,
		scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode,
		scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude,
		longitude, geoAccuracy, melissaMappingKey, dateInserted, deltaDate
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
--PRINT 'COMMIT'; COMMIT TRANSACTION;

/*
COMMIT
20190114 : 5:24 PM
*/