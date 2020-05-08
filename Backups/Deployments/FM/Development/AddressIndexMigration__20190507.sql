SET NOEXEC OFF;

BEGIN TRANSACTION


EXEC sp_help 'dbo.V_ActiveNonLocationOfLoss';
EXEC sp_help 'dbo.V_ActiveLocationOfLoss';
/*AddressIndexUpdate*/
DROP INDEX NIX_ActiveNonLocationOfLos_melissaMappingKey
	ON dbo.V_ActiveNonLocationOfLoss
DROP INDEX NIX_ActiveLocationOfLoss_melissaMappingKey
	ON dbo.V_ActiveLocationOfLoss
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_originalStateCode
	ON dbo.V_ActiveLocationOfLoss (originalStateCode)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
CREATE NONCLUSTERED INDEX NIX_ActiveNonLocationOfLoss_originalStateCode
	ON dbo.V_ActiveNonLocationOfLoss (originalStateCode)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalZipCode, scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, longitude, latitude, geoAccuracy, dateInserted, isoClaimId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

EXEC sp_help 'dbo.V_ActiveNonLocationOfLoss';
EXEC sp_help 'dbo.V_ActiveLocationOfLoss';
ROLLBACK TRANSACTION