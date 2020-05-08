
BEGIN TRANSACTION

	EXEC dbo.hsp_UpdateInsertFMPendingClaim
	--@dateFilterParam
	@dailyLoadOverride = 1

--BEGIN TRANSACTION

	EXEC dbo.hsp_UpdateInsertFMHistoricClaim
	--@dateFilterParam
	@dailyLoadOverride = 1

BEGIN TRANSACTION

	EXEC dbo.hsp_UpdateInsertFireMarshalExtract
	--@dateFilterParam = 'sourceDateTime2019- 07-01 00:00:00'
	@dailyLoadOverride = 1

ROLLBACK TRANSACTION
SELECT @@TRANCOUNT

SELECT * FROM dbo.FMPendingClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC
SELECT * FROM dbo.FMClaimSendHistoryActivityLog WITH(NOLOCK) ORDER BY 1 DESC
SELECT * FROM dbo.FireMarshalExtractActivityLog WITH(NOLOCK) ORDER BY 1 DESC

--SELECT @@TRANCOUNT

--'dbo.FireMarshalPendingClaim'

--'dbo.FireMarshalExtract'

SELECT TOP 300
	*
FROM
	dbo.FireMarshalPendingClaim
	LEFT OUTER JOIN dbo.FireMarshalClaimSendHistory AS SentClaims
		ON FireMarshalPendingClaim.elementalClaimId = SentClaims.elementalClaimId
		AND SentClaims.reportingStatus = 'Sent'
WHERE
	SentClaims.elementalClaimId IS NULL
	AND FireMarshalPendingClaim.lossStateCode = 'KS'
	AND FireMarshalPendingClaim.isCurrent = 1
	AND FireMarshalPendingClaim.claimIsOpen = 0
	AND FireMarshalPendingClaim.reportingStatus = 'Exception'

/*
	178737	3	549102	1K004052156	Exception	Estimated and/or Settlement Amount Missing
	455984	3	1519481	7R004678582	Exception	Loss Description Invalid and Estimated and/or Settlement Amount Missing
	304338	3	679468	2D004543575	Exception	Loss Description Invalid	2019-07-01	0	2018-03-13	500553727311	0163876738	10237	Z995	INSURANCE SERVICES OFFICE, INC (Z995)	Z995	INSURANCE SERVICES OFFICE, INC (Z995)	Z995	INSURANCE SERVICES OFFICE, INC (Z995)	Z995	INSURANCE SERVICES OFFICE, INC (Z995)	526 NE BURGESS ST	526 NE BURGESS ST	TOPEKA	KS	KANSAS	66608	NULL	NULL	NULL	FIRE	NULL	2018-03-12	FIRE	Fire	PAPP	Personal Automobile	COMP	Comprehensive	0.00	6789.00	0.00	0.00	0.00	0.00	0	1			NULL	NULL	1641370	STOCKMAN,PHILLIP	416118	1	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0	1	2019-07-01 16:02:11	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL
*/	
SELECT
	*
FROM	
	dbo.FireMarshalClaimSendHistory
SELECT
	*
FROM	
	dbo.FireMarshalExtract
	
--USE ClaimSearch_Dev
SELECT
	FireMarshalPendingClaim.isActive,
	FireMarshalPendingClaim.isCurrent,
	FireMarshalPendingClaim.claimIsOpen,
	FireMarshalPendingClaim.dateInserted,
	*
FROM
	dbo.FireMarshalPendingClaim
WHERE
	FireMarshalPendingClaim.elementalClaimId IN
	(
		178737,
		455984,
		304338
	)
	
SELECT
	FireMarshalClaimSendHistory.isActive,
	FireMarshalClaimSendHistory.claimIsOpen,
	FireMarshalClaimSendHistory.dateInserted,
	*
FROM	
	dbo.FireMarshalClaimSendHistory
WHERE
	FireMarshalClaimSendHistory.elementalClaimId IN
	(
		178737,
		455984,
		304338
	)	
SELECT
	FireMarshalExtract.isActive,
	FireMarshalExtract.isCurrent,
	FireMarshalExtract.claimIsOpen,
	FireMarshalExtract.dateInserted,
	*
FROM	
	dbo.FireMarshalExtract
WHERE
	FireMarshalExtract.elementalClaimId IN
	(
		178737,
		455984,
		304338
	)
	
	
	
	
	
/*
----USE CLAIMSEARCH_DEV
--SELECT * FROM dbo.FMPendingClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC
--SELECT * FROM dbo.FMClaimSendHistoryActivityLog WITH(NOLOCK) ORDER BY 1 DESC
--SELECT * FROM dbo.FireMarshalExtractActivityLog WITH(NOLOCK) ORDER BY 1 DESC

--USE ClaimSearch_Prod
----USE ClaimSearch_DEV
DECLARE @IALLCLMN VARCHAR(11) = '1J004975504'
SELECT
	NULL AS 'Dev',
	*
FROM
	dbo.ElementalClaim
WHERE
ElementalClaim.isoClaimId = @IALLCLMN

SELECT
	NULL AS 'Dev',
	*
FROM
	dbo.FireMarshalPendingClaim
WHERE
FireMarshalPendingClaim.isoFileNumber =@IALLCLMN


SELECT
	NULL AS 'prod',
	*
FROM
	ClaimSearch_Prod.dbo.ElementalClaim
WHERE
ElementalClaim.isoClaimId = @IALLCLMN

SELECT
	NULL AS 'prod',
	*
FROM
	ClaimSearch_Prod.dbo.FireMarshalPendingClaim
WHERE
FireMarshalPendingClaim.isoFileNumber =@IALLCLMN

--6G004033773
--4H004935357

*/