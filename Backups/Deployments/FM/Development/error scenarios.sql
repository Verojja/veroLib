/*
SELECT
	ElementalClaim.claimId,
	COUNT(*)
FROM
	dbo.ElementalClaim
	INNER JOIN dbo.Claim
		ON ElementalClaim.claimId = Claim.claimId
	INNER JOIN dbo.V_ActiveLocationOfLoss
		ON Claim.locationOfLossAddressId = V_ActiveLocationOfLoss.addressId
	INNER JOIN dbo.FireMarshalController
		ON FireMarshalController.fmStateCode = V_ActiveLocationOfLoss.originalStateCode
	INNER JOIN (
		SELECT
			
			SUM(InnerElementalClaim.settlementAmount) AS sumSet,
			SUM(InnerElementalClaim.estimatedLossAmount) AS sumEs
		FROM
			dbo.ElementalClaim AS InnerElementalClaim
		WHERE
			settlementAmount > 0
			OR estimatedLossAmount >0
	) AS ClaimHasMoney
		ON 
WHERE
	ElementalClaim.lossTypeCode = 'FIRE'
	AND FireMarshalController.fmStateStatusCode IN ('P','A')
	AND FireMarshalController.endDate IS NULL
GROUP BY
	ElementalClaim.claimId
HAVING
	COUNT(*) > 1
	
--SELECT * FROM dbo.FireMarshalController
--*/	
/*
1208244	2
339928	2
1298858	2
1423215	2
661242	2
1401389	2
972545	2
1691985	2
1125063	2
1171340	2
342268	2
671321	2
*/
--/*
DECLARE @claimId BIGINT = 1208244;

SELECT
	*
FROM
	dbo.ElementalClaim
WHERE
	ElementalClaim.claimId = @claimId
	
SELECT
	*
FROM
	dbo.FireMarshalExtract
WHERE
	FireMarshalExtract.claimId = @claimId
--*/