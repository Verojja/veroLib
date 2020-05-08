
SELECT
	*
FROM	
	dbo.FMPendingClaimActivityLog
	
	
SELECT NULL AS 'FireMarshalPendingClaim', FireMarshalPendingClaim.isActive, FireMarshalPendingClaim.isCurrent, FireMarshalPendingClaim.dateInserted,
FireMarshalPendingClaim.coverageTypeCode,
FireMarshalPendingClaim.estimatedLossAmount,
 FireMarshalPendingClaim.*
FROM
	dbo.FireMarshalPendingClaim
	LEFT OUTER JOIN dbo.ElementalClaim
		ON FireMarshalPendingClaim.elementalClaimId = ElementalClaim.elementalClaimId 
WHERE
	ElementalClaim.elementalClaimId IS NULL;

SELECT
	COUNT(*)
FROM
	dbo.FireMarshalPendingClaim
	LEFT OUTER JOIN dbo.ElementalClaim
		ON FireMarshalPendingClaim.elementalClaimId = ElementalClaim.elementalClaimId 
WHERE
	ElementalClaim.elementalClaimId IS NULL;


SELECT
	COUNT(*)
FROM
	dbo.FireMarshalPendingClaim
468,743	/  338,311

2,012,490

SELECT COUNT(*) FROM dbo.ElementalClaim