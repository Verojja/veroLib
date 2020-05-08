SELECT
	*
FROM
	[ClaimSearch_Dev].[dbo].[FM_DriverDescrepencies]
	INNER JOIN dbo.FireMarshalDriver
		ON FireMarshalDriver.isoClaimId = [FM_DriverDescrepencies].I_ALLCLM
	INNER JOIN ClaimSearch_Prod.dbo.CLT00001
		ON CLT00001.I_ALLCLM = [FM_DriverDescrepencies].I_ALLCLM
WHERE
	compare <> 'included';

SELECT
	*
FROM
	[ClaimSearch_Dev].[dbo].[FM_DriverDescrepencies]
	INNER JOIN ClaimSearch_Prod.dbo.CLT00001
		ON CLT00001.I_ALLCLM = [FM_DriverDescrepencies].I_ALLCLM
WHERE
	FM_DriverDescrepencies.compare <> 'included'
	AND
	(
		CLT00001.D_RCV >= '20140101'
		OR CLT00001.D_RCV IS NULL
	);
	

SELECT
	FM_DriverDescrepencies.I_ALLCLM,
	FM_DriverDescrepencies.compare,
	
	CLT00001.D_RCV,
	CLT00001.D_OCUR,
	CLT00001.Date_Insert,
	CLT00001.*
FROM
	[ClaimSearch_Dev].[dbo].[FM_DriverDescrepencies]
	INNER JOIN ClaimSearch_Prod.dbo.CLT00001
		ON CLT00001.I_ALLCLM = [FM_DriverDescrepencies].I_ALLCLM
WHERE
	FM_DriverDescrepencies.compare <> 'included'
	--AND
	--(
	--	CLT00001.D_RCV >= '20140101'
	--	OR CLT00001.D_RCV IS NULL
	--);
	
/*	
	'dbo.FM_DriverDescrepencies'
*/