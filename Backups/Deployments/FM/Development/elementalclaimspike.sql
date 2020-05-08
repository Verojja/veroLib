/*
claim (claim-metadata different insert/update frequency but too small and no time)


Claim  N : 1 with policy
Policy N : 1 with IP
IP N : N with address
Claim  N : N Address
elementary claim N:1 with claim
*/

--DECLARE @iallClm VARCHAR(11) = '6H004913783' 
--DECLARE @nmAdr TINYINT = 2
--DECLARE @lossTypeCode VARCHAR(10) = 'OTAU'



DECLARE @iallClm VARCHAR(11) = '5M004920996' 
DECLARE @nmAdr TINYINT = 1
DECLARE @lossTypeCode VARCHAR(10) = NULL;
DECLARE @lobCode VARCHAR(10) = 'A'
		
		
SELECT
	*
FROM
	dbo.CLT00014
WHERE
	I_ALLCLM = @iallClm
	AND I_NM_ADR = @nmAdr
	
SELECT
	*
FROM
	dbo.CLT00002
WHERE
	CLT00002.I_ALLCLM = @iallClm
	
/*	


lossTypeCode
CovTypeCode
lineOfBusinessCode
AdjusterId
amounts
vin
coverageMapCode	



'5U004921579'
1

3 diff adj, 2 the same
4 dif los type, which matched coverageTypeCode
2 different lineOfBusinessCode, 2 same
2 populated vins, 2 same
3 dif coverageMapCodes 2 same

1T004919608	1

2 diff adj, 3 the same
4 dif los type, which DID NOT matched coverageTypeCode
2 different lineOfBusinessCode,  same
2 populated vins, 2 same
3 dif coverageMapCodes 2 same


I_ALLCLM	I_NM_ADR	C_LOSS_TYP
6H004913783	2	OTAU


*/