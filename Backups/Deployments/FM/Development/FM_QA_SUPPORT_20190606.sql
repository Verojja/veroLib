/*
	USE CLAIMSEARCH_Dev
	USE CLAIMSEARCH_PROD
	6C004959424 - closed vs. open
	4Z004958743 or 7F004969735
	5O004981156
	6C004959424

	-	3T004966462 – This is GA Fire claim with multiple coverages one coverage with Amount and Other coverage does not have amounts. We are expecting to see this claim on the dashboard with the exception as ‘Settlement Amount Missing’. But we are not seeing this claim on the dashboard
-	0F004958890 – This is KS Fire claim with multiple coverages one coverage with Amount and Other coverage does not have amounts. We are expecting to see this claim on the Details tab with Pending status because the Loss Description is ‘Fire Test’, but we are not seeing this claim on the dashboard
-	Login with Internal user and go to the Details tab – We are not seeing any claims with ‘SENT’ status for any company. Could you please clarify why?
-	For all the claims with the Pending status, the Fire Marshall Status is showing as ‘July’ even though those are submitted with the monthly states.

8T004993371
4E004958066


0V004950911 kansas
6C004959424 george closed to open, wasn't updated/detected
7U005012601 - multiple coverage $$ issue

SendDate Error: 20190711
8R005029600
2D005005466


6T004973075
-	3T004966462 – This is GA Fire claim with multiple coverages one coverage with Amount and Other coverage does not have amounts. We are expecting to see this claim on the dashboard with the exception as ‘Settlement Amount Missing’. But we are not seeing this claim on the dashboard
-	0F004958890 – This is KS Fire claim with multiple coverages one coverage with Amount and Other coverage does not have amounts. We are expecting to see this claim on the Details tab with Pending status because the Loss Description is ‘Fire Test’, but we are not seeing this claim on the dashboard
-	Login with Internal user and go to the Details tab – We are not seeing any claims with ‘SENT’ status for any company. Could you please clarify why?
-	For all the claims with the Pending status, the Fire Marshall Status is showing as ‘July’ even though those are submitted with the monthly states.

Please clarify the above issues 
SELECT TOP 100 * FROM dbo.FireMarshalExtract
*/
--1G004548024, 6I004926655 ,9R003589957
--0G004423074
--SELECT TOP 10 * FROM dbo.FireMarshalExtract
--WHERE
--	FireMarshalExtract.reportingStatus = 'Exception'r


--20190724
--6V005024123 money  -- 
--7U005012601 - multiple coverage, one coverage closed
--4M005006272
/*
3K004950199
2Q004998711
8U004973007
*/
DECLARE @IAllClm VARCHAR(11) = '5C004215225'
SELECT NULL AS 'FireMarshalExtract',FireMarshalExtract.lossStateCode, *
FROM dbo.FireMarshalExtract WITH(NOLOCK)
WHERE FireMarshalExtract.isoFileNumber =@IAllClm
--USE CLAIMSEARCH_PROD


SELECT NULL AS 'V_Extract_FM_V1', *
FROM ClaimSearch_Prod.dbo.V_Extract_FM_V1
WHERE
	V_Extract_FM_V1.I_ALLCLM = @IAllClm


--SELECT NULL AS 'InvolvedParty',
--	involvedPartySequenceId,
--	dateInserted,
--	*
--FROM
--	dbo.InvolvedParty WITH(NOLOCK)
--WHERE
--	InvolvedParty.isoClaimId = @IAllClm
--ORDER BY
--	InvolvedParty.dateInserted,
--	InvolvedParty.involvedPartySequenceId;

/*
SELECT NULL AS 'InvolvedParty',
	involvedPartySequenceId,
	dateInserted,
	*
FROM
	dbo.InvolvedParty WITH(NOLOCK)
WHERE
	InvolvedParty.isoClaimId = @IAllClm
ORDER BY
	InvolvedParty.involvedPartySequenceId,
	InvolvedParty.dateInserted;
*/

SELECT NULL AS 'CLT00001', *
FROM
	ClaimSearch_Prod.dbo.CLT00001
WHERE
	CLT00001.I_ALLCLM = @IAllClm

SELECT NULL AS 'CLT0001A', *
FROM
	ClaimSearch_Prod.dbo.CLT0001A
WHERE
	CLT0001A.I_ALLCLM = @IAllClm
	/*
SELECT NULL AS 'CLT00035',
	*
FROM
	ClaimSearch_Prod.dbo.CLT00035
WHERE
	CLT00035.I_ALLCLM = @IAllClm


SELECT NULL AS 'CLT0035A',
	*
FROM
	ClaimSearch_Prod.dbo.CLT0035A
WHERE
	CLT0035A.I_ALLCLM = @IAllClm
	*/
	
	

SELECT NULL AS 'CLT00004', *
FROM
	ClaimSearch_Prod.dbo.CLT00004
WHERE
	CLT00004.I_ALLCLM = @IAllClm
ORDER BY
	I_NM_ADR
	
	
SELECT NULL AS 'CLT00014',
	CLT00014.C_LOSS_TYP,
	CLT00014.C_CVG_TYP,
	*
FROM ClaimSearch_Prod.dbo.CLT00014
WHERE
	CLT00014.I_ALLCLM = @IAllClm
	

	
SELECT NULL AS 'CLT00017',

	*
FROM ClaimSearch_Prod.dbo.CLT00017
WHERE
	CLT00017.I_ALLCLM = @IAllClm
	
	
	/*
	
SELECT NULL AS 'Aliases', *
FROM
	ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
WHERE
	Aliases.I_ALLCLM = @IAllClm

SELECT NULL AS 'DuplicateDataSetPerformanceHackAliases', *
FROM
	(
		SELECT
			Aliases.I_ALLCLM AS isoClaimId,
			Aliases.I_NM_ADR AS nonAliasInvolvedPartySequenceId,
			Aliases.I_NM_ADR_AKA AS aliasInvolvedPartySequenceId,
			ROW_NUMBER() OVER (
				PARTITION BY
					Aliases.I_ALLCLM,
					Aliases.I_NM_ADR_AKA
				ORDER BY
					Aliases.Date_Insert
			) AS uniqueInstanceValue
		FROM
			ClaimSearch_Prod.dbo.CLT00006 AS Aliases WITH (NOLOCK)
		WHERE
			Aliases.I_ALLCLM = @IAllClm
	) AS DuplicateDataSetPerformanceHackAliases
	
SELECT NULL AS 'ServicesProviders', *
FROM
	ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
WHERE
	ServicesProviders.I_ALLCLM = @IAllClm;
	
SELECT NULL AS 'DuplicateDataSetPerformanceHackSP', *
FROM
	(
		SELECT
			ServicesProviders.I_ALLCLM AS isoClaimId,
			ServicesProviders.I_NM_ADR AS nonSPInvolvedPartySequenceId,
			ServicesProviders.I_NM_ADR_SVC_PRVD AS sPInvolvedPartySequenceId,
			ROW_NUMBER() OVER (
				PARTITION BY
					ServicesProviders.I_ALLCLM,
					ServicesProviders.I_NM_ADR_SVC_PRVD
				ORDER BY
					ServicesProviders.Date_Insert
			) AS uniqueInstanceValue
		FROM
			ClaimSearch_Prod.dbo.CLT00021 AS ServicesProviders WITH (NOLOCK)
		WHERE
			ServicesProviders.I_ALLCLM = @IAllClm
	) AS DuplicateDataSetPerformanceHackSP


*/
SELECT NULL AS 'FireMarshalPendingClaim', FireMarshalPendingClaim.isActive, FireMarshalPendingClaim.isCurrent, FireMarshalPendingClaim.dateInserted,
FireMarshalPendingClaim.coverageTypeCode,
FireMarshalPendingClaim.estimatedLossAmount,
 *
FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.isoFileNumber = @IAllClm
ORDER BY
FireMarshalPendingClaim.elementalClaimId,
FireMarshalPendingClaim.dateInserted

SELECT NULL AS 'FireMarshalClaimSendHistory',FireMarshalClaimSendHistory.isActive, *
FROM dbo.FireMarshalClaimSendHistory WITH(NOLOCK)
WHERE FireMarshalClaimSendHistory.isoFileNumber = @IAllClm

SELECT NULL AS 'FireMarshalExtract',FireMarshalExtract.lossStateCode, FireMarshalExtract.isActive, FireMarshalExtract.isCurrent, *
FROM dbo.FireMarshalExtract WITH(NOLOCK)
WHERE FireMarshalExtract.isoFileNumber = @IAllClm

SELECT NULL AS 'ElementalClaim', *
FROM
	dbo.ElementalClaim
WHERE
	ElementalClaim.isoClaimId = @IAllClm
	

SELECT NULL AS 'FireMarshalDriver', *
FROM
	dbo.FireMarshalDriver
WHERE
	FireMarshalDriver.isoClaimId = @IAllClm
	


SELECT NULL AS 'Claim', *
FROM
	dbo.Claim
WHERE
	Claim.isoClaimId = @IAllClm
	

SELECT NULL AS 'InvolvedParty', *
FROM
	dbo.InvolvedParty WITH(NOLOCK)
WHERE
	InvolvedParty.isoClaimId = @IAllClm

SELECT NULL AS 'Address',
isoClaimId, *
FROM
	dbo.Address
WHERE
	Address.isoClaimId = @IAllClm

SELECT NULL AS 'InvolvedPartyAddressMap', *
FROM dbo.InvolvedPartyAddressMap WITH(NOLOCK)
WHERE
	InvolvedPartyAddressMap.isoClaimId = @IAllClm
	
SELECT NULL AS 'Policy', *
FROM dbo.Policy WITH(NOLOCK)
WHERE
	Policy.isoClaimId = @IAllClm


SELECT NULL AS 'V_MM_Hierarchy', *
FROM ClaimSearch_Prod.dbo.V_MM_Hierarchy WITH(NOLOCK)
	INNER JOIN dbo.Policy WITH(NOLOCK)
	ON Policy.insuranceProviderCompanyCode = V_MM_Hierarchy.lvl0
WHERE
	Policy.isoClaimId = @IAllClm


--SELECT
--	*
--FROM
--	dbo.FireMarshalDriver
--WHERE
--FireMarshalDriver.isoClaimId = @IAllClm

--SELECT *
--FROM dbo.FireMarshalExtract