USE ClaimSearch_Dev
--USE ClaimSearch_PROD
SELECT * FROM 
/* SELECT @@TRANCOUNT
ROLLBACK
COMMIT
--TRUNCATE TABLE dbo.FireMarshalDriver
------------------------------------------------------------------------
------------------------------------------------------------------------
SELECT * FROM dbo.FireMarshalDriverActivityLog
EXEC dbo.hsp_UpdateInsertFireMarshalDriver
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1
/*fullhistoryload runtime:  5 min 46 sec*/
SELECT COUNT(*) FROM dbo.FireMarshalDriver
SELECT COUNT(*) FROM dbo.FireMarshalDriverActivityLog
COUNT = 4,517,929 vs. 2,211,895
SELECT * FROM dbo.FireMarshalDriverActivityLog WITH(NOLOCK) ORDER BY 1 DESC

(1783754 row(s) affected)

(1 row(s) affected)

(0 row(s) affected)

(1 row(s) affected)

(0 row(s) affected)

(1 row(s) affected)

(1783754 row(s) affected)

(1 row(s) affected)

------------------------------------------------------------------------
------------------------------------------------------------------------
EXEC dbo.hsp_UpdateInsertAddress
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1		
/*fullhistoryload runtime: 1.5h
	delta: 10. min 30 sec*/
SELECT COUNT(*) FROM dbo.Address
COUNT= 12,981,663 --> 13,038,011
SELECT * FROM dbo.AddressActivityLog WITH(NOLOCK) ORDER BY 1 DESC

(2271967 row(s) affected)

(1 row(s) affected)

(63 row(s) affected)

(1 row(s) affected)

(489039 row(s) affected)

(1 row(s) affected)

(4733912 row(s) affected)

(1 row(s) affected)

(1328 row(s) affected)

(1 row(s) affected)

(676769 row(s) affected)

(1 row(s) affected)


EXEC dbo.hsp_UpdateInsertPolicy
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 3.1 min*/
SELECT COUNT(*) FROM dbo.Policy
COUNT= 4,517,929
SELECT * FROM dbo.PolicyActivityLog WITH(NOLOCK) ORDER BY 1 DESC

EXEC dbo.hsp_UpdateInsertAdjuster
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 3. min 25 sec*/
SELECT COUNT(*) FROM dbo.Adjuster
COUNT= 3,329,359 1,776,032
SELECT * FROM dbo.AdjusterActivityLog WITH(NOLOCK) ORDER BY 1 DESC

SELECT @@trancount ROLLBACK
------------------------------------------------------------------------
------------------------------------------------------------------------

/*Claim*/
EXEC dbo.hsp_UpdateInsertClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1
/*fullhistoryload runtime: was 5 min, after adding addressId : 5 min 53 sec*/
/*delta runtime: was 15 min!!!!!*/
SELECT COUNT(*) FROM dbo.Claim
COUNT= 4,517,9291
SELECT * FROM dbo.ClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC
SELECT TOP 100 * FROM dbo.Claim WITH(NOLOCK)


/*IP*/
EXEC dbo.hsp_UpdateInsertInvolvedParty
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 16 min 40 sec*/
SELECT COUNT(*) FROM dbo.InvolvedParty
COUNT= 8,462,046
SELECT COUNT(*) FROM dbo.InvolvedPartyAddressMap
COUNT= 8,461,934
SELECT * FROM dbo.InvolvedPartyActivityLog WITH(NOLOCK) ORDER BY 1 DESC
SELECT * FROM dbo.IPAddressMapActivityLog WITH(NOLOCK) ORDER BY 1 DESC
SELECT TOP 100 * FROM dbo.InvolvedPartyAddressMap
SELECT @@trancount ROLLBACK
------------------------------------------------------------------------
------------------------------------------------------------------------
/*EC*/
EXEC dbo.hsp_UpdateInsertElementalClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 9 min*/
SELECT * FROM dbo.ElementalClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC
SELECT COUNT(*) FROM dbo.ElementalClaim
COUNT= 3,431,237
SELECT @@trancount
ROLLBACK
COMMIT
------------------------------------------------------------------------
------------------------------------------------------------------------
/*Pend*/'dbo.V_ActiveCurrentPendingFMClaim'
EXEC dbo.hsp_UpdateInsertFMPendingClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
/*


(41155 row(s) affected)

(1 row(s) affected)

(34632 row(s) affected)

(32516 row(s) affected)

(1 row(s) affected)r

(8726 row(s) affected)

(1 row(s) affected)

(32429 row(s) affected)

(1 row(s) affected)
*/	
	SELECT * FROM dbo.V_ActiveCurrentPendingFMClaim WITH(NOLOCK)


SELECT
	FireMarshalPendingclaim.elementalClaimId,
	FireMarshalPendingclaim.isCurrent,
	COUNT(*)
FROM
	dbo.FireMarshalPendingclaim
GROUP BY
	FireMarshalPendingclaim.elementalClaimId,
	FireMarshalPendingclaim.isCurrent
HAVING
	COUNT(*) > 1
	
/*fullhistoryload runtime: min*/
SELECT COUNT(*) FROM dbo.FireMarshalPendingClaim WITH(NOLOCK)
SELECT COUNT(*) FROM dbo.V_ActiveCurrentPendingFMClaim WITH(NOLOCK)
SELECT * FROM dbo.FMPendingClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC
COUNT= 836146
SELECT @@trancount
ROLLBACK
COMMIT
SELECT COUNT(*) FROM dbo.ElementalClaim
------------------------------------------------------------------------
-------------------------------------------------;-----------------------
------------------------------------------------------------------------
/*Historic/Passive*/
EXEC dbo.hsp_UpdateInsertFMHistoricClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: min*/
SELECT COUNT(*) FROM dbo.FireMarshalClaimSendHistory WITH(NOLOCK)
SELECT COUNT(*) FROM dbo.FMClaimSendHistoryActivityLog WITH(NOLOCK) ORDER BY 1 DESC
COUNT= 
SELECT @@trancount
ROLLBACK
COMMIT
USE CLAIMSEARCH_DEV
SELECT * FROM dbo.FMClaimSendHistoryActivityLog WITH(NOLOCK) ORDER BY 1 DESC
------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------
/*Extract*/
EXEC dbo.hsp_UpdateInsertFireMarshalExtract
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 1.2 min min*/
SELECT COUNT(*) FROM dbo.FireMarshalExtract WITH(NOLOCK)
SELECT * FROM dbo.FireMarshalExtractActivityLog WITH(NOLOCK) ORDER BY 1 DESC
COUNT= 
SELECT @@trancount
ROLLBACK
COMMIT
SELECT
	*
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLES.TABLE_NAME LIKE '%ActivityLog%'
	FireMarshalExtractActivityLog
SELECT * FROM dbo.FireMarshalExtractActivityLog WITH(NOLOCK)

SELECT
	YEAR(dateSubmittedToIso),
	MONTH(dateSubmittedToIso),
	COUNT(*)
FROM dbo.FireMarshalPendingClaim
	WHERE FireMarshalPendingClaim.reportingStatus = 'Pending'
GROUP BY
	YEAR(dateSubmittedToIso),
	MONTH(dateSubmittedToIso)
ORDER BY
	YEAR(dateSubmittedToIso),
	MONTH(dateSubmittedToIso)
	
SELECT COUNT(*) FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.reportingStatus = 'Pending'
AND FireMarshalPendingClaim.isActive = 1
SELECT COUNT(*) FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.reportingStatus <> 'Pending'
SELECT COUNT(*) FROM dbo.FireMarshalClaimSendHistory
------------------------------------------------------------------------
------------------------------------------------------------------------

