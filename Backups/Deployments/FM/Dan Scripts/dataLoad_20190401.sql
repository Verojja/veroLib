USE ClaimSearch_Dev
/* SELECT @@TRANCOUNT
ROLLBACK

------------------------------------------------------------------------
------------------------------------------------------------------------
EXEC dbo.hsp_UpdateInsertFireMarshalDriver
	@dateFilterParam = '20080101',
	@dailyLoadOverride = 1
/*fullhistoryload runtime:  2 min 46 sec*/
SELECT COUNT(*) FROM dbo.FireMarshalDriver
COUNT = 4,517,929
SELECT * FROM dbo.FireMarshalDriverActivityLog WITH(NOLOCK) ORDER BY 1 DESC
------------------------------------------------------------------------
------------------------------------------------------------------------
EXEC dbo.hsp_UpdateInsertAddress
	@dateFilterParam = '20080101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 1.5h
	delta: 4 min 50 sec*/
SELECT COUNT(*) FROM dbo.Address
COUNT= 12,981,663
SELECT * FROM dbo.AddressActivityLog WITH(NOLOCK)


EXEC dbo.hsp_UpdateInsertPolicy
	@dateFilterParam = '20080101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 3.1 min*/
SELECT COUNT(*) FROM dbo.Policy
COUNT= 4,517,929
SELECT * FROM dbo.PolicyActivityLog WITH(NOLOCK)

EXEC dbo.hsp_UpdateInsertAdjuster
	@dateFilterParam = '20080101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 3. min 25 sec*/
SELECT COUNT(*) FROM dbo.Adjuster
COUNT= 3329359
SELECT * FROM dbo.AdjusterActivityLog WITH(NOLOCK)

SELECT @@trancount ROLLBACK
------------------------------------------------------------------------
------------------------------------------------------------------------

/*Claim*/
EXEC dbo.hsp_UpdateInsertClaim
	@dateFilterParam = '20080101',
	@dailyLoadOverride = 1
/*fullhistoryload runtime: was 5 min, after adding addressId : 5 min 53 sec*/
SELECT COUNT(*) FROM dbo.Claim
COUNT= 4,517,929
SELECT * FROM dbo.ClaimActivityLog WITH(NOLOCK)


/*IP*/
EXEC dbo.hsp_UpdateInsertInvolvedParty
	@dateFilterParam = '20080101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 16 min 40 sec*/
SELECT COUNT(*) FROM dbo.InvolvedParty
COUNT= 8,462,046
SELECT COUNT(*) FROM dbo.InvolvedPartyAddressMap
COUNT= 8,461,934
SELECT * FROM dbo.InvolvedPartyActivityLog WITH(NOLOCK)
SELECT * FROM dbo.IPAddressMapActivityLog WITH(NOLOCK)
SELECT TOP 100 * FROM dbo.InvolvedPartyAddressMap
SELECT @@trancount ROLLBACK
------------------------------------------------------------------------
------------------------------------------------------------------------
/*EC*/
EXEC dbo.hsp_UpdateInsertElementalClaim
	@dateFilterParam = '20080101',
	@dailyLoadOverride = 1	
/*fullhistoryload runtime: 9 min*/
SELECT * FROM dbo.ElementalClaimActivityLog WITH(NOLOCK)
SELECT COUNT(*) FROM dbo.ElementalClaim
COUNT= 3,431,237
SELECT @@trancount ROLLBACK
COMMIT
------------------------------------------------------------------------
------------------------------------------------------------------------

