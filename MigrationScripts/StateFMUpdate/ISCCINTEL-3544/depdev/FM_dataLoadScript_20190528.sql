/*
USE ClaimSearch_Prod;
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 0.) Driver*/
EXEC dbo.hsp_UpdateInsertFireMarshalDriver
	@dailyLoadOverride = 1
	
	--Should take around 1.5 minutes.
	--You should get around 1,755,147 rows inserted.
	--	SELECT COUNT(*) FROM dbo.Address
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 1.) Address*/
EXEC dbo.hsp_UpdateInsertAddress
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1		
	
	--Should take around 6 minutes.
	--You should get around 5,746,239 rows inserted.
	--	SELECT COUNT(*) FROM dbo.Address
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 2.) Policy*/
EXEC dbo.hsp_UpdateInsertPolicy
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	

	--Should take around 1 minute.
	--You should get around 1,754,784 rows inserted.
	--	SELECT COUNT(*) FROM dbo.Policy
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 3.) Adjuster*/
EXEC dbo.hsp_UpdateInsertAdjuster
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	

	--Should take around 1 minutes.
	--You should get around 1,451,445 rows inserted.
	--	SELECT COUNT(*) FROM dbo.Adjuster
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 4.) Claim*/
EXEC dbo.hsp_UpdateInsertClaim
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1

	--Should take around 3.5 minutes.
	--You should get around 1,754,784 rows inserted.
	--	SELECT COUNT(*) FROM dbo.Claim
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 5.) InvolvedParty && IP_Address_ClaimRole_Map*/
EXEC dbo.hsp_UpdateInsertInvolvedParty
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1

	--Should take around 10.5 minutes.
	--You should get around 3,992,202 inserted into IP && InvolvedPartyAddressMap
	--	SELECT COUNT(*) FROM dbo.InvolvedParty
	--	SELECT COUNT(*) FROM dbo.InvolvedPartyAddressMap
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 6.) ElementalClaim*/
EXEC dbo.hsp_UpdateInsertElementalClaim
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	

	--Should take around 7 minutes.
	--You should get around 1,851,465 rows inserted
	--	SELECT COUNT(*) FROM dbo.ElementalClaim
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 7.) PendingFMClaim*/
EXEC dbo.hsp_UpdateInsertFMPendingClaim
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	

	--Should take around 1 minutes.
	--You should get around 335,136 rows inserted
	--	SELECT COUNT(*) FROM dbo.FireMarshalPendingClaim
------------------------------------------------------------------------
------------------------------------------------------------------------
/* 8.) HistoricFMClaim*/
EXEC dbo.hsp_UpdateInsertFMHistoricClaim
	@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	

	--Should take around 7 minutes.
	--You should get around 197,330 rows inserted
	--	SELECT COUNT(*) FROM dbo.FireMarshalClaimSendHistory
------------------------------------------------------------------------
------------------------------------------------------------------------