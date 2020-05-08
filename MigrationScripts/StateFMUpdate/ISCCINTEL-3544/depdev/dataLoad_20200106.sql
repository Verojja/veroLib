USE ClaimSearch_Dev
--USE ClaimSearch_PROD

PRINT 'hsp_UpdateInsertFireMarshalDriver';
EXEC dbo.hsp_UpdateInsertFireMarshalDriver
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;


SELECT @@TRANCOUNT;
SELECT * FROM dbo.FireMarshalDriverActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertAddress';
EXEC dbo.hsp_UpdateInsertAddress
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;


SELECT @@TRANCOUNT;
SELECT * FROM dbo.AddressActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertPolicy';
EXEC dbo.hsp_UpdateInsertPolicy
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;


SELECT @@TRANCOUNT;
SELECT * FROM dbo.PolicyActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertAdjuster';
EXEC dbo.hsp_UpdateInsertAdjuster
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;


SELECT @@TRANCOUNT;
SELECT * FROM dbo.AdjusterActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertClaim';
EXEC dbo.hsp_UpdateInsertClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;


SELECT @@TRANCOUNT;
SELECT * FROM dbo.ClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertInvolvedParty';
EXEC dbo.hsp_UpdateInsertInvolvedParty
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;


SELECT @@TRANCOUNT;
SELECT * FROM dbo.InvolvedPartyActivityLog WITH(NOLOCK) ORDER BY 1 DESC;
SELECT * FROM dbo.IPAddressMapActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertElementalClaim';
EXEC dbo.hsp_UpdateInsertElementalClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;


SELECT @@TRANCOUNT;
SELECT * FROM dbo.ElementalClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC;
------------------------------------------------------------------
BEGIN TRANSACTION;

PRINT 'hsp_UpdateInsert: #LocationOfLossData';
EXEC dbo.hsp_FireMarshalSendClaims
	@mustMatchDB2FMProcess = 0,
	@executionDateParam = '20080101';


SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FireMarshalGenerationLog WITH(NOLOCK) ORDER BY 1 DESC;

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;
------------------------------------------------------------------

PRINT 'hsp_UpdateInsertFMPendingClaim';
EXEC dbo.hsp_UpdateInsertFMPendingClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;
	

SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FMPendingClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertFMHistoricClaim';
EXEC dbo.hsp_UpdateInsertFMHistoricClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;
	

SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FMClaimSendHistoryActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertFireMarshalExtract';
EXEC dbo.hsp_UpdateInsertFireMarshalExtract
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;
	

SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FireMarshalExtractActivityLog WITH(NOLOCK) ORDER BY 1 DESC;

------------------------------------------------------------------
BEGIN TRANSACTION;

PRINT 'Migrate for FM Claims with qualifying ProjectedGenDate';
EXEC dbo.hsp_FireMarshalSendClaims
	@mustMatchDB2FMProcess = 0,
	@executionDateParam = NULL;


SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FireMarshalGenerationLog WITH(NOLOCK) ORDER BY 1 DESC;

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;
------------------------------------------------------------------

PRINT 'hsp_UpdateInsertFMPendingClaim';
EXEC dbo.hsp_UpdateInsertFMPendingClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;
	

SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FMPendingClaimActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertFMHistoricClaim';
EXEC dbo.hsp_UpdateInsertFMHistoricClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;
	

SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FMClaimSendHistoryActivityLog WITH(NOLOCK) ORDER BY 1 DESC;


PRINT 'hsp_UpdateInsertFireMarshalExtract';
EXEC dbo.hsp_UpdateInsertFireMarshalExtract
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1;
	

SELECT @@TRANCOUNT AS 'TranCount';
SELECT * FROM dbo.FireMarshalExtractActivityLog WITH(NOLOCK) ORDER BY 1 DESC;
