BEGIN TRANSACTION;
USE CLAIMSEARCH_PROD
	--DECLARE @maxdateIP date,
	--		@maxdatedriver DATE,
	--		@maxdatepolicy DATE,
	--		@maxdateaddress DATE,
	--		@maxdateadjuster DATE,
	--		@maxdateclaim DATE,
	--		@maxdateeclaim DATE,
	--		@maxdatesdmIP DATE,
	--		@maxdatesdmdriver DATE,
	--		@maxdatesdmpolicy DATE,
	--		@maxdatesdmaddress DATE,
	--		@maxdatesdmadjuster DATE,
	--		@maxdatesdmclaim DATE,
	--		@maxdatesdmeclaim DATE;

--	SELECT
--		@maxdateIP = MAX(executionDateTime)
--	FROM
--		dbo.InvolvedPartyActivityLog WITH(NOLOCK)
--	WHERE
--		stepid = 100

--	SELECT
--		@maxdatedriver = MAX(executionDateTime) 
--	FROM
--		dbo.FireMarshalDriverActivityLog WITH(NOLOCK)
--	WHERE
--		stepid = 100

--	SELECT
--		@maxdateclaim = MAX(executionDateTime) 
--	FROM
--		dbo.ClaimActivityLog WITH(NOLOCK)
--	WHERE
--		stepid = 100

--	SELECT
--		@maxdateeclaim = MAX(executionDateTime) 
--	FROM
--		dbo.ElementalClaimActivityLog WITH(NOLOCK)
--	WHERE
--		stepid = 100
--		AND DAY(executionDateTime) < DAY(GETDATE());

--	SELECT
--		@maxdateeclaim AS maxdateeclaim
		
--	SELECT
--		@maxdateaddress = MAX(executionDateTime) 
--	FROM
--		dbo.AddressActivityLog WITH(NOLOCK)
--	WHERE
--		stepid = 100

--	SELECT
--		@maxdatepolicy = MAX(executionDateTime) 
--	FROM
--		dbo.PolicyActivityLog WITH(NOLOCK)
--	WHERE
--		stepid = 100

--	SELECT
--		@maxdateadjuster = MAX(executionDateTime) 
--	FROM
--		dbo.AdjusterActivityLog WITH(NOLOCK)
--	WHERE
--		stepid = 100

--SELECT
--	@maxdateIP AS maxdateIP,
--	@maxdatedriver AS maxdatedriver,
--	@maxdatepolicy AS maxdatepolicy,
--	@maxdateaddress AS maxdateaddress,
--	@maxdateadjuster AS maxdateadjuster,
--	@maxdateclaim AS maxdateclaim,
--	@maxdateeclaim AS maxdateeclaim,
--	@maxdatesdmIP AS maxdatesdmIP,
--	@maxdatesdmdriver AS maxdatesdmdriver,
--	@maxdatesdmpolicy AS maxdatesdmpolicy,
--	@maxdatesdmaddress AS maxdatesdmaddress,
--	@maxdatesdmadjuster AS maxdatesdmadjuster,
--	@maxdatesdmclaim AS maxdatesdmclaim,
--	@maxdatesdmeclaim AS maxdatesdmeclaim;
	
	
	

	--SELECT
	--	@maxdateeclaim = MAX(executionDateTime) 
	--FROM
	--	dbo.ElementalClaimActivityLog WITH(NOLOCK)
	--WHERE
	--	stepid = 100
		--AND DAY(executionDateTime) < DAY(GETDATE());

	--SELECT
	--	@maxdateeclaim AS maxdateeclaim;
	

declare @maxdateIP date
, @maxdatedriver date
,@maxdatepolicy date
,@maxdateaddress date
,@maxdateadjuster date
,@maxdateclaim date
,@maxdateeclaim date
,@maxdatesdmIP date
, @maxdatesdmdriver date
,@maxdatesdmpolicy date
,@maxdatesdmaddress date
,@maxdatesdmadjuster date
,@maxdatesdmclaim date
,@maxdatesdmeclaim date



select @maxdateIP = max(executionDateTime) 
from dbo.InvolvedPartyActivityLog  with(nolock)
where stepid = 100

select @maxdatedriver = max(executionDateTime) 
from dbo.FireMarshalDriverActivityLog with(nolock)
where stepid = 100

select @maxdateclaim = max(executionDateTime) 
from dbo.ClaimActivityLog with(nolock)
where stepid = 100

select @maxdateeclaim = max(executionDateTime) 
from dbo.ElementalClaimActivityLog with(nolock)
where stepid = 100
AND DAY(executionDateTime) < DAY(GETDATE());

select @maxdateaddress = max(executionDateTime) 
from dbo.AddressActivityLog with(nolock)
where stepid = 100


select @maxdatepolicy = max(executionDateTime) 
from dbo.PolicyActivityLog with(nolock)
where stepid = 100


select @maxdateadjuster = max(executionDateTime) 
from dbo.AdjusterActivityLog with(nolock)
where stepid = 100

----------------------------------------------
--if exists ( select 1 from dbo.ElementalClaimActivityLog with(nolock)
--where stepid = 100 and convert(date, executionDateTime) > @maxdateeclaim)

--begin



--EXEC dbo.hsp_FireMarshalSendClaims


--end


--------------------------------------------
if exists ( select 1 from [ClaimSearch_Prod].[dbo].ElementalClaimActivityLog with(nolock)
where stepid = 100 and convert(date, executionDateTime) > @maxdateeclaim)

begin

--EXEC dbo.hsp_UpdateInsertFMPendingClaim
PRINT '-EXEC dbo.hsp_UpdateInsertFMPendingClaim'
end

------------------------------------------------------------------------------
--------------------------------------------
if exists ( select 1 from [ClaimSearch_Prod].dbo.FMPendingClaimActivityLog with(nolock)
where stepid = 100 and convert(date, executionDateTime) > @maxdateeclaim)

begin

--EXEC dbo.hsp_UpdateInsertFMHistoricClaim
PRINT '-EXEC dbo.hsp_UpdateInsertFMHistoricClaim'
end

------------------------------------------------------------------------------
--------------------------------------------
if exists ( select 1 from dbo.FMClaimSendHistoryActivityLog with(nolock)
where stepid = 100 and convert(date, executionDateTime) > @maxdateeclaim)

begin

--EXEC dbo.hsp_UpdateInsertFireMarshalExtract
PRINT '-EXEC dbo.hsp_UpdateInsertFireMarshalExtract'
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
---------------------------------------------
		
PRINT 'ROLLBACK';ROLLBACK TRANSACTION;