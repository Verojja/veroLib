SET NOEXEC OFF;

USE ClaimSearch_Dev
--USE ClaimSearch_Prod

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
DECLARE @IAllClm VARCHAR(11) = '0V004950911';

UPDATE dbo.ElementalClaim
SET
	settlementAmount = 1.00
WHERE
	ElementalClaim.isoClaimId = @IAllClm

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

DECLARE @IAllClm VARCHAR(11) = '0V004950911';

SELECT NULL AS 'FireMarshalPendingClaim', FireMarshalPendingClaim.isActive, *
FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.isoFileNumber = @IAllClm


SELECT NULL AS 'FireMarshalExtract', *
FROM dbo.FireMarshalExtract WITH(NOLOCK)
WHERE FireMarshalExtract.isoFileNumber = @IAllClm

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
DECLARE @IAllClm VARCHAR(11) = '0V004950911';

--/*Pend*/'dbo.V_ActiveCurrentPendingFMClaim'
EXEC dbo.hsp_UpdateInsertFMPendingClaim
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	
	
EXEC dbo.hsp_UpdateInsertFireMarshalExtract
	--@dateFilterParam = '20140101',
	@dailyLoadOverride = 1	

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
DECLARE @IAllClm VARCHAR(11) = '0V004950911';

SELECT NULL AS 'FireMarshalPendingClaim', FireMarshalPendingClaim.isActive, *
FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.isoFileNumber = @IAllClm


SELECT NULL AS 'FireMarshalExtract', *
FROM dbo.FireMarshalExtract WITH(NOLOCK)
WHERE FireMarshalExtract.isoFileNumber = @IAllClm
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
ROLLBACK TRANSACTION