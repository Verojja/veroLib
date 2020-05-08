SET NOEXEC OFF;
USE ClaimSearch_Prod;

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*One Time update for forcing delta to capture current month.*/
UPDATE dbo.FireMarshalController
	SET
		FireMarshalController.projectedGenerationDate = UpdateValueList.newProjectedGenDateVal
FROM
	dbo.FireMarshalController
	INNER JOIN
	(
		VALUES
			(CAST('2019-08-05' AS DATE),CAST('2019-04-05' AS DATE)),
			(CAST('2019-07-05' AS DATE),CAST('2019-06-05' AS DATE))
	) AS UpdateValueList (currentProjectedGenDateVal, newProjectedGenDateVal)
		ON FireMarshalController.projectedGenerationDate = UpdateValueList.currentProjectedGenDateVal
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
EXEC dbo.hsp_FireMarshalSendClaims
		@mustMatchDB2FMProcess = 1
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

PRINT 'COMMIT';COMMIT TRANSACTION;