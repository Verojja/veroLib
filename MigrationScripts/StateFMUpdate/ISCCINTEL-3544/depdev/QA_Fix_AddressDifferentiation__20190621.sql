BEGIN TRANSACTION

USE ClaimSearch_Dev;
--USE ClaimSearch_Prod;

--SELECT * FROM dbo.AddressActivityLog;

UPDATE dbo.AddressActivityLog
	SET
		AddressActivityLog.isSuccessful = 0,
		AddressActivityLog.stepExecutionNotes = 'FullHistoryLoad 20190624 to acount for isLocationOfLossDifferentiator bug'
WHERE
	AddressActivityLog.isSuccessful = 1;

--SELECT * FROM dbo.AddressActivityLog;

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION
PRINT 'COMMIT'; COMMIT TRANSACTION