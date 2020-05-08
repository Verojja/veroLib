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
/***********************************************
WorkItem: ISCSM-24
Date: 2019-07-30
Author: Robert David Warner
Description: Mechanism for data-refresh of the InvolvedParty Table.
				Also inserts new associations into the IPACRMap.

	Creates several indexes on a few key objects, for strong performance improvement on a time-sensitive process.
************************************************/
	CREATE NONCLUSTERED INDEX NIX_CLT00008_Date_Insert
		ON dbo.CLT00008 (Date_Insert)

	CREATE NONCLUSTERED INDEX NIX_CLT00008_N_DRV_LIC
		ON dbo.CLT00008 (N_DRV_LIC)

	CREATE NONCLUSTERED INDEX NIX_CLT00008_I_ALLCLM_I_NM_ADR
		ON dbo.CLT00008 (I_ALLCLM, I_NM_ADR)

	CREATE NONCLUSTERED INDEX NIX_CLT00007_N_SSN
		ON dbo.CLT00007 (N_SSN)
	
	

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

/*For dbo.reason_codes_v1_20190726  AFTER it has been moved:

ALTER TABLE dbo.reason_codes_v1_20190726
	ALTER COLUMN I_ALLCLM VARCHAR(50) NOT NULL

ALTER TABLE dbo.reason_codes_v1_20190726
	ADD CONSTRAINT PK_TempReasonCodes_I_ALLCLM
		PRIMARY KEY (I_ALLCLM)
*/

--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION;