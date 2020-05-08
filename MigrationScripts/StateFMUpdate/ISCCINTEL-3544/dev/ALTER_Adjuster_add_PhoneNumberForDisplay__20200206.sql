SET NOEXEC OFF;

--USE ClaimSearch_Prod;
USE ClaimSearch_Dev;

/******MSGLog Snippet. Can be added to comment block at end of query after execute for recordkeeping.******/
DECLARE @tab CHAR(1) = CHAR(9);
DECLARE @newLine CHAR(2) = CHAR(13) + CHAR(10);
DECLARE @currentDBEnv VARCHAR(100) = CAST(@@SERVERNAME + '.' + DB_NAME() AS VARCHAR(100));
DECLARE @currentUser VARCHAR(100) = CAST(CURRENT_USER AS VARCHAR(100));
DECLARE @executeTimestamp VARCHAR(20) = CAST(GETDATE() AS VARCHAR(20));
Print '*****************************************' + @newLine
	+ '*' + @tab + 'Env: ' + 
	+ CASE
	WHEN
		LEN(@currentDBEnv) >=27
	THEN
		@currentDBEnv
	ELSE
		@currentDBEnv + @tab
	END
	+ @tab + '*' +@newLine
	+ '*' + @tab + 'User: ' + @currentUser + @tab + @tab + @tab + @tab + '*' +@newLine
	+ '*' + @tab + 'Time: ' + @executeTimestamp + @tab + @tab + @tab + '*' +@newLine
	+'*****************************************';
/**********************************************************************************************************/
BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-3544
Date: 20200130
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
			
			Performance: No current notes.

************************************************/
ALTER TABLE dbo.Adjuster
	ADD adjusterPhoneNumberForDisplay AS
		CAST(
			ISNULL(
				NULLIF(
					RIGHT(
						'000' + LTRIM(
							RTRIM(
								CAST(
									Adjuster.adjusterAreaCode AS CHAR(3)
								)
							)
						),
						3
					),
					'000'
				),
				''
			) + 
			NULLIF(
				RIGHT(
					'0000000' + LTRIM(
						RTRIM(
							CAST(
								Adjuster.adjusterPhoneNumber AS CHAR(7)
							)
						)
					),
					7
				),
				'0000000'
			)
			AS VARCHAR(10)
		);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
--PRINT 'COMMIT';COMMIT  TRANSACTION;