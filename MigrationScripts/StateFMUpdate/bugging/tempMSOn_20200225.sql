SET NOEXEC OFF;

USE ClaimSearch_Dev;
--USE ClaimSearch_Prod;

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

UPDATE dbo.FireMarshalController
SET
	FireMarshalController.endDate = @dateInserted	
OUTPUT
	DELETED.fmStateCode,
	DELETED.fmQualificationRequirmentSetId,
	'P' /*new fmStateStatusCode*/,
	DELETED.frequencyCode,
	DELETED.projectedGenerationDate,
	DELETED.receivesPrint,
	DELETED.receivesFTP,
	DELETED.receivesEmail,
	DELETED.fmContactFirstName,
	DELETED.fmContactMiddleName,
	DELETED.fmContactLastName,
	DELETED.fmContactSuffixName,
	DELETED.fmContactDeptartmentName,
	DELETED.fmContactDivisionName,
	DELETED.fmContactDeliveryAddressLine1,
	DELETED.fmContactDeliveryAddressLine2,
	DELETED.fmContactDeliveryCity,
	DELETED.fmContactDeliveryStateCode,
	DELETED.fmContactZipCode,
	DELETED.fmContactTitleName,
	DELETED.fmContactSalutation,
	@dateInserted,
	DELETED.endDate
INTO dbo.FireMarshalController
(
	fmStateCode,
	fmQualificationRequirmentSetId,
	fmStateStatusCode,
	frequencyCode,
	projectedGenerationDate,
	receivesPrint,
	receivesFTP,
	receivesEmail,
	fmContactFirstName,
	fmContactMiddleName,
	fmContactLastName,
	fmContactSuffixName,
	fmContactDeptartmentName,
	fmContactDivisionName,
	fmContactDeliveryAddressLine1,
	fmContactDeliveryAddressLine2,
	fmContactDeliveryCity,
	fmContactDeliveryStateCode,
	fmContactZipCode,
	fmContactTitleName,
	fmContactSalutation,
	dateInserted,
	endDate
)
FROM
	dbo.FireMarshalController
WHERE
	FireMarshalController.endDate IS NULL
	AND FireMarshalController.fmStateCode = 'MS';
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO



PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Jan 30 2020  3:32PM			*
*****************************************
COMMIT TRANSACTION

*/