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
WorkItem: ISCCINTEL-_____
Date: YYYY-MM-DD
Author: ______ ______
Description: Generic Address object
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Address
(
	addressId BIGINT IDENTITY (1,1) NOT NULL,
	isLocationOfLoss BIT NOT NULL,

	originalAddressLine1 /*T_ADR_LN1*/ VARCHAR(50) NULL,
	originalAddressLine2 /*T_ADR_LN2*/ VARCHAR(50) NULL,
	originalCityName /*M_CITY*/ VARCHAR(25) NULL,
	originalStateCode /*C_ST_ALPH*/ CHAR(2) NULL,
	originalZipCode /*C_ZIP*/ VARCHAR(9) NULL,
	
	scrubbedAddressLine1 /*T_ADR_LN1*/ VARCHAR(50) NULL,
	scrubbedAddressLine2 /*T_ADR_LN2*/ VARCHAR(50) NULL,
	scrubbedCityName /*M_CITY*/ VARCHAR(25) NULL,
	scrubbedStateCode /*C_ST_ALPH*/ CHAR(2) NULL,
	scrubbedZipCode /*C_ZIP*/ CHAR(5) NULL,
	scrubbedZipCodeExtended /*C_ZIP*/ CHAR(4) NULL,
	scrubbedCountyName /*C_CNTRY*/ VARCHAR(25) NULL,
	scrubbedCountyFIPS CHAR(5) NULL,
	scrubbedCountryCode /*C_CNTRY*/ VARCHAR(3) NULL, 
	latitude VARCHAR(15) NULL,
	longitude VARCHAR(15) NULL,
	/*DevNote: Deprecating; July2011 SpatialFeature WhitePapers SQLServer2012 highlight several performance
		improvements; 5-30x performance depending on operation.
		Possibly Deprecate until local isntance using 2012.
	geolocation AS geography::STPointFromText
	(
		'POINT('
		+ longitude
		+ ' '
		+ latitude
		+ ')',
		 4326
	),
	--*/
	geoAccuracy VARCHAR(15) NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	melissaMappingKey BIGINT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	CONSTRAINT PK_Address_addressId
		PRIMARY KEY CLUSTERED (addressId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_"TableOrViewName"_"columnName"
	ON ____."TableOrViewName" ("columnName")
	INCLUDE ("columnName", "columnName", "columnName", "columnName"...);
	GO
CREATE NONCLUSTERED INDEX NIX_"TableOrViewName"_"columnName"_"columnName"
	ON ____."TableOrViewName" ("columnName", "columnName")
	INCLUDE ("columnName", "columnName", "columnName", "columnName"...);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-_____
Date: YYYY-MM-DD
Author: _______ _______
Description: INDEXED VIEW representing ________....
************************************************/
CREATE VIEW ____.V_______
WITH SCHEMABINDING
AS
(
	SELECT
		________,
		________,
		________,
		________,
		________,
		.....
	FROM
		____.______
	WHERE
		______.________ 
		AND ______.________
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_"TableOrViewName"_"columnName"
	ON dbo.V_"TableOrViewName" ("columnName")
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_"TableOrViewName"_"columnName"
	ON dbo.V_"TableOrViewName" ("columnName", "columnName")
	INCLUDE ("columnName", "columnName", "columnName"...);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-______
Date: YYYY-MM-DD
Author: ________ ________
Description: Logging table for the ________ object
************************************************/
CREATE TABLE ____.________ActivityLog
(
	________ActivityLogId BIGINT IDENTITY(1,1) NOT NULL,
	productCode VARCHAR(50) NULL,
	sourceDateTime DATETIME2(0) NOT NULL,
	executionDateTime DATETIME2(0) NOT NULL,
	stepId TINYINT NOT NULL,
	stepDescription VARCHAR(1000) NULL,
	stepStartDateTime DATETIME2(0) NULL,
	stepEndDateTime DATETIME2(0) NULL,
	executionDurationInSeconds AS DATEDIFF(SECOND,stepStartDateTime,stepEndDateTime),
	recordsAffected BIGINT NULL,
	isSuccessful BIT NOT NULL,
	stepExecutionNotes VARCHAR(1000) NULL,
	CONSTRAINT PK_________ActivityLog_addressActivityLogId
		PRIMARY KEY CLUSTERED (________ActivityLogId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_________ActivityLog_isSuccessful_stepId_executionDateTime
	ON ____.________ActivityLog (isSuccessful, stepId, executionDateTime);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

EXEC sp_help 'dbo.Address'
EXEC sp_help 'dbo.V_"TableOrViewName"'
EXEC sp_help 'dbo.________ActivityLog'

PRINT 'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT 'COMMIT TRANSACTION';COMMIT TRANSACTION;

/*
*****************************************
*	Env: JDESQLPRD3.ClaimSearch_Dev		*
*	User: VRSKJDEPRD\i24325				*
*	Time: Nov 20 2019 12:08PM			*
*****************************************
COMMIT TRANSACTION

*/