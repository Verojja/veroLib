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
			Remove Table for Re-Create
************************************************/
DROP VIEW dbo.V_ActiveAdjuster;
DROP TABLE dbo.Adjuster;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-24
Author: Daniel Ravaglia and Robert David Warner
Description: Generic Adjuster Object
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 20200130
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
			
			Performance: No current notes.

************************************************/
CREATE TABLE dbo.Adjuster
(   
	adjusterId BIGINT IDENTITY (1,1) NOT NULL,
	adjusterCompanyCode CHAR(4) NOT NULL, /*I_CUST*/
	adjusterCompanyName VARCHAR(55) NOT NULL, /*Customer_lvl0*/
	adjusterOfficeCode CHAR(5) NOT NULL, /*I_REGOFF*/ 
	adjusterDateSubmitted date NULL, /*D_ADJ_SUBM*/
	adjusterName VARCHAR(70) NULL, /*M_FUL_NM*/ 
	/*
		deprecated:20200206 RDW.
			adjusterAreaCode SMALLINT NULL, /*N_AREA_WK_SIU*/
			adjusterPhoneNumber INT NULL, /*N_TEL_WK_SIU*/
	*/
	adjusterPhoneNumber VARCHAR(10)
	isActive BIT NOT NULL, 
	dateInserted DATETIME2(0) NOT NULL,	
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	adjusterSequenceId SMALLINT NULL /*N_ADJ_SEQ*/
	CONSTRAINT PK_Adjsuster_adjusterId
		PRIMARY KEY CLUSTERED (adjusterId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_Adjuster_isoClaimId_involvedPartySequenceId_adjusterSequenceId
	ON dbo.Adjuster (isoClaimId, adjusterSequenceId)
	INCLUDE (adjusterCompanyCode, adjusterCompanyName, adjusterOfficeCode, adjusterDateSubmitted, adjusterName, adjusterPhoneNumber, isActive, dateInserted);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-24
Author: Robert David Warner
Description: Filtered Clustered Index for Active Insurance Adjuster(s)
				
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveAdjuster
WITH SCHEMABINDING
AS
(
	SELECT
		Adjuster.adjusterId,
		Adjuster.adjusterCompanyCode,
		Adjuster.adjusterCompanyName,
		Adjuster.adjusterOfficeCode,
		Adjuster.adjusterDateSubmitted,
		Adjuster.adjusterName, 
		Adjuster.adjusterPhoneNumber,
		Adjuster.dateInserted,
		Adjuster.isoClaimId,
		Adjuster.involvedPartySequenceId,
		adjusterSequenceId
	FROM
		dbo.Adjuster
	WHERE
		Adjuster.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveAdjuster_adjusterId
	ON dbo.V_ActiveAdjuster (adjusterId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveAdjuster_isoClaimId_involvedPartySequenceId_adjusterSequenceId
	ON dbo.V_ActiveAdjuster (isoClaimId, involvedPartySequenceId, adjusterSequenceId)
	INCLUDE (adjusterCompanyCode, adjusterCompanyName, adjusterOfficeCode, adjusterDateSubmitted, adjusterName, adjusterPhoneNumber, dateInserted);	
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO