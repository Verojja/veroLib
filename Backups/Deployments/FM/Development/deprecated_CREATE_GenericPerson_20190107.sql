USE ClaimSearch_Dev

SET NOEXEC OFF;
/*
	need to complete the renaming of columns,
	need to add nullability
	need to add first, middle, last etc.
	
	
	SELECT
	*
FROM
	INFORMATION_SCHEMA.COLUMNS
WHERE
	COLUMNS.COLUMN_NAME = 'C_CVG_TYP'

SELECT
	*
FROM
	dbo.Dim_Policy_Type
ORDER BY 1

SELECT
	*
FROM
	dbo.Dim_Loss_Type
ORDER BY 1


SELECT
	*
FROM
	Dim_Coverage_Type
ORDER BY 1


USE ClaimSearch
SELECT
	*
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLES.TABLE_NAME LIKE '%100%'
SELECT
	COUNT(*)
FROM
	CS.Raw_CLT00100_replaced_monthly
	
*/
BEGIN TRANSACTION

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ??????
Date: 2019-01-07
Author: Robert David Warner
Description: Generic Person object for representation of data captured about people and their aliases.
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.InvolvedParty /*People*/
(
	involvedPartyId BIGINT IDENTITY(1,1) NOT NULL,
	isAliasOfInvolvedPartyId BIGINT NULL,
	isBusiness BIT NOT NULL,
	socialSecurityNumber CHAR(9) NULL,
	taxIdentificationNumber CHAR(9) NULL, /*For aliens and corporations (called EIN)*/
	dateOfBirth DATE NULL,
	fullName /*M_FUL_NM*/ VARCHAR(70),
	firstName VARCHAR(100) NULL,
	middleName VARCHAR(100) NULL,
	lastName VARCHAR(100) NULL,
	suffix VARCHAR(50) NULL,
	honorific VARCHAR(50) NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0),
	CONSTRAINT PK_InvolvedParty_involvedPartyId
		PRIMARY KEY (involvedPartyId),
	CONSTRAINT FK_InvolvedParty_isAliasOfInvolvedPartyId_involvedPartyId
		FOREIGN KEY (isAliasOfInvolvedPartyId)
			REFERENCES dbo.InvolvedParty (involvedPartyId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedParty_isAliasOfInvolvedPartyId
	ON dbo.InvolvedParty (isAliasOfInvolvedPartyId)
		INCLUDE (fullName);
CREATE UNIQUE NONCLUSTERED INDEX FUK_InvolvedParty_socialSecurityNumber
	ON dbo.InvolvedParty (socialSecurityNumber)
		WHERE
			InvolvedParty.isActive = 1
			AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL
CREATE UNIQUE NONCLUSTERED INDEX FUK_InvolvedParty_taxIdentificationNumber
	ON dbo.InvolvedParty (taxIdentificationNumber)
		WHERE
			InvolvedParty.isActive = 1
			AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL;
/*Consider check constraint for MUTUAL-EXCLUSIVITY & INCLUSIVE-OR constaint on 
	TaxIdentificationNumber and socialSecurityNumber
*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE VIEW dbo.V_ActiveNonAliasedInvolvedParty
WITH SCHEMABINDING
AS
	SELECT
		InvolvedParty.involvedPartyId,
		InvolvedParty.socialSecurityNumber,
		InvolvedParty.taxIdentificationNumber,
		InvolvedParty.dateOfBirth,
		InvolvedParty.fullName,
		InvolvedParty.firstName,
		InvolvedParty.middleName,
		InvolvedParty.lastName,
		InvolvedParty.suffix,
		InvolvedParty.honorific,
		InvolvedParty.dateInserted
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX UK_V_ActiveNonAliasedInvolvedParty_involvedPartyId
	ON dbo.V_ActiveNonAliasedInvolvedParty (involvedPartyId)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
--PRINT 'COMMIT';COMMIT TRANSACTION;