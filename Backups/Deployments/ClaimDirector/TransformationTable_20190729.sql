SET NOEXEC OFF;

USE ClaimSearch_DEV
--USE ClaimSearch_Prod
/*
*/	

BEGIN TRANSACTION

--/*
	--/*
	--DROP VIEW 
	----*/
	----DROP TABLE 
	--DROP TABLE 
--*/

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2828
Date: 2019-07-29
Author: Robert David Warner
Description: Output display table for UAT Dashboard for ClaimDirectorPlatinumRules.
				Includes Transformation of "rules".
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.ClaimDirectorPlatinumExtract /*TransformationTable*/
(
	--isoClaimID ????? GUID? "ID"?  BIGINT?,
	i_CUST CHAR(4) NOT NULL, /*I_CUST*/
	isoFileNumber VARCHAR(11) NULL, /*I_ALLCLM*/
	claimNumber VARCHAR(30) NULL, /*N_CLM*/
	policyNumber VARCHAR(30) NOT NULL,  /*N_POL*/
	lossDate DATETIME2(0) NULL, /*D_OCUR*/
	lossTypeCode CHAR(4) NULL,
	lossTypeDescription /*"lossType"*/ VARCHAR(42) NULL,
	lossGroupTypeDescription /*"t_LOSS_TYP_GRP"*/
	
	coverageTypeCode CHAR(4) NULL,
	coverageTypeDescription VARCHAR(42) NULL,
	
	policyTypeCode CHAR(4) NULL, /*C_POL_TYP*/
	c_POL_TYP_grp CHAR(4) NULL, /*C_POL_TYP*/
	policyTypeDescription /*"policyType"*/ VARCHAR(100) NULL, /*T_POL_TYP*/
	scoredDate DATE NOT NULL,
	score SMALLINT NOT NULL
	policyInceptionDate  DATE NOT NULL,
	policyExpirationDate  DATE NOT NULL,
	firstReportDate  DATE NOT NULL,
	
	address
		--scrubbedAddressLine1 /*T_ADR_LN1*/ VARCHAR(50) NULL,
		--scrubbedAddressLine2 /*T_ADR_LN2*/ VARCHAR(50) NULL,
	suite
		-- ????
	city
		--scrubbedCityName /*M_CITY*/ VARCHAR(25) NULL,
	loss State
		--scrubbedStateCode /*C_ST_ALPH*/ CHAR(2) NULL,
	zip
		--scrubbedZipCode /*C_ZIP*/ CHAR(5) NULL,
	lossAddr_Lat
		--latitude VARCHAR(15) NULL,
	lossAddr_Long
		--longitude VARCHAR(15) NULL,


/*NOT PERFORMANT*/
	--scrubbedAddressLine1 /*T_ADR_LN1*/ VARCHAR(50) NULL,
	--scrubbedAddressLine2 /*T_ADR_LN2*/ VARCHAR(50) NULL,
	--scrubbedCityName /*M_CITY*/ VARCHAR(25) NULL,
	--scrubbedStateCode /*C_ST_ALPH*/ CHAR(2) NULL,
	--scrubbedZipCode /*C_ZIP*/ CHAR(5) NULL,
	--scrubbedZipCodeExtended /*C_ZIP*/ CHAR(4) NULL,
	--scrubbedCountyName /*C_CNTRY*/ VARCHAR(25) NULL,
	--scrubbedCountyFIPS CHAR(5) NULL,
	--scrubbedCountryCode /*C_CNTRY*/ VARCHAR(3) NULL, 
	--latitude VARCHAR(15) NULL,
	--longitude VARCHAR(15) NULL,
/*MORE PERFORMANT*/
	--originalAddressLine1 /*T_ADR_LN1*/ VARCHAR(50) NULL,
	--originalAddressLine2 /*T_ADR_LN2*/ VARCHAR(50) NULL,
	--originalCityName /*M_CITY*/ VARCHAR(25) NULL,
	--originalStateCode /*C_ST_ALPH*/ CHAR(2) NULL,
	--originalZipCode /*C_ZIP*/ VARCHAR(9) NULL,
	

);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_________ 
	ON dbo.NIX_________ ( )
	INCLUDE ();
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO