SET NOEXEC OFF;
/*
	need to complete the renaming of columns,
	need to add nullability
	need to add first, middle, last etc.
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
Date: 2019-01-03
Author: Robert David Warner
Description: Generic InvolvedParty Dimension. Requires at least all data that will be used
				for FireMarshal objects; may be expanded in the future as needed.
				
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.InsurancePolicyClaim /*InsurancePolicyClaim*/
(
	--involvedPart CHAR(9) NULL,yId BIGINT IDENTITY(1,1) NOT NULL,
	insurancePolicyClaimId BIGINT IDENTITY(1,1) NOT NULL,
	iSOFileNumber /*I_ALLCLM*/ VARCHAR(11) NOT NULL,
	involvedPartySequenceNumber /*I_NM_ADR*/ TINYINT NOT NULL,
		/*DevNote: I don't expect we're going to have more than 2 billion involved parties on a single claim,
			so I changed the dataType to TINYINT*/
	--isAliasOfInvolvedPartySequenceNumber TINYINT NULL,
	
	/*PersonObject*/
	--involvedPartyFullName /*M_FUL_NM*/ VARCHAR(70),
	--involvedPartyFirstName VARCHAR(100) NULL,
	--involvedPartyMiddleName VARCHAR(100) NULL,
	--involvedPartyLastName VARCHAR(100) NULL,
	--involvedPartySuffix VARCHAR(50) NULL,
	--involvedPartyHonorific VARCHAR(50) NULL,
	
	involvedPartyRole /*C_ROLE*/ VARCHAR(2),
	/*Deprecating: notSure /*N_NM_ADR_SEQ*/ SMALLINT,*/
		/*DevNote: I think this column is identical to I_NM_ADR.*/
	notSure /*C_NM_TYP*/ VARCHAR(1),
	notSure /*M_FUL_NM*/ VARCHAR(70),
	dateOfBirth /*D_BRTH*/ DATE,
	/*Deprecating: notSure /*NULLINDICATOR1*/ VARCHAR(1),*/
		/*DevNote: Derived*/
	genderCode /*C_GEND*/ VARCHAR(2),
	/*AddressData:*/
	/*DevNote: 
		I think address data should exist in it's own table and re-join
		InvolvedParty at the claim level.*/
	--addressTypeCode /*C_ADR_TYP*/ VARCHAR(3),
	--/*Deprecating: age /*N_AGE*/ SMALLINT,*/
	--	/*DevNote: Derived*/
	--addressLine1 /*T_ADR_LN1*/ VARCHAR(50),
	--addressLine2 /*T_ADR_LN2*/ VARCHAR(50),
	--addressCity /*M_CITY*/ VARCHAR(25),
	--addressStateCode /*C_ST_ALPH*/ VARCHAR(2),
	--addressZipCode /*C_ZIP*/ VARCHAR(9),
	--addressCounty /*C_CNTRY*/ VARCHAR(3),
	
	notSure /*C_NAISC*/ VARCHAR(2),
	notSure /*N_PSPRT*/ VARCHAR(9),
	notSure /*T_RTE_INFO_NM*/ VARCHAR(5),
	notSure /*N_VIN_VEH_OCP*/ VARCHAR(20),
	notSure /*D_OCUR_RPT*/ DATE,
	/*Deprecating: notSure /*NULLINDICATOR2*/ VARCHAR(1),*/
		/*DevNote: Derived*/
	notSure /*F_CSLN_RQST*/ VARCHAR(1),
	notSure /*F_SIU_INVST*/ VARCHAR(1),
	notSure /*F_CLM_NOT_PD*/ VARCHAR(1),
	notSure /*F_ENF_ACTN*/ VARCHAR(1),
	notSure /*F_FRAUD_BUR_RPT*/ VARCHAR(1),
	notSure /*F_FRAUD_OCUR*/ VARCHAR(1),
	notSure /*D_DTH*/ DATE,
	notSure /*NULLINDICATOR3*/ VARCHAR(1),
	notSure /*F_MEDCR_ELIG*/ VARCHAR(1),
	notSure /*F_MEDCR_ELIG_QRY*/ VARCHAR(1),
	notSure /*T_HICN_MEDCR*/ VARCHAR(36),
	notSure /*F_BYP_CMS*/ VARCHAR(1),
	notSure /*F_BYP_CMS_QRY*/ VARCHAR(1),
	notSure /*D_PARADIS_SENT*/ DATE,
	notSure /*NULLINDICATOR4*/ VARCHAR(1),
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0),
	CONSTRAINT PK_InvolvedParty_involvedPartyId
		PRIMARY KEY (involvedPartyId),
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE NONCLUSTERED INDEX UK_InvolvedParty_iSOFileNumber_involvedPartySequenceNumber
	ON dbo.InvolvedParty (iSOFileNumber, involvedPartySequenceNumber);
CREATE NONCLUSTERED INDEX NIX_InvolvedParty_isAliasOfInvolvedPartySequenceNumber
	ON dbo.InvolvedParty (isAliasOfInvolvedPartySequenceNumber)
		INCLUDE (involvedPartyFullName);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END

PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
--PRINT 'COMMIT';COMMIT TRANSACTION;