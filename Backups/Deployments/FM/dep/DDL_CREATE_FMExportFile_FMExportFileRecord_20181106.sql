SET NOEXEC OFF;

BEGIN TRANSACTION

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE TABLE dbo.FMExportFile
(
	fMExportFileId BIGINT NOT NULL,
	recordCount BIGINT NOT NULL,
	stateCode CHAR(2) NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	CONSTRAINT PK_FMExportFile_fMExportFileId
		PRIMARY KEY CLUSTERED (fMExportFileId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE TABLE dbo.FMExportFileRecord
(
	iSOFileNumber /*I_ALLCLM*/ VARCHAR(11),
	involvedPartyName /*M_FUL_NM*/ VARCHAR(70),
	locationOfLossState /*LOL_ST*/ VARCHAR(2),
	customerCode /*I_CUST*/ VARCHAR(4),
	officeCode /*I_REGOFF*/ VARCHAR(5),
	customerPolicyNumber /*POLICY_NO*/ VARCHAR(13),
	customerClaimNumber /*CLAIM_NO*/ VARCHAR(30),
	dateOfLoss /*DATE_OF_LOSS*/ VARCHAR(18),
	policyType /*C_POL_TYP*/ VARCHAR(4),
	coverageType /*C_CVG_TYP*/ VARCHAR(4),
	lossType /*C_LOSS_TYP*/ VARCHAR(4),
	claimStatus /*Status_Type*/ VARCHAR(16),
	estimatedLossAmount /*EST_LOSS*/ BIGINT,
	settlementAmount /*SETTL_AMT*/ BIGINT,
	policyAmount /*POLICY_AMT*/ BIGINT,
	lossDescription /*LOSS_DSC*/ VARCHAR(50),
	locationOfLossAddress /*LOL_ADDR*/ VARCHAR(50),
	locationOfLossCity /*LOL_CITY*/ VARCHAR(25),
	locationOfLossZipCode /*LOL_ZIP*/ VARCHAR(9),
	adjusterFullName /*ADJ_NAME*/ VARCHAR(70),
	adjusterCompanyCode /*ADJ_COMP_CD*/ VARCHAR(9),
	adjusterCompanyName /*ADJ_COMP_NM*/ VARCHAR(50),
	adjusterAddressLine1 /*ADJ_ADDR1*/ VARCHAR(50),
	adjusterAddressLine2 /*ADJ_ADDR2*/ VARCHAR(50),
	adjusterCity /*ADJ_CITY*/ VARCHAR(25),
	adjusterState /*ADJ_ST*/ VARCHAR(2),
	adjusterZipCode /*ADJ_ZIP*/ VARCHAR(9),
	adjusterPhoneNumber /*ADJ_PHONE*/ VARCHAR(10),
	incendinaryFireIndicator /*F_INCEND_FIRE*/ bit,
	partySubjectToSiuInvestigation /*F_SIU_INVST*/ bit,
	siuCompanyCode /*M_SIU_COMP*/ VARCHAR(55),
	siuInvestigatorName /*M_FUL_NM_SIU*/ VARCHAR(55),
	siuWorkPhoneNumber /*N_PHONE_WK_SIU*/ VARCHAR(10),
	siuCellPhoneNumber /*N_PHONE_CELL_SIU*/ VARCHAR(10),
	scrubbedCityName /*MD_City*/ VARCHAR(35),
	scrubbedState  /*MD_State*/ VARCHAR(15),
	scrubbedZipCode /*MD_Zip*/ VARCHAR(10),
	countyName /*MD_GeoCounty*/ VARCHAR(25),
	fipsCountyCode /*MD_GeoCountyFIPS*/ VARCHAR(5),
	lattitude /*MD_Latitude*/ VARCHAR(12),
	longitude /*MD_Longitude*/ VARCHAR(12),
	geocodeAccuracyLevel /*GEOCODE_LEVEL*/ VARCHAR(15),
	totalInsuranceAmount /*A_AMT_INS*/ INT,
	actualCashValueOfBuilding /*A_BLDG_ACTL_VAL*/ INT,
	buildingLossAmountPaid /*A_BLDG_PD*/ INT,
	contentLossAmountPaid /*A_CNNT_PD*/ INT,
	fMExportFileId BIGINT
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
--PRINT 'COMMIT';COMMIT TRANSACTION;
