SET NOEXEC OFF;

--USE ClaimSearch_Dev;
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
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
			Remove TABLE\VIEW for Re-Create
************************************************/
/*
DROP VIEW dbo.V_ActiveAdjuster;
DROP VIEW dbo.V_ActiveLocationOfLoss
DROP VIEW dbo.V_ActiveNonLocationOfLoss
DROP VIEW dbo.V_ActivePolicy
DROP VIEW dbo.V_ActiveClaim
DROP VIEW dbo.V_ActiveInvolvedParty
DROP VIEW dbo.V_ActiveAliaseInvolvedParty
DROP VIEW dbo.V_ActiveNonAliaseServiceProvider
DROP VIEW dbo.V_ActiveAliaseServiceProvider
DROP VIEW dbo.V_ActiveIPAddressMap
DROP VIEW dbo.V_ActiveElementalClaim
DROP VIEW dbo.V_ActiveCurrentPendingFMClaim;
DROP TABLE dbo.Adjuster;
DROP TABLE dbo.Address
DROP TABLE dbo.Policy
DROP TABLE dbo.Claim
DROP TABLE dbo.InvolvedParty
DROP TABLE dbo.InvolvedPartyAddressMap
DROP TABLE dbo.ElementalClaim
DROP TABLE dbo.FireMarshalPendingClaim;
--*/
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
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	adjusterSequenceId SMALLINT NULL, /*N_ADJ_SEQ*/
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
	adjusterPhoneNumber VARCHAR(10) NULL, /*combination of adjusterAreaCode and N_TEL_WK_SIU(geographicRegionPrefix and LineNumber)*/
	isActive BIT NOT NULL, 
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
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
CREATE NONCLUSTERED INDEX NIX_Adjuster_isoClaimId_adjusterSequenceId
	ON dbo.Adjuster (isoClaimId, adjusterSequenceId)
	INCLUDE (
		adjusterCompanyCode, adjusterCompanyName, adjusterOfficeCode,
		adjusterDateSubmitted, adjusterName, adjusterPhoneNumber,
		isActive, dateInserted, deltaDate
	);
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
		Adjuster.isoClaimId,
		Adjuster.adjusterSequenceId,
		Adjuster.adjusterCompanyCode,
		Adjuster.adjusterCompanyName,
		Adjuster.adjusterOfficeCode,
		Adjuster.adjusterDateSubmitted,
		Adjuster.adjusterName, 
		Adjuster.adjusterPhoneNumber,
		Adjuster.dateInserted,
		Adjuster.deltaDate
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
CREATE NONCLUSTERED INDEX NIX_ActiveAdjuster_isoClaimId_adjusterSequenceId
	ON dbo.V_ActiveAdjuster (isoClaimId, adjusterSequenceId)
	INCLUDE (
		adjusterCompanyCode, adjusterCompanyName, adjusterOfficeCode,
		adjusterDateSubmitted, adjusterName, adjusterPhoneNumber,
		dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Generic Address object
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			NOTE: Sanitized\Scrubbed-Address-data for NON-LocationOfLoss is currently not being imported\updated due to performance reasons.
			
			Performance: Significant performance improvements on one-query(with-date-calc)
							over cross-db-query-with-index-added (40 sec vs. 30 min in worst case).
************************************************/
CREATE TABLE dbo.Address
(
	addressId BIGINT IDENTITY (1,1) NOT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
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
	melissaMappingKey BIGINT NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
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
CREATE NONCLUSTERED INDEX NIX_Address_isoClaimId
	ON dbo.Address (isoClaimId)
	INCLUDE (involvedPartySequenceId, isLocationOfLoss, originalAddressLine1,
		originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1,
		scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended,
		scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude, longitude,
		geoAccuracy, melissaMappingKey, isActive, dateInserted, deltaDate
	);
	GO
CREATE NONCLUSTERED INDEX NIX_Address_isoClaimId_involvedPartySequenceId
	ON dbo.Address (isoClaimId, involvedPartySequenceId)
	INCLUDE (isLocationOfLoss, originalAddressLine1,
		originalAddressLine2, originalCityName, originalStateCode, originalZipCode, scrubbedAddressLine1,
		scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode, scrubbedZipCodeExtended,
		scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude, longitude,
		geoAccuracy, melissaMappingKey, isActive, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW representing addresses that correlate
				to the covered-incident of insurance claims.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		Address.addressId,
		Address.isoClaimId,
		Address.originalAddressLine1,
		Address.originalAddressLine2,
		Address.originalCityName,
		Address.originalStateCode,
		Address.originalZipCode,
		Address.scrubbedAddressLine1,
		Address.scrubbedAddressLine2,
		Address.scrubbedCityName,
		Address.scrubbedStateCode,
		Address.scrubbedZipCode,
		Address.scrubbedZipCodeExtended,
		Address.scrubbedCountyName,
		Address.scrubbedCountyFIPS,
		Address.scrubbedCountryCode,
		Address.latitude,
		Address.longitude,
		Address.geoAccuracy,
		Address.melissaMappingKey,
		Address.dateInserted,
		Address.deltaDate
	FROM
		dbo.Address
	WHERE
		Address.isActive = 1
		AND Address.isLocationOfLoss = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveLocationOfLoss_addressId
	ON dbo.V_ActiveLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveLocationOfLoss_isoClaimId
	ON dbo.V_ActiveLocationOfLoss (isoClaimId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode,
		scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode,
		scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude,
		longitude, geoAccuracy, melissaMappingKey, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW representing addresses that correlate
				to the mailing address for the Insured at the time of a claim.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveNonLocationOfLoss
WITH SCHEMABINDING
AS
(
	SELECT
		Address.addressId,
		Address.isoClaimId,
		Address.involvedPartySequenceId,
		Address.originalAddressLine1,
		Address.originalAddressLine2,
		Address.originalCityName,
		Address.originalStateCode,
		Address.originalZipCode,
		Address.scrubbedAddressLine1,
		Address.scrubbedAddressLine2,
		Address.scrubbedCityName,
		Address.scrubbedStateCode,
		Address.scrubbedZipCode,
		Address.scrubbedZipCodeExtended,
		Address.scrubbedCountyName,
		Address.scrubbedCountyFIPS,
		Address.scrubbedCountryCode,
		Address.latitude,
		Address.longitude,
		Address.geoAccuracy,
		Address.melissaMappingKey,
		Address.dateInserted,
		Address.deltaDate
	FROM
		dbo.Address
	WHERE
		Address.isActive = 1
		AND Address.isLocationOfLoss = 0
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveNonLocationOfLos_addressId
	ON dbo.V_ActiveNonLocationOfLoss (addressId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveNonLocationOfLos_isoClaimId
	ON dbo.V_ActiveNonLocationOfLoss (isoClaimId, involvedPartySequenceId)
	INCLUDE (originalAddressLine1, originalAddressLine2, originalCityName, originalStateCode, originalZipCode,
		scrubbedAddressLine1, scrubbedAddressLine2, scrubbedCityName, scrubbedStateCode, scrubbedZipCode,
		scrubbedZipCodeExtended, scrubbedCountyName, scrubbedCountyFIPS, scrubbedCountryCode, latitude,
		longitude, geoAccuracy, melissaMappingKey, dateInserted, deltaDate
);
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
Description: Generic Policy Object
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Policy
(
	policyId BIGINT IDENTITY (1,1) NOT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	insuranceProviderCompanyCode CHAR(4) NOT NULL, /*I_CUST*/
	insuranceProviderOfficeCode CHAR(5) NOT NULL, /*I_REGOFF*/
	originalPolicyNumber VARCHAR(30) NOT NULL,  /*N_POL*/
	policyTypeCode CHAR(4) NULL, /*C_POL_TYP*/
	policyTypeDescription VARCHAR(100) NULL, /*T_POL_TYP*/
	originalPolicyInceptionDate DATE NULL, /*D_POL_INCP*/
	originalPolicyExperiationDate DATE NULL, /*D_POL_EXPIR*/
	isActive BIT NOT NULL, 
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
	CONSTRAINT PK_Policy_policyId
		PRIMARY KEY CLUSTERED (policyId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_Policy_isoClaimId
	ON dbo.Policy (isoClaimId)
	INCLUDE (insuranceProviderCompanyCode, insuranceProviderOfficeCode, originalPolicyNumber, policyTypeCode, policyTypeDescription,
		originalPolicyInceptionDate, originalPolicyExperiationDate, isActive, dateInserted, deltaDate
	);
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
Description: Indexed view for Generic Policy Object,
				filtered to exclude non Active rows.
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActivePolicy
WITH SCHEMABINDING
AS
(
	SELECT
		Policy.policyId,
		Policy.isoClaimId,
		Policy.insuranceProviderCompanyCode,
		Policy.insuranceProviderOfficeCode,
		Policy.originalPolicyNumber,
		Policy.policyTypeCode,
		Policy.policyTypeDescription,
		Policy.originalPolicyInceptionDate,
		Policy.originalPolicyExperiationDate,
		Policy.dateInserted,
		Policy.deltaDate
	FROM
		dbo.Policy
	WHERE
		Policy.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActivePolicy_policyId
	ON dbo.V_ActivePolicy (policyId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActivePolicy_isoClaimId
	ON dbo.V_ActivePolicy (isoClaimId)
	INCLUDE (insuranceProviderCompanyCode, insuranceProviderOfficeCode, originalPolicyNumber, policyTypeCode, policyTypeDescription,
		originalPolicyInceptionDate, originalPolicyExperiationDate, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Generic Claim object. The relationship between
				Claim and Policy is N:1; however this is
				difficult to capture while entitiy resolution is
				still being implemented.
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.Claim
(
	claimId BIGINT IDENTITY (1,1) NOT NULL,
	isoClaimId VARCHAR(11) NULL /*I_ALLCLM*/,
	originalClaimNumber VARCHAR(30) NULL, /*N_CLM*/
		locationOfLossAddressId BIGINT NOT NULL,
		policyId BIGINT NULL,
	claimSearchSourceSystem CHAR(1) NULL, /*C_CLM_SRCE*/
	claimEntryMethod CHAR(1) NULL, /*C_RPT_SRCE*/
	
	isVoidedByInsuranceCarrier BIT NOT NULL, /*F_VOID*/
	/*isUpdatedViaWeb BIT NULL, /*F_UPD*/*/
	
	lossDescription VARCHAR(50) NULL, /*T_LOSS_DSC*/
	lossDescriptionExtended VARCHAR(200) NULL, /*T_LOSS_DSC_EXT*/
	
	/*Deprecated:*//*catastropheId VARCHAR(4) NULL, /*C_CAT CLT1A*/*/
	isClaimSearchProperty BIT NULL, /*F_PROP*/
	isClaimSearchAuto BIT NULL, /*F_AUTO*/
	isClaimSearchCasualty BIT NULL, /*F_CSLTY*/
	isClaimSearchAPD BIT NULL, /*F_APD*/
	
	isClaimUnderSIUInvestigation BIT NULL,
	siuCompanyName VARCHAR(70) NULL,
	siuRepresentativeFullName VARCHAR(250) NULL,
	siuWorkPhoneNumber CHAR(10) NULL,
	siuCellPhoneNumber CHAR(10) NULL,
	
	dateOfLoss DATETIME2(0) NULL, /*D_OCUR*/
	insuranceCompanyReceivedDate DATETIME2(0) NULL, /*D_INS_CO_RCV*/
	systemDateReceived DATETIME2(0) NULL, /*D_RCV*/

	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
	
	CONSTRAINT PK_Claim_claimId
		PRIMARY KEY CLUSTERED (claimId)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_Claim_isoClaimId
	ON dbo.Claim (isoClaimId)
	INCLUDE (originalClaimNumber, locationOfLossAddressId, policyId, claimSearchSourceSystem, claimEntryMethod,
		isVoidedByInsuranceCarrier, lossDescription, lossDescriptionExtended, isClaimSearchProperty, isClaimSearchAuto,
		isClaimSearchCasualty, isClaimSearchAPD, isClaimUnderSIUInvestigation, siuCompanyName, siuRepresentativeFullName,
		siuWorkPhoneNumber, siuCellPhoneNumber, dateOfLoss, insuranceCompanyReceivedDate, systemDateReceived,
		isActive, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Filtered Clustered Index for Active Insurance Claims.
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveClaim
WITH SCHEMABINDING
AS
(
	SELECT
		claimId,
		isoClaimId,
		originalClaimNumber,
		locationOfLossAddressId,
		policyId,
		claimSearchSourceSystem,
		claimEntryMethod,
		isVoidedByInsuranceCarrier,
		lossDescription,
		lossDescriptionExtended,
		isClaimSearchProperty,
		isClaimSearchAuto,
		isClaimSearchCasualty,
		isClaimSearchAPD,
		isClaimUnderSIUInvestigation,
		siuCompanyName,
		siuRepresentativeFullName,
		siuWorkPhoneNumber,
		siuCellPhoneNumber,
		dateOfLoss,
		insuranceCompanyReceivedDate,
		systemDateReceived,
		/*isActive*/
		dateInserted,
		deltaDate
	FROM
		dbo.Claim
	WHERE
		Claim.isActive = 1
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveClaim_claimId
	ON dbo.V_ActiveClaim (claimId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveClaim_isoClaimId
	ON dbo.V_ActiveClaim (isoClaimId)
	INCLUDE (originalClaimNumber, locationOfLossAddressId, policyId, claimSearchSourceSystem, claimEntryMethod,
		isVoidedByInsuranceCarrier, lossDescription, lossDescriptionExtended, isClaimSearchProperty, isClaimSearchAuto,
		isClaimSearchCasualty, isClaimSearchAPD, isClaimUnderSIUInvestigation, siuCompanyName, siuRepresentativeFullName,
		siuWorkPhoneNumber, siuCellPhoneNumber, dateOfLoss, insuranceCompanyReceivedDate, systemDateReceived,
		dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner, Daniel Ravaglia
Description: Generic Person object for representation of people
				their aliases, and their service providers.
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.InvolvedParty /*People*/
(
	involvedPartyId BIGINT IDENTITY(1,1) NOT NULL,
	isAliasOfInvolvedPartyId BIGINT NULL,
	isServiceProviderOfInvolvedPartyId BIGINT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	isBusiness BIT NOT NULL,
	/*Deprecated: moved to claimIPMap*//*involvedPartyRoleCode VARCHAR(2) NULL, /*C_ROLE */*/
	taxIdentificationNumberObfuscated VARCHAR(36) NULL, /*For aliens and corporations (called EIN)*/ -- DCR
	taxIdentificationNumberLastFour CHAR(4) NULL, /*For aliens and corporations (called EIN)*/ -- DCR
	socialSecurityNumberObfuscated VARCHAR(36) NULL,			-- DCR (N_SSN4 from clt7a
	socialSecurityNumberLastFour CHAR(4) NULL,			-- DCR (N_SSN4 from clt7a
	hICNObfuscated VARCHAR(36) NULL,							---4
	driversLicenseNumberObfuscated VARCHAR(52) NULL,			-- DCR (N_DRV_LIC from clt8a
	driversLicenseNumberLast3 CHAR(3) NULL,			-- DCR (N_DRV_LIC from clt8a
	driversLicenseClass VARCHAR(3) NULL,			-- DCR (C_DRV_LIC_CLASS from clt8a
	driversLicenseState CHAR(2) NULL,				-- DCR (C_ST_ALPH from clt8a
	genderCode CHAR(2) NULL,						-- DCR {C_GEND from clt4
	passportID VARCHAR(9) NULL,							-- DCR {N_PSPRT from clt4
	professionalMedicalLicense VARCHAR(20) NULL,	-- DCR (N_PROF_MED_LIC FROM CLT10
	isUnderSiuInvestigation BIT NOT NULL,			-- DCR {F_SIU_INVST from clt4
	isLawEnforcementAction BIT NOT NULL,			-- DCR {F_ENF_ACTN
	isReportedToFraudBureau BIT NOT NULL,			-- DCR {F_FRAUD_BUR_RPT
	isFraudReported BIT NOT NULL,					-- DCR {F_FRAUD_OCUR
	dateOfBirth DATE NULL,
	fullName /*M_FUL_NM*/ VARCHAR(70) NULL,
	firstName VARCHAR(100) NULL,
	middleName VARCHAR(100) NULL,
	lastName VARCHAR(100) NULL,
	suffix VARCHAR(50) NULL,
	businessArea CHAR(3) NULL,
	businessTel CHAR(7) NULL,
	cellArea CHAR(3) NULL,
	cellTel CHAR(7) NULL,
	faxArea CHAR(3) NULL,
	faxTel CHAR(7) NULL,
	homeArea CHAR(3) NULL,
	homeTel CHAR(7) NULL,
	pagerArea CHAR(3) NULL,
	pagerTel CHAR(7) NULL,
	otherArea CHAR(3) NULL,
	otherTel CHAR(7) NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
	CONSTRAINT PK_InvolvedParty_involvedPartyId
		PRIMARY KEY (involvedPartyId)
	/*CONSTRAINT FK_InvolvedParty_isAliasOfInvolvedPartyId_involvedPartyId
		FOREIGN KEY (isAliasOfInvolvedPartyId)
			REFERENCES dbo.InvolvedParty (involvedPartyId),
	CONSTRAINT FK_InvolvedParty_isServiceProviderOfInvolvedPartyId_involvedPartyId
		FOREIGN KEY (isServiceProviderOfInvolvedPartyId)
			REFERENCES dbo.InvolvedParty (involvedPartyId)
	*/
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedParty_isoClaimId_involvedPartySequenceId
	ON dbo.InvolvedParty (isoClaimId, involvedPartySequenceId)
	INCLUDE (isAliasOfInvolvedPartyId, isServiceProviderOfInvolvedPartyId, isBusiness, taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour,
		hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction,
		isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel,
		cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel,
		isActive, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Filtered Indexed View specific to InvolvedParty records
				that are both NOT Aliases, and NOT ServiceProviders.
			
			/*Deprecated 20190130 RDW*//*InvolvedParty.isMedicareEligible, */
			/*InvolvedParty.involvedPartyRoleCode moved to IMAP*/
			
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveInvolvedParty
WITH SCHEMABINDING
AS
	SELECT
		InvolvedParty.involvedPartyId,
		/*isAliasOfInvolvedPartyId*/
		/*isServiceProviderOfInvolvedPartyId*/
		InvolvedParty.isoClaimId,
		InvolvedParty.involvedPartySequenceId,
		InvolvedParty.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass,
		InvolvedParty.driversLicenseState,
		InvolvedParty.genderCode,
		InvolvedParty.passportID,
		InvolvedParty.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau,
		InvolvedParty.isFraudReported,
		InvolvedParty.dateOfBirth,
		InvolvedParty.fullName,
		InvolvedParty.firstName,
		InvolvedParty.middleName,
		InvolvedParty.lastName,
		InvolvedParty.suffix,
		InvolvedParty.businessArea,
		InvolvedParty.businessTel,
		InvolvedParty.cellArea,
		InvolvedParty.cellTel,
		InvolvedParty.faxArea,
		InvolvedParty.faxTel,
		InvolvedParty.homeArea,
		InvolvedParty.homeTel,
		InvolvedParty.pagerArea,
		InvolvedParty.pagerTel,
		InvolvedParty.otherArea,
		InvolvedParty.otherTel,
		/*isActive*/
		InvolvedParty.dateInserted,
		InvolvedParty.deltaDate
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveInvolvedParty_involvedPartyId
	ON dbo.V_ActiveInvolvedParty (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveInvolvedParty_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveInvolvedParty (isoClaimId, involvedPartySequenceId)
	INCLUDE (isBusiness, taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour,
		hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction,
		isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel,
		cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel,
		dateInserted, deltaDate
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Filtered Indexed View specific to InvolvedParty records
				that ARE Aliases, but are NOT ServiceProviders.
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveAliaseInvolvedParty
WITH SCHEMABINDING
AS
	SELECT
		InvolvedParty.involvedPartyId,
		isAliasOfInvolvedPartyId,
		/*isServiceProviderOfInvolvedPartyId*/
		InvolvedParty.isoClaimId,
		InvolvedParty.involvedPartySequenceId,
		InvolvedParty.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass,
		InvolvedParty.driversLicenseState,
		InvolvedParty.genderCode,
		InvolvedParty.passportID,
		InvolvedParty.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau,
		InvolvedParty.isFraudReported,
		InvolvedParty.dateOfBirth,
		InvolvedParty.fullName,
		InvolvedParty.firstName,
		InvolvedParty.middleName,
		InvolvedParty.lastName,
		InvolvedParty.suffix,
		InvolvedParty.businessArea,
		InvolvedParty.businessTel,
		InvolvedParty.cellArea,
		InvolvedParty.cellTel,
		InvolvedParty.faxArea,
		InvolvedParty.faxTel,
		InvolvedParty.homeArea,
		InvolvedParty.homeTel,
		InvolvedParty.pagerArea,
		InvolvedParty.pagerTel,
		InvolvedParty.otherArea,
		InvolvedParty.otherTel,
		/*isActive*/
		InvolvedParty.dateInserted,
		InvolvedParty.deltaDate
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NOT NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveAliaseInvolvedParty_involvedPartyId
	ON dbo.V_ActiveAliaseInvolvedParty (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveAliaseInvolvedParty_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveAliaseInvolvedParty (isoClaimId, involvedPartySequenceId)
	INCLUDE (isAliasOfInvolvedPartyId, isBusiness, taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour,
		hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction,
		isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel,
		cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel,
		dateInserted, deltaDate
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Filtered Indexed View specific to InvolvedParty records
				that are NOT Aliases, but ARE ServiceProviders.
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveNonAliaseServiceProvider
WITH SCHEMABINDING
AS
	SELECT
		InvolvedParty.involvedPartyId,
		/*isAliasOfInvolvedPartyId*/
		InvolvedParty.isServiceProviderOfInvolvedPartyId,
		InvolvedParty.isoClaimId,
		InvolvedParty.involvedPartySequenceId,
		InvolvedParty.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass,
		InvolvedParty.driversLicenseState,
		InvolvedParty.genderCode,
		InvolvedParty.passportID,
		InvolvedParty.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau,
		InvolvedParty.isFraudReported,
		InvolvedParty.dateOfBirth,
		InvolvedParty.fullName,
		InvolvedParty.firstName,
		InvolvedParty.middleName,
		InvolvedParty.lastName,
		InvolvedParty.suffix,
		InvolvedParty.businessArea,
		InvolvedParty.businessTel,
		InvolvedParty.cellArea,
		InvolvedParty.cellTel,
		InvolvedParty.faxArea,
		InvolvedParty.faxTel,
		InvolvedParty.homeArea,
		InvolvedParty.homeTel,
		InvolvedParty.pagerArea,
		InvolvedParty.pagerTel,
		InvolvedParty.otherArea,
		InvolvedParty.otherTel,
		/*isActive*/
		InvolvedParty.dateInserted,
		InvolvedParty.deltaDate
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NOT NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveNonAliaseServiceProvider_involvedPartyId
	ON dbo.V_ActiveNonAliaseServiceProvider (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveNonAliaseServiceProvider_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveNonAliaseServiceProvider (isoClaimId, involvedPartySequenceId)
	INCLUDE (isServiceProviderOfInvolvedPartyId, isBusiness, taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour,
		hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction,
		isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel,
		cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel,
		dateInserted, deltaDate
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: Filtered Indexed View specific to InvolvedParty records
				that ARE BOTH Aliases, AND ServiceProviders.
				
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveAliaseServiceProvider
WITH SCHEMABINDING
AS
	SELECT
		InvolvedParty.involvedPartyId,
		InvolvedParty.isAliasOfInvolvedPartyId,
		InvolvedParty.isServiceProviderOfInvolvedPartyId,
		InvolvedParty.isoClaimId,
		InvolvedParty.involvedPartySequenceId,
		InvolvedParty.isBusiness,
		InvolvedParty.taxIdentificationNumberObfuscated,
		InvolvedParty.taxIdentificationNumberLastFour,
		InvolvedParty.socialSecurityNumberObfuscated,
		InvolvedParty.socialSecurityNumberLastFour,
		InvolvedParty.hICNObfuscated,
		InvolvedParty.driversLicenseNumberObfuscated,
		InvolvedParty.driversLicenseNumberLast3,
		InvolvedParty.driversLicenseClass,
		InvolvedParty.driversLicenseState,
		InvolvedParty.genderCode,
		InvolvedParty.passportID,
		InvolvedParty.professionalMedicalLicense,
		InvolvedParty.isUnderSiuInvestigation,
		InvolvedParty.isLawEnforcementAction,
		InvolvedParty.isReportedToFraudBureau,
		InvolvedParty.isFraudReported,
		InvolvedParty.dateOfBirth,
		InvolvedParty.fullName,
		InvolvedParty.firstName,
		InvolvedParty.middleName,
		InvolvedParty.lastName,
		InvolvedParty.suffix,
		InvolvedParty.businessArea,
		InvolvedParty.businessTel,
		InvolvedParty.cellArea,
		InvolvedParty.cellTel,
		InvolvedParty.faxArea,
		InvolvedParty.faxTel,
		InvolvedParty.homeArea,
		InvolvedParty.homeTel,
		InvolvedParty.pagerArea,
		InvolvedParty.pagerTel,
		InvolvedParty.otherArea,
		InvolvedParty.otherTel,
		/*isActive*/
		InvolvedParty.dateInserted,
		InvolvedParty.deltaDate
	FROM
		dbo.InvolvedParty
	WHERE
		InvolvedParty.isActive = 1
		AND InvolvedParty.isAliasOfInvolvedPartyId IS NOT NULL
		AND InvolvedParty.isServiceProviderOfInvolvedPartyId IS NOT NULL
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveAliaseServiceProvider_involvedPartyId
	ON dbo.V_ActiveAliaseServiceProvider (involvedPartyId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveAliaseServiceProvider_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveAliaseServiceProvider (isoClaimId, involvedPartySequenceId)
	INCLUDE (isAliasOfInvolvedPartyId, isServiceProviderOfInvolvedPartyId, isBusiness, taxIdentificationNumberObfuscated, taxIdentificationNumberLastFour, socialSecurityNumberObfuscated, socialSecurityNumberLastFour,
		hICNObfuscated, driversLicenseNumberObfuscated, driversLicenseNumberLast3, driversLicenseClass, driversLicenseState, genderCode, passportID, professionalMedicalLicense, isUnderSiuInvestigation, isLawEnforcementAction,
		isReportedToFraudBureau, isFraudReported, dateOfBirth, fullName, firstName, middleName, lastName, suffix, businessArea, businessTel,
		cellArea, cellTel, faxArea, faxTel, homeArea, homeTel, pagerArea, pagerTel, otherArea, otherTel,
		dateInserted, deltaDate
);
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
Description: Maping between InvolvedParty and HighLevel-Claim.
			  This is already a many-to-many, althoug there exists
			  an even lower-level of granularity to be aware of (elemental claim).

			  The thing to keep in mind here is that the non PrimaryKeyValues be
			  functionally dependent upon the entire primary key.
			  
			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.InvolvedPartyAddressMap
(
	involvedPartyAddressRollMapId BIGINT IDENTITY(1,1) NOT NULL,
	claimId BIGINT NOT NULL,
	involvedPartyId BIGINT NOT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	nonLocationOfLossAddressId BIGINT NOT NULL,
	claimRoleCode VARCHAR(2) NOT NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
	CONSTRAINT PK_IPAddressMap_involvedPartyAddressRollMapId
		PRIMARY KEY CLUSTERED (involvedPartyAddressRollMapId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_InvolvedPartyAddressMap_isoClaimId_involvedPartySequenceId
	ON dbo.InvolvedPartyAddressMap (isoClaimId, involvedPartySequenceId)
	INCLUDE(claimId, involvedPartyId, nonLocationOfLossAddressId, claimRoleCode,
		isActive, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW for ElementalClaim(s), filtered
				to only include "active" rows.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveIPAddressMap
WITH SCHEMABINDING
AS
(
	SELECT
		InvolvedPartyAddressMap.involvedPartyAddressRollMapId,
		InvolvedPartyAddressMap.claimId,
		InvolvedPartyAddressMap.involvedPartyId,
		InvolvedPartyAddressMap.isoClaimId,
		InvolvedPartyAddressMap.involvedPartySequenceId,
		InvolvedPartyAddressMap.nonLocationOfLossAddressId,
		InvolvedPartyAddressMap.claimRoleCode,
		/*InvolvedPartyAddressMap.isActive*/
		InvolvedPartyAddressMap.dateInserted,
		InvolvedPartyAddressMap.deltaDate
	FROM
		dbo.InvolvedPartyAddressMap
	WHERE
		InvolvedPartyAddressMap.isActive = 1
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveIPAddressMap_involvedPartyAddressRollMapId
	ON dbo.V_ActiveIPAddressMap (involvedPartyAddressRollMapId);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveIPAddressMap_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveIPAddressMap (isoClaimId, involvedPartySequenceId)
	INCLUDE(claimId, involvedPartyId, nonLocationOfLossAddressId, claimRoleCode,
		dateInserted, deltaDate
	);
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
Description: Most granular level of a claim. Stores data related to
				lossType\CoverageType\Claim\IP\$

			Performance: No current notes.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.ElementalClaim
(   
	elementalClaimId BIGINT IDENTITY(1,1) NOT NULL,
	claimId BIGINT NOT NULL,
	involvedPartyId BIGINT NOT NULL,
	isoClaimId VARCHAR(11) NULL, /*I_ALLCLM*/
	involvedPartySequenceId INT NULL, /*I_NM_ADR*/
	adjusterId BIGINT NULL,
	lossTypeCode CHAR(4) NULL,
	lossTypeDescription VARCHAR(42) NULL,
	coverageTypeCode CHAR(4) NULL,
	coverageTypeDescription VARCHAR(42) NULL,
	dateClaimClosed DATE NULL,
	coverageStatus VARCHAR(3) NULL,
	settlementAmount MONEY NULL,
	estimatedLossAmount MONEY NULL,
	reserveAmount MONEY NULL,
	totalInsuredAmount MONEY NULL,
	policyAmount MONEY NULL,
	replacementAmount MONEY NULL,
	actualCashAmount MONEY NULL,
	buildingPolicyAmount MONEY NULL,
	buildingTotalInsuredAmount MONEY NULL,
	buildingReplacementAmount MONEY NULL,
	buildingActualCashAmount MONEY NULL,
	buildingEstimatedLossAmount MONEY NULL,
	contentPolicyAmount MONEY NULL,
	contentTotalInsuredAmount MONEY NULL,
	contentReplacementAmount MONEY NULL,
	contentActualCashAmount MONEY NULL,
	contentEstimatedLossAmount MONEY NULL,
	stockPolicyAmount MONEY NULL,
	stockTotalInsuredAmount MONEY NULL,
	stockReplacementAmount MONEY NULL,
	stockActualCashAmount MONEY NULL,
	stockEstimatedLossAmount MONEY NULL,
	lossOfUsePolicyAmount MONEY NULL,
	lossOfUseTotalInsuredAmount MONEY NULL,
	lossOfUseReplacementAmount MONEY NULL,
	lossOfUseActualCashAmount MONEY NULL,
	lossOfUseEstimatedLossAmount MONEY NULL,
	otherPolicyAmount MONEY NULL,
	otherTotalInsuredAmount MONEY NULL,
	otherReplacementAmount MONEY NULL,
	otherActualCashAmount MONEY NULL,
	otherEstimatedLossAmount MONEY NULL,
	buildingReserveAmount MONEY NULL,
	buildingPaidAmount MONEY NULL,
	contentReserveAmount MONEY NULL,
	contentPaidAmount MONEY NULL,
	stockReserveAmount MONEY NULL,
	stockPaidAmount MONEY NULL,
	lossOfUseReserve MONEY NULL,
	lossOfUsePaid MONEY NULL,
	otherReserveAmount MONEY NULL,
	otherPaidAmount MONEY NULL,
	isActive BIT NOT NULL,
	dateInserted DATETIME2(0) NOT NULL,
	deltaDate DATETIME2(0) NOT NULL,
	CONSTRAINT PK_ElementalClaim_elementalClaimId
		PRIMARY KEY CLUSTERED (elementalClaimId) 
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ElementalClaim_isoClaimId_involvedPartySequenceId
	ON dbo.ElementalClaim (isoClaimId, involvedPartySequenceId)
	INCLUDE (claimId, involvedPartyId, adjusterId, lossTypeCode, lossTypeDescription, coverageTypeCode, coverageTypeDescription, dateClaimClosed,
		coverageStatus, settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount, policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount, buildingTotalInsuredAmount,
		buildingReplacementAmount, buildingActualCashAmount, buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount, contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount, stockPolicyAmount, stockTotalInsuredAmount,
		stockReplacementAmount, stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount, lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount, lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
		otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount, buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount, stockReserveAmount, stockPaidAmount, lossOfUseReserve,
		lossOfUsePaid, otherReserveAmount, otherPaidAmount, isActive, dateInserted, deltaDate
	);
CREATE NONCLUSTERED INDEX NIX_ElementalClaim_claimId_involvedPartyId_adjusterId
	ON dbo.ElementalClaim (claimId, involvedPartyId, adjusterId)
	INCLUDE (isoClaimId, involvedPartySequenceId, lossTypeCode, lossTypeDescription, coverageTypeCode, coverageTypeDescription, dateClaimClosed,
		coverageStatus, settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount, policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount, buildingTotalInsuredAmount,
		buildingReplacementAmount, buildingActualCashAmount, buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount, contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount, stockPolicyAmount, stockTotalInsuredAmount,
		stockReplacementAmount, stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount, lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount, lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
		otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount, buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount, stockReserveAmount, stockPaidAmount, lossOfUseReserve,
		lossOfUsePaid, otherReserveAmount, otherPaidAmount, isActive, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2019-01-07
Author: Robert David Warner
Description: INDEXED VIEW for ElementalClaim(s), filtered
				to only include "active" rows.
***********************************************
WorkItem: ISCCINTEL-3544
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.

			Performance: No current notes.
************************************************/
CREATE VIEW dbo.V_ActiveElementalClaim
WITH SCHEMABINDING
AS
(
	SELECT
		ElementalClaim.elementalClaimId,
		ElementalClaim.claimId,
		ElementalClaim.involvedPartyId,
		ElementalClaim.isoClaimId,
		ElementalClaim.involvedPartySequenceId,
		ElementalClaim.adjusterId,
		ElementalClaim.lossTypeCode,
		ElementalClaim.lossTypeDescription,
		ElementalClaim.coverageTypeCode,
		ElementalClaim.coverageTypeDescription,
		ElementalClaim.dateClaimClosed,
		ElementalClaim.coverageStatus,
		ElementalClaim.settlementAmount,
		ElementalClaim.estimatedLossAmount,
		ElementalClaim.reserveAmount,
		ElementalClaim.totalInsuredAmount,
		ElementalClaim.policyAmount,
		ElementalClaim.replacementAmount,
		ElementalClaim.actualCashAmount,
		ElementalClaim.buildingPolicyAmount,
		ElementalClaim.buildingTotalInsuredAmount,
		ElementalClaim.buildingReplacementAmount,
		ElementalClaim.buildingActualCashAmount,
		ElementalClaim.buildingEstimatedLossAmount,
		ElementalClaim.contentPolicyAmount,
		ElementalClaim.contentTotalInsuredAmount,
		ElementalClaim.contentReplacementAmount,
		ElementalClaim.contentActualCashAmount,
		ElementalClaim.contentEstimatedLossAmount,
		ElementalClaim.stockPolicyAmount,
		ElementalClaim.stockTotalInsuredAmount,
		ElementalClaim.stockReplacementAmount,
		ElementalClaim.stockActualCashAmount,
		ElementalClaim.stockEstimatedLossAmount,
		ElementalClaim.lossOfUsePolicyAmount,
		ElementalClaim.lossOfUseTotalInsuredAmount,
		ElementalClaim.lossOfUseReplacementAmount,
		ElementalClaim.lossOfUseActualCashAmount,
		ElementalClaim.lossOfUseEstimatedLossAmount,
		ElementalClaim.otherPolicyAmount,
		ElementalClaim.otherTotalInsuredAmount,
		ElementalClaim.otherReplacementAmount,
		ElementalClaim.otherActualCashAmount,
		ElementalClaim.otherEstimatedLossAmount,
		ElementalClaim.buildingReserveAmount,
		ElementalClaim.buildingPaidAmount,
		ElementalClaim.contentReserveAmount,
		ElementalClaim.contentPaidAmount,
		ElementalClaim.stockReserveAmount,
		ElementalClaim.stockPaidAmount,
		ElementalClaim.lossOfUseReserve,
		ElementalClaim.lossOfUsePaid,
		ElementalClaim.otherReserveAmount,
		ElementalClaim.otherPaidAmount,
		/*ElementalClaim.isActive,*/
		ElementalClaim.dateInserted,
		ElementalClaim.deltaDate
	FROM
		dbo.ElementalClaim
	WHERE
		ElementalClaim.isActive = 1
)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE UNIQUE CLUSTERED INDEX PK_ActiveElementalClaim_elementalClaimId
	ON dbo.V_ActiveElementalClaim (elementalClaimId)
	WITH (FILLFACTOR = 80);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
CREATE NONCLUSTERED INDEX NIX_ActiveElementalClaim_isoClaimId_involvedPartySequenceId
	ON dbo.V_ActiveElementalClaim (isoClaimId, involvedPartySequenceId)
	INCLUDE (claimId, involvedPartyId, adjusterId, lossTypeCode, lossTypeDescription, coverageTypeCode, coverageTypeDescription, dateClaimClosed,
		coverageStatus, settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount, policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount, buildingTotalInsuredAmount,
		buildingReplacementAmount, buildingActualCashAmount, buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount, contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount, stockPolicyAmount, stockTotalInsuredAmount,
		stockReplacementAmount, stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount, lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount, lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
		otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount, buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount, stockReserveAmount, stockPaidAmount, lossOfUseReserve,
		lossOfUsePaid, otherReserveAmount, otherPaidAmount, dateInserted, deltaDate
	);
CREATE NONCLUSTERED INDEX NIX_ActiveElementalClaim_claimId_involvedPartyId_adjusterId
	ON dbo.V_ActiveElementalClaim (claimId, involvedPartyId, adjusterId)
	INCLUDE (isoClaimId, involvedPartySequenceId, lossTypeCode, lossTypeDescription, coverageTypeCode, coverageTypeDescription, dateClaimClosed,
		coverageStatus, settlementAmount, estimatedLossAmount, reserveAmount, totalInsuredAmount, policyAmount, replacementAmount, actualCashAmount, buildingPolicyAmount, buildingTotalInsuredAmount,
		buildingReplacementAmount, buildingActualCashAmount, buildingEstimatedLossAmount, contentPolicyAmount, contentTotalInsuredAmount, contentReplacementAmount, contentActualCashAmount, contentEstimatedLossAmount, stockPolicyAmount, stockTotalInsuredAmount,
		stockReplacementAmount, stockActualCashAmount, stockEstimatedLossAmount, lossOfUsePolicyAmount, lossOfUseTotalInsuredAmount, lossOfUseReplacementAmount, lossOfUseActualCashAmount, lossOfUseEstimatedLossAmount, otherPolicyAmount, otherTotalInsuredAmount,
		otherReplacementAmount, otherActualCashAmount, otherEstimatedLossAmount, buildingReserveAmount, buildingPaidAmount, contentReserveAmount, contentPaidAmount, stockReserveAmount, stockPaidAmount, lossOfUseReserve,
		lossOfUsePaid, otherReserveAmount, otherPaidAmount, dateInserted, deltaDate
	);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

DECLARE @dateInserted DATETIME2(0) = GETDATE();

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
	AND FireMarshalController.fmStateCode = 'MS'
	AND FireMarshalController.fmStateStatusCode <> 'P';
;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO







--exec sp_help 'dbo.V_ActiveClaim'
--exec sp_help 'dbo.Policy'
--exec sp_help 'dbo.InvolvedParty'
--exec sp_help 'dbo.V_ActiveInvolvedParty'
--exec sp_help 'dbo.V_ActiveAliaseInvolvedParty'
--exec sp_help 'dbo.V_ActiveNonAliaseServiceProvider'
--exec sp_help 'dbo.V_ActiveAliaseServiceProvider'
--exec sp_help 'dbo.InvolvedPartyAddressMap'
--exec sp_help 'dbo.V_ActiveIPAddressMap'
--exec sp_help 'dbo.ElementalClaim'
--exec sp_help 'dbo.V_ActiveElementalClaim'
--exec sp_help 'dbo.FireMarshalPendingClaim'
--exec sp_help 'dbo.V_ActiveCurrentPendingFMClaim'


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