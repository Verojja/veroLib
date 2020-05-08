SET NOCOUNT ON;
/*!!!!TODO:
	1.) update header section to be driven off of dbo.FireMarshalContactList
	2.) Possible Performance tune improvement possible from replacing DeclareAndInsertIntoTempTable with SelectInto syntax (specifically, around log minimalization).
	3.) Wrap as sproc
*/
BEGIN TRANSACTION
/*******Variable Declaration and initialization******/
DECLARE
	@flatFileContent VARCHAR(MAX),
	@date DATE = GETDATE(),
	@startOfMonth DATE =
		DATEADD(
			MONTH,
			DATEDIFF(
				MONTH,
				0,
				GETDATE()
			),
			0
		),
	@eoMonth DATE =
		DATEADD(
			SECOND,
			-1,
			DATEADD(
				MONTH,
				DATEDIFF(
					MONTH,
					0,
					GETDATE()
				) +1,
				0
			)
		),
	@headerSpaceOne CHAR(36) = '                                    ',
	@headerSpaceTwo CHAR(1) = ' ',
	
	@newLine CHAR(1) = CHAR(13),
	@formatedHeaderDate VARCHAR(23),
	
	@senderOrganizationName VARCHAR(200) = 'ISO Claimsearch',
	@senderOrganizationNameTwo VARCHAR(200) = 'ClaimSearch Operations',
	@senderOrganizationAddressLineOne VARCHAR(200) = '545 Washington Blvd  22-8',
	@senderOrganizationAddressLineTwo VARCHAR(200) = 'Jersey City, NJ 07310-1636',
	
	@senderFullName VARCHAR(50) = 'Carlos Martins, SCLA',
	@senderTitle VARCHAR(50) = 'Vice President',
	@senderContactPhoneNumber VARCHAR(20) = '(201) 469-3103',
	
	/*Possible to simply declare and populate at another time*/
	@recipientFirstName VARCHAR(50) = '0MR. GALE',
	@recipientLastName VARCHAR(50) = 'HAAG',
	@recipientHonorific VARCHAR(10) = 'MR.',
	@recipientTitle VARCHAR(200) = 'STATE FIRE MARSHAL',
	@recipientAddressLineOne VARCHAR(200) = '700 SW JACKSON',
	@recipientAddressLineTwo VARCHAR(200) = 'SUITE 600',
	@recipientAddressLineThree VARCHAR(200) = 'TOPEKA, KS 66603-3714'
	--@recipientFirstName VARCHAR(50) = NULL,
	--@recipientLastName VARCHAR(50) = NULL,
	--@recipientHonorific VARCHAR(10) = NULL,
	--@recipientTitle VARCHAR(200) = NULL,
	--@recipientAddressLineOne VARCHAR(200) = NULL,
	--@recipientAddressLineTwo VARCHAR(200) = NULL,
	--@recipientAddressLineThree VARCHAR(200) = NULL
	;
	
SELECT
	@formatedHeaderDate =
		CASE
			WHEN MONTH(@date) = 1 THEN 'January'
			WHEN MONTH(@date) = 2 THEN 'February'
			WHEN MONTH(@date) = 3 THEN 'March'
			WHEN MONTH(@date) = 4 THEN 'April'
			WHEN MONTH(@date) = 5 THEN 'May'
			WHEN MONTH(@date) = 6 THEN 'June'
			WHEN MONTH(@date) = 7 THEN 'July'
			WHEN MONTH(@date) = 8 THEN 'August'
			WHEN MONTH(@date) = 9 THEN 'September'
			WHEN MONTH(@date) = 10 THEN 'October'
			WHEN MONTH(@date) = 11 THEN 'November'
			ELSE 'December'
		END + '   ' + SUBSTRING(CAST(@eoMonth AS CHAR(10)),6,2) +', ' + SUBSTRING(CAST(@eoMonth AS CHAR(10)),1,4);

CREATE TABLE #TempFMDataTableClaims
(
	detailsClaimType VARCHAR(22) NULL,
	detailsClaimDate DATE NULL,
	detailsFileNumber CHAR(11) NOT NULL,
	detailsLossType VARCHAR(38) NULL,
	detailsCauseOfLossDesc VARCHAR(51) NULL,
	detailsTypeOfPropertyDesc VARCHAR(51) NULL,
	detailsLocationOfLossStreet VARCHAR(26) NULL,
	detailsLocationOfLossCity VARCHAR(26) NULL,
	detailsLocationOfLossStateCode CHAR(2) NULL,
	detailsCompanyName VARCHAR(43) NULL,
	detailsCompanyPhone CHAR(10) NULL,
	detailsContactName VARCHAR(43) NULL,
	detailsContactPhone CHAR(10) NULL,
	detailsAdjCompanyName VARCHAR(27) NULL,
	detailsAdjCompanyAddress VARCHAR(26) NULL,
	detailsAdjCompanyCity CHAR(26) NULL,
	detailsAdjCompanyStateCode CHAR(2) NULL,
	detailsAdjCompanyZip CHAR(5) NULL,
	detailsClaimNumber VARCHAR(40) NULL,
	detailsPolicyNumber VARCHAR(40) NULL
);

CREATE TABLE #TempFMDataTableInvolvedParty
(
	detailsClaimNumber CHAR(11) NOT NULL,
	involvedPartyID INT NOT NULL,
	detailsInvolvedPartyDescription VARCHAR(50) NULL,
	detailsInvolvedPartyName VARCHAR(100) NULL,
	detailsInvolvedPartyAddress VARCHAR(26) NULL,
	detailsInvolvedPartyCity VARCHAR(21) NULL,
	detailsInvolvedPartyStateCode CHAR(2) NULL,
	detailsInvolvedPartyDOB CHAR(10) NULL,
	detailsInvolvedPartySSN CHAR(9) NULL,
	detailsInvolvedPartySSNIssuedNote VARCHAR(31) NULL,
	detailsInvolvedPartyPhone CHAR(10) NULL,
	detailsIsIncendiaryFire VARCHAR(3) NULL,
	detailsIsUnderSIUInvestigation VARCHAR(3) NULL
);

CREATE TABLE #TempFMDataTableCoverage
(
	detailsClaimNumber CHAR(11) NOT NULL,
	involvedPartyID INT NOT NULL,
	detailsCoverageDescription VARCHAR(100) NULL,
	coverageDetailsPolicyAmountBuild INT NULL,
	coverageDetailsPolicyAmountContents INT NULL,
	coverageDetailsPolicyAmountStock INT NULL,
	coverageDetailsPolicyAmountOccupancy INT NULL,
	coverageDetailsPolicyAmountOther INT NULL,
	coverageDetailsEstimatedLossBuild INT NULL,
	coverageDetailsEstimatedLossContents INT NULL,
	coverageDetailsEstimatedLossStock INT NULL,
	coverageDetailsEstimatedLossOccupancy INT NULL,
	coverageDetailsEstimatedLossOther INT NULL,
	coverageDetailsSettlementAmountBuild INT NULL,
	coverageDetailsSettlementAmountContents INT NULL,
	coverageDetailsSettlementAmountStock INT NULL,
	coverageDetailsSettlementAmountOccupancy INT NULL,
	coverageDetailsSettlementAmountOther INT NULL,
	coverageDetailsVin VARCHAR(20) NULL,
	coverageDetailsVehicleYear CHAR(4) NULL,
	coverageDetailsVehicleStyle CHAR(2) NULL,
	coverageDetailsVehicleMake VARCHAR(35) NULL,
	coverageDetailsVehicleModel VARCHAR(35) NULL,
	coverageDetailsVehicleType CHAR(2) NULL,
	coverageDetailsLicencePlate VARCHAR(10) NULL,
	coverageDetailsLicenceState CHAR(2),
	coverageDetailsLicenceYear CHAR(4),
	coverageDetailsLicenceType VARCHAR(2)
);

INSERT INTO #TempFMDataTableClaims
(
	detailsClaimType,
	detailsClaimDate,
	detailsFileNumber,
	detailsLossType ,
	detailsCauseOfLossDesc,
	detailsTypeOfPropertyDesc,
	detailsLocationOfLossStreet,
	detailsLocationOfLossCity,
	detailsLocationOfLossStateCode,
	detailsCompanyName,
	detailsCompanyPhone,
	detailsContactName,
	detailsContactPhone,
	detailsAdjCompanyName,
	detailsAdjCompanyAddress,
	detailsAdjCompanyCity,
	detailsAdjCompanyStateCode,
	detailsAdjCompanyZip,
	detailsClaimNumber,
	detailsPolicyNumber
)
VALUES
	(
		/*detailsClaimType*/'UF CLAIM',
		/*detailsClaimDate*/'2018-10-11',
		/*detailsFileNumber*/'1G004770520',
		/*detailsLossType */'Personal Automobile',
		/*detailsCauseOfLossDesc*/'FIRE IV',
		/*detailsTypeOfPropertyDesc*/NULL,
		/*detailsLocationOfLossStreet*/'1354 TAYLOR ROAD',
		/*detailsLocationOfLossCity*/'INDEPENDENCE',
		/*detailsLocationOfLossStateCode*/'KS',
		/*detailsCompanyName*/'ESURANCE PROPERTY & CASUALTY INS CO',
		/*detailsCompanyPhone*/'8003437262',
		/*detailsContactName*/'BLANCO,ATHENA',
		/*detailsContactPhone*/'8003437262',
		/*detailsAdjCompanyName*/'ROCKLIN OFC #2',
		/*detailsAdjCompanyAddress*/'PO BOX 2890',
		/*detailsAdjCompanyCity*/'ROCKLIN',
		/*detailsAdjCompanyStateCode*/'CA',
		/*detailsAdjCompanyZip*/'95677',
		/*detailsClaimNumber*/'TXA0214767',
		/*detailsPolicyNumber*/'PAKS006689246'
	),
	(
		/*detailsClaimType*/'UF CLAIM',
		/*detailsClaimDate*/'2018-10-13',
		/*detailsFileNumber*/'1C004779861',
		/*detailsLossType */'Personal Property Homeowners',
		/*detailsCauseOfLossDesc*/'FIRE UNKNOWN ORIGIN IN SW CORNER BEDROOM OF THE IN',
		/*detailsTypeOfPropertyDesc*/NULL,
		/*detailsLocationOfLossStreet*/'5104 W 148TH ST',
		/*detailsLocationOfLossCity*/'LEAWOOD',
		/*detailsLocationOfLossStateCode*/'KS',
		/*detailsCompanyName*/'AMERICAN FAMILY MUTUAL INSURANCE COMPANY',
		/*detailsCompanyPhone*/'6082424100',
		/*detailsContactName*/NULL,
		/*detailsContactPhone*/NULL,
		/*detailsAdjCompanyName*/NULL,
		/*detailsAdjCompanyAddress*/NULL,
		/*detailsAdjCompanyCity*/NULL,
		/*detailsAdjCompanyStateCode*/NULL,
		/*detailsAdjCompanyZip*/NULL,
		/*detailsClaimNumber*/'00825105402',
		/*detailsPolicyNumber*/'15DD508001'
	)
	/*
		,(
			/*detailsClaimType*/'UF CLAIM',
			/*detailsClaimDate*/'2018-10-15',
			/*detailsFileNumber*/'3P004778015',
			/*detailsLossType */'Personal Property Homeowners',
			/*detailsCauseOfLossDesc*/'SPOKE WITH: BEVERLY CONFIRMED PROPERTY AND MAILING',
			/*detailsTypeOfPropertyDesc*/NULL,
			/*detailsLocationOfLossStreet*/'404 W 12TH ST N',
			/*detailsLocationOfLossCity*/'WICHITA',
			/*detailsLocationOfLossStateCode*/'KS',
			/*detailsCompanyName*/'AMERICAN FAMILY HOME INSURANCE CO',
			/*detailsCompanyPhone*/NULL,
			/*detailsContactName*/'WULKER,TAYLOR',
			/*detailsContactPhone*/NULL,
			/*detailsAdjCompanyName*/'XML',
			/*detailsAdjCompanyAddress*/'XML',
			/*detailsAdjCompanyCity*/'CINCINNATI',
			/*detailsAdjCompanyStateCode*/'OH',
			/*detailsAdjCompanyZip*/'45201',
			/*detailsClaimNumber*/'480943AAEXP1',
			/*detailsPolicyNumber*/'0048531107'
		)
	*/
	;

INSERT INTO #TempFMDataTableInvolvedParty
(
	detailsClaimNumber,
	involvedPartyID,
	detailsInvolvedPartyDescription,
	detailsInvolvedPartyName,
	detailsInvolvedPartyAddress,
	detailsInvolvedPartyCity,
	detailsInvolvedPartyStateCode,
	detailsInvolvedPartyDOB,
	detailsInvolvedPartySSN,
	detailsInvolvedPartySSNIssuedNote,
	detailsInvolvedPartyPhone,
	detailsIsIncendiaryFire,
	detailsIsUnderSIUInvestigation
)
VALUES
	(
		/*detailsFileNumber*/'1G004770520',
		/*involvedPartyID*/1,
		/*detailsInvolvedPartyDescription*/'Both Claimant & Insured',
		/*detailsInvolvedPartyName*/'BLAISDELL,VICKI',
		/*detailsInvolvedPartyAddress*/'912 N PENN AVE',
		/*detailsInvolvedPartyCity*/'INDEPENDENCE',
		/*detailsInvolvedPartyStateCode*/'KS',
		/*detailsInvolvedPartyDOB*/'XX/XX/1949',
		/*detailsInvolvedPartySSN*/'XXXXX8299',
		/*detailsInvolvedPartySSNIssuedNote*/NULL,
		/*detailsInvolvedPartyPhone*/'6207792195',
		/*detailsIsIncendiaryFire*/NULL,
		/*detailsIsUnderSIUInvestigation*/NULL
	),
	(
		/*detailsFileNumber*/'1C004779861',
		/*involvedPartyID*/1,
		/*detailsInvolvedPartyDescription*/'Both Claimant & Insured',
		/*detailsInvolvedPartyName*/'KESSLER,ALAN Z & DEBRA',
		/*detailsInvolvedPartyAddress*/'5104 W 148TH ST',
		/*detailsInvolvedPartyCity*/'LEAWOOD',
		/*detailsInvolvedPartyStateCode*/'KS',
		/*detailsInvolvedPartyDOB*/NULL,
		/*detailsInvolvedPartySSN*/NULL,
		/*detailsInvolvedPartySSNIssuedNote*/NULL,
		/*detailsInvolvedPartyPhone*/NULL,
		/*detailsIsIncendiaryFire*/NULL,
		/*detailsIsUnderSIUInvestigation*/NULL
	)
	/*
		,(
			/*detailsFileNumber*/'3P004778015',
			/*involvedPartyID*/1,
			/*detailsInvolvedPartyDescription*/'Insured',
			/*detailsInvolvedPartyName*/'DANLEY,BEVERLY M',
			/*detailsInvolvedPartyAddress*/'1301 N WACO AVE',
			/*detailsInvolvedPartyCity*/'WICHITA',
			/*detailsInvolvedPartyStateCode*/'KS',
			/*detailsInvolvedPartyDOB*/'XX/XX/1947',
			/*detailsInvolvedPartySSN*/'000009974',
			/*detailsInvolvedPartySSNIssuedNote*/'(SSN ISSUED       KS/1963-1963)',
			/*detailsInvolvedPartyPhone*/NULL,
			/*detailsIsIncendiaryFire*/NULL,
			/*detailsIsUnderSIUInvestigation*/NULL
		)
	*/
	;

INSERT INTO #TempFMDataTableCoverage
(
	detailsClaimNumber,
	involvedPartyID,
	detailsCoverageDescription,
	coverageDetailsPolicyAmountBuild,
	coverageDetailsPolicyAmountContents,
	coverageDetailsPolicyAmountStock,
	coverageDetailsPolicyAmountOccupancy,
	coverageDetailsPolicyAmountOther,
	coverageDetailsEstimatedLossBuild,
	coverageDetailsEstimatedLossContents,
	coverageDetailsEstimatedLossStock,
	coverageDetailsEstimatedLossOccupancy,
	coverageDetailsEstimatedLossOther,
	coverageDetailsSettlementAmountBuild,
	coverageDetailsSettlementAmountContents,
	coverageDetailsSettlementAmountStock,
	coverageDetailsSettlementAmountOccupancy,
	coverageDetailsSettlementAmountOther,
	coverageDetailsVin,
	coverageDetailsVehicleYear,
	coverageDetailsVehicleStyle,
	coverageDetailsVehicleMake,
	coverageDetailsVehicleModel,
	coverageDetailsVehicleType,
	coverageDetailsLicencePlate,
	coverageDetailsLicenceState,
	coverageDetailsLicenceYear,
	coverageDetailsLicenceType
)
VALUES
	(
		/*detailsClaimNumber*/'1G004770520',
		/*involvedPartyID*/1,
		/*detailsCoverageDescription*/ 'Comprehensive',
		/*coverageDetailsPolicyAmountBuild*/ NULL,
		/*coverageDetailsPolicyAmountContents*/ NULL,
		/*coverageDetailsPolicyAmountStock*/ NULL,
		/*coverageDetailsPolicyAmountOccupancy*/ NULL,
		/*coverageDetailsPolicyAmountOther*/ 190000,
		/*coverageDetailsEstimatedLossBuild*/ NULL,
		/*coverageDetailsEstimatedLossContents*/ NULL,
		/*coverageDetailsEstimatedLossStock*/ NULL,
		/*coverageDetailsEstimatedLossOccupancy*/ NULL,
		/*coverageDetailsEstimatedLossOther*/ 1160,
		/*coverageDetailsSettlementAmountBuild*/ NULL,
		/*coverageDetailsSettlementAmountContents*/ NULL,
		/*coverageDetailsSettlementAmountStock*/ NULL,
		/*coverageDetailsSettlementAmountOccupancy*/ NULL,
		/*coverageDetailsSettlementAmountOther*/ NULL,
		/*coverageDetailsVin*/ '1GCEC19T2YE103516',
		/*coverageDetailsVehicleYear*/ '2000',
		/*coverageDetailsVehicleStyle*/ NULL,
		/*coverageDetailsVehicleMake*/ 'CHEVROLET',
		/*coverageDetailsVehicleModel*/ 'SILVERADO 1',
		/*coverageDetailsVehicleType*/ NULL,
		/*coverageDetailsLicencePlate*/ NULL,
		/*coverageDetailsLicenceState*/ 'KS',
		/*coverageDetailsLicenceYear*/ NULL,
		/*coverageDetailsLicenceType*/ NULL
	),
	(
		/*detailsClaimNumber*/'1C004779861',
		/*involvedPartyID*/1,
		/*detailsCoverageDescription*/ 'Property',
		/*coverageDetailsPolicyAmountBuild*/  1075140,
		/*coverageDetailsPolicyAmountContents*/ NULL,
		/*coverageDetailsPolicyAmountStock*/ NULL,
		/*coverageDetailsPolicyAmountOccupancy*/ NULL,
		/*coverageDetailsPolicyAmountOther*/ NULL,
		/*coverageDetailsEstimatedLossBuild*/ 1175000,
		/*coverageDetailsEstimatedLossContents*/ NULL,
		/*coverageDetailsEstimatedLossStock*/ NULL,
		/*coverageDetailsEstimatedLossOccupancy*/ NULL,
		/*coverageDetailsEstimatedLossOther*/ NULL,
		/*coverageDetailsSettlementAmountBuild*/ 5000,
		/*coverageDetailsSettlementAmountContents*/ NULL,
		/*coverageDetailsSettlementAmountStock*/ NULL,
		/*coverageDetailsSettlementAmountOccupancy*/ NULL,
		/*coverageDetailsSettlementAmountOther*/ NULL,
		/*coverageDetailsVin*/ NULL,
		/*coverageDetailsVehicleYear*/ NULL,
		/*coverageDetailsVehicleStyle*/ NULL,
		/*coverageDetailsVehicleMake*/ NULL,
		/*coverageDetailsVehicleModel*/ NULL,
		/*coverageDetailsVehicleType*/ NULL,
		/*coverageDetailsLicencePlate*/ NULL,
		/*coverageDetailsLicenceState*/ NULL,
		/*coverageDetailsLicenceYear*/ NULL,
		/*coverageDetailsLicenceType*/ NULL
	)
	/*
		,(
			/*detailsClaimNumber*/'3P004778015',
			/*involvedPartyID*/1,
			/*detailsCoverageDescription*/ 'Property',
			/*coverageDetailsPolicyAmountBuild*/ 30000,
			/*coverageDetailsPolicyAmountContents*/ NULL,
			/*coverageDetailsPolicyAmountStock*/ NULL,
			/*coverageDetailsPolicyAmountOccupancy*/ NULL,
			/*coverageDetailsPolicyAmountOther*/ NULL,
			/*coverageDetailsEstimatedLossBuild*/ 42500730,
			/*coverageDetailsEstimatedLossContents*/ NULL,
			/*coverageDetailsEstimatedLossStock*/ NULL,
			/*coverageDetailsEstimatedLossOccupancy*/ NULL,
			/*coverageDetailsEstimatedLossOther*/ NULL,
			/*coverageDetailsSettlementAmountBuild*/ NULL,
			/*coverageDetailsSettlementAmountContents*/ NULL,
			/*coverageDetailsSettlementAmountStock*/ NULL,
			/*coverageDetailsSettlementAmountOccupancy*/ NULL,
			/*coverageDetailsSettlementAmountOther*/ NULL,
			/*coverageDetailsVin*/ NULL,
			/*coverageDetailsVehicleYear*/ NULL,
			/*coverageDetailsVehicleStyle*/ NULL,
			/*coverageDetailsVehicleMake*/ NULL,
			/*coverageDetailsVehicleModel*/ NULL,
			/*coverageDetailsVehicleType*/ NULL,
			/*coverageDetailsLicencePlate*/ NULL,
			/*coverageDetailsLicenceState*/ NULL,
			/*coverageDetailsLicenceYear*/ NULL,
			/*coverageDetailsLicenceType*/ NULL
		)
	*/
	;

DECLARE
	@detailsClaimType VARCHAR(22) = NULL,
	@detailsClaimTypeFormatted VARCHAR(22) = '                      ',
	@detailsClaimDate DATE = NULL,
	@detailsFileNumber CHAR(11) = NULL,
	@detailsLossType VARCHAR(38) = NULL,
	@detailsLossTypeFormatted CHAR(38) = '                                  FIRE',
	@detailsCauseOfLossDesc VARCHAR(51) = NULL,
	@detailsTypeOfPropertyDesc VARCHAR(51) = NULL,
	@detailsLocationOfLossStreet VARCHAR(26) = NULL,
	@detailsLocationOfLossCity VARCHAR(26) = NULL,
	@detailsLocationOfLossCityFormatted CHAR(26) = NULL,
	@detailsLocationOfLossStateCode CHAR(2),
	@detailsCompanyName VARCHAR(43),
	@detailsCompanyNameFormatted CHAR(43) = '                                           ',
	@detailsCompanyPhone CHAR(10),
	@detailsContactName VARCHAR(43),
	@detailsContactNameFormatted CHAR(43) = '                                           ',
	@detailsContactPhone CHAR(10),
	@detailsAdjCompanyName VARCHAR(27),
	@detailsAdjCompanyAddress VARCHAR(26) = NULL,
	@detailsAdjCompanyCity VARCHAR(26) = NULL,
	@detailsAdjCompanyCityFormatted CHAR(26) = NULL,
	@detailsAdjCompanyStateCode CHAR(2) = NULL,
	@detailsAdjCompanyZip CHAR(5) = NULL,
	@detailsClaimNumber VARCHAR(40) = NULL,
	@detailsPolicyNumber VARCHAR(40) = NULL,
	
	@detailsInvolvedPartyDescription VARCHAR(50) = NULL,
	@involvedPartyID INT = NULL,
	@detailsInvolvedPartyName VARCHAR(100) = NULL,
	@detailsInvolvedPartyAddress VARCHAR(26) = NULL,
	@detailsInvolvedPartyAddressFormatted CHAR(26) = NULL,
	@detailsInvolvedPartyCity VARCHAR(21) = NULL,
	@detailsInvolvedPartyCityFormatted CHAR(21) = NULL,
	@detailsInvolvedPartyStateCode CHAR(2) = NULL,
	@detailsInvolvedPartyDOB CHAR(10) = NULL,
	@detailsInvolvedPartySSN CHAR(9) = NULL,
	@detailsInvolvedPartySSNIssuedNote VARCHAR(31) = NULL,
	@detailsInvolvedPartyPhone CHAR(10) = NULL,
	@detailsIsIncendiaryFire VARCHAR(3) = NULL,
	@detailsIsUnderSIUInvestigation VARCHAR(3) = NULL,
	
	@detailsCoverageDescription VARCHAR(100) = NULL,
	@coverageDetailsPolicyAmountBuild INT = NULL,
	@coverageDetailsPolicyAmountBuildFormatted VARCHAR(15) = NULL,
	@coverageDetailsPolicyAmountContents INT = NULL,
	@coverageDetailsPolicyAmountContentsFormatted VARCHAR(15) = NULL,
	@coverageDetailsPolicyAmountStock INT = NULL,
	@coverageDetailsPolicyAmountStockFormatted VARCHAR(15) = NULL,
	@coverageDetailsPolicyAmountOccupancy INT = NULL,
	@coverageDetailsPolicyAmountOccupancyFormatted VARCHAR(15) = NULL,
	@coverageDetailsPolicyAmountOther INT = NULL,
	@coverageDetailsPolicyAmountOtherFormatted VARCHAR(15) = NULL,
	@coverageDetailsEstimatedLossBuild INT = NULL,
	@coverageDetailsEstimatedLossBuildFormatted VARCHAR(15) = NULL,
	@coverageDetailsEstimatedLossContents INT = NULL,
	@coverageDetailsEstimatedLossContentsFormatted VARCHAR(15) = NULL,
	@coverageDetailsEstimatedLossStock INT = NULL,
	@coverageDetailsEstimatedLossStockFormatted VARCHAR(15) = NULL,
	@coverageDetailsEstimatedLossOccupancy INT = NULL,
	@coverageDetailsEstimatedLossOccupancyFormatted VARCHAR(15) = NULL,
	@coverageDetailsEstimatedLossOther INT = NULL,
	@coverageDetailsEstimatedLossOtherFormatted VARCHAR(15) = NULL,
	@coverageDetailsSettlementAmountBuild INT = NULL,
	@coverageDetailsSettlementAmountBuildFormatted VARCHAR(15) = NULL,
	@coverageDetailsSettlementAmountContents INT = NULL,
	@coverageDetailsSettlementAmountContentsFormatted VARCHAR(15) = NULL,
	@coverageDetailsSettlementAmountStock INT = NULL,
	@coverageDetailsSettlementAmountStockFormatted VARCHAR(15) = NULL,
	@coverageDetailsSettlementAmountOccupancy INT = NULL,
	@coverageDetailsSettlementAmountOccupancyFormatted VARCHAR(15) = NULL,
	@coverageDetailsSettlementAmountOther INT = NULL,
	@coverageDetailsSettlementAmountOtherFormatted VARCHAR(15) = NULL,
	
	@coverageDetailsVin VARCHAR(20) = NULL,
	@coverageDetailsVehicleYear CHAR(4) = NULL,
	@coverageDetailsVehicleStyle CHAR(2) = NULL,
	@coverageDetailsVehicleMake VARCHAR(35) = NULL,
	@coverageDetailsVehicleModel VARCHAR(35) = NULL,
	@coverageDetailsVehicleType CHAR(2) = NULL,
	@coverageDetailsLicencePlate VARCHAR(10) = NULL,
	@coverageDetailsLicenceState CHAR(2) = NULL,
	@coverageDetailsLicenceYear CHAR(4) = NULL,
	@coverageDetailsLicenceType VARCHAR(2) = NULL;

/*****************Content Generation*****************/
----------/*Header Section*/----------
SELECT
	@flatFileContent = STUFF(@headerSpaceOne, 1, 1, '1') + @formatedHeaderDate + @newLine +
	+ @newLine +
	@headerSpaceOne + @senderOrganizationName + @newLine +
	@headerSpaceOne + @senderOrganizationAddressLineOne + @newLine +
	@headerSpaceOne + @senderOrganizationAddressLineTwo + @newLine +
	@recipientFirstName + ' ' + @recipientLastName + @newLine +
	@headerSpaceTwo + @recipientTitle + @newLine +
	@headerSpaceTwo + @recipientAddressLineOne + @newLine +
	CASE
		WHEN NULLIF(LTRIM(RTRIM(@recipientAddressLineTwo)),'') IS NOT NULL
		THEN
			@headerSpaceTwo + @recipientAddressLineTwo + @newLine
		ELSE ''
	END +
	CASE
		WHEN NULLIF(LTRIM(RTRIM(@recipientAddressLineThree)),'') IS NOT NULL
		THEN
			@headerSpaceTwo + @recipientAddressLineThree + @newLine
		ELSE ''
	END +
	@headerSpaceTwo + 'DEAR ' + @recipientHonorific + ' ' + @recipientLastName + ':' + @newLine +
	@headerSpaceTwo + 'Enclosed are reports processed between ' + CAST(@startOfMonth AS CHAR(10)) + ' AND ' + CAST(@eoMonth AS CHAR(10)) + '.' + @newLine +
	@headerSpaceOne + 'Very Truly Yours,' + @newLine +
	STUFF(@headerSpaceOne, 1, 1, '0') + @senderFullName + @newLine +
	@headerSpaceOne + @senderTitle + @newLine +
	@headerSpaceOne + @senderOrganizationNameTwo + @newLine +
	@headerSpaceOne + @senderContactPhoneNumber + @newLine;
----------/*Claim level*/----------
DECLARE CU_FireClaimCursor CURSOR
FOR SELECT
	detailsClaimType,
	detailsClaimDate,
	detailsFileNumber,
	detailsLossType ,
	detailsCauseOfLossDesc,
	detailsTypeOfPropertyDesc,
	detailsLocationOfLossStreet,
	detailsLocationOfLossCity,
	detailsLocationOfLossStateCode,
	detailsCompanyName,
	detailsCompanyPhone,
	detailsContactName,
	detailsContactPhone,
	detailsAdjCompanyName,
	detailsAdjCompanyAddress,
	detailsAdjCompanyCity,
	detailsAdjCompanyStateCode,
	detailsAdjCompanyZip,
	detailsClaimNumber,
	detailsPolicyNumber
FROM
	#TempFMDataTableClaims;

OPEN CU_FireClaimCursor
FETCH NEXT FROM CU_FireClaimCursor
INTO
	@detailsClaimType,
	@detailsClaimDate,
	@detailsFileNumber,
	@detailsLossType,
	@detailsCauseOfLossDesc,
	@detailsTypeOfPropertyDesc,
	@detailsLocationOfLossStreet,
	@detailsLocationOfLossCity,
	@detailsLocationOfLossStateCode,
	@detailsCompanyName,
	@detailsCompanyPhone,
	@detailsContactName,
	@detailsContactPhone,
	@detailsAdjCompanyName,
	@detailsAdjCompanyAddress,
	@detailsAdjCompanyCity,
	@detailsAdjCompanyStateCode,
	@detailsAdjCompanyZip,
	@detailsClaimNumber,
	@detailsPolicyNumber;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SELECT
			@detailsClaimTypeFormatted = ISNULL(CAST(@detailsClaimType AS CHAR(10)),'') + STUFF(ISNULL(@detailsClaimTypeFormatted,''),1,LEN(ISNULL(@detailsClaimType,'')),''),
			@detailsLossTypeFormatted = ISNULL(@detailsLossType,'') + STUFF(ISNULL(@detailsLossTypeFormatted,''),1,LEN(ISNULL(@detailsLossType,'')),''),
			@detailsLocationOfLossCityFormatted = ISNULL(@detailsLocationOfLossCity,'') + STUFF(ISNULL(@detailsLocationOfLossCityFormatted,''),1,LEN(ISNULL(@detailsLocationOfLossCity,'')),''),
			@detailsCompanyNameFormatted = ISNULL(@detailsCompanyName,'') + STUFF(ISNULL(@detailsCompanyNameFormatted,''),1,LEN(ISNULL(@detailsCompanyName,'')),''),
			@detailsContactNameFormatted = ISNULL(@detailsContactName,'') + STUFF(ISNULL(@detailsContactNameFormatted,''),1,LEN(ISNULL(@detailsContactName,'')),''),
			@detailsAdjCompanyCityFormatted = ISNULL(@detailsAdjCompanyCity,'') + STUFF(ISNULL(@detailsAdjCompanyCityFormatted,''),1,LEN(ISNULL(@detailsAdjCompanyCity,'')),'');

		SELECT
			@flatFileContent = @flatFileContent + @newLine +
			'1 Activity & Date: ' + ISNULL(@detailsClaimTypeFormatted,'') + ISNULL(CAST(@detailsClaimDate AS CHAR(10)),'') + '   File Number: ' + ISNULL(@detailsFileNumber,'') + @newLine +
			/*/**20181206 WordingChangeRequest: FROM TypeOfLoss TO "Loss Description" (see next line):**/'     Type of Loss: ' + ISNULL(@detailsLossTypeFormatted,'') + @newLine +*/
			' Loss Description: ' + ISNULL(@detailsLossTypeFormatted,'') + @newLine +
			'    Cause of Loss: ' + ISNULL(@detailsCauseOfLossDesc,'') + @newLine +
			CASE
				WHEN NULLIF(LTRIM(RTRIM(@detailsTypeOfPropertyDesc)),'') IS NOT NULL
				THEN
					' Type of Property: ' + @detailsTypeOfPropertyDesc + @newLine
				ELSE ''
			END +
			' Location of Loss: ' + ISNULL(@detailsLocationOfLossStreet,'') + @newLine +
			'             City: ' + ISNULL(@detailsLocationOfLossCityFormatted,'') + 'ST: ' + ISNULL(@detailsLocationOfLossStateCode,'') + @newLine +
			'          Company: ' + ISNULL(@detailsCompanyNameFormatted,'') + ' Phone: ' + ISNULL(@detailsCompanyPhone,'') + @newLine +
			CASE
				WHEN NULLIF(LTRIM(RTRIM(@detailsContactNameFormatted)),'') IS NOT NULL
				THEN
			'          Contact: ' + @detailsContactNameFormatted + ' Phone: ' + ISNULL(@detailsContactPhone,'') + @newLine
				ELSE ''
			END +
			CASE
				WHEN NULLIF(LTRIM(RTRIM(@detailsAdjCompanyName)),'') IS NOT NULL
				THEN
			'      ADJ COMPANY: ' + @detailsAdjCompanyName + @newLine +
			'          ADDRESS: ' + ISNULL(@detailsAdjCompanyAddress,'') + @newLine +
			'             City: ' + ISNULL(@detailsAdjCompanyCityFormatted,'') + 'ST: ' + ISNULL(@detailsAdjCompanyStateCode,'') + '  Zip: ' + ISNULL(@detailsAdjCompanyZip,'') + @newLine
				ELSE ''
			END +
			'     Claim Number: ' + ISNULL(@detailsClaimNumber,'') + @newLine +
			'    Policy Number: ' + ISNULL(@detailsPolicyNumber,'') + @newLine;
----------/*Involved Party level*/----------
			DECLARE CU_FireClaimIPCursor CURSOR
			FOR SELECT
				involvedPartyID,
				detailsInvolvedPartyDescription,
				detailsInvolvedPartyName,
				detailsInvolvedPartyAddress,
				detailsInvolvedPartyCity,
				detailsInvolvedPartyStateCode,
				detailsInvolvedPartyDOB,
				detailsInvolvedPartySSN,
				detailsInvolvedPartySSNIssuedNote,
				detailsInvolvedPartyPhone,
				detailsIsIncendiaryFire,
				detailsIsUnderSIUInvestigation
			FROM
				#TempFMDataTableInvolvedParty
			WHERE
				#TempFMDataTableInvolvedParty.detailsClaimNumber = @detailsFileNumber;
				
			OPEN CU_FireClaimIPCursor
			FETCH NEXT FROM CU_FireClaimIPCursor
			INTO
				@involvedPartyID,
				@detailsInvolvedPartyDescription,
				@detailsInvolvedPartyName,
				@detailsInvolvedPartyAddress,
				@detailsInvolvedPartyCity,
				@detailsInvolvedPartyStateCode,
				@detailsInvolvedPartyDOB,
				@detailsInvolvedPartySSN,
				@detailsInvolvedPartySSNIssuedNote,
				@detailsInvolvedPartyPhone,
				@detailsIsIncendiaryFire,
				@detailsIsUnderSIUInvestigation;
						
			WHILE @@FETCH_STATUS = 0
				BEGIN
					SELECT
						@detailsInvolvedPartyAddressFormatted = ISNULL(@detailsInvolvedPartyAddress,'') + STUFF(ISNULL(@detailsInvolvedPartyAddressFormatted,''),1,LEN(ISNULL(@detailsInvolvedPartyAddress,'')),''),
						@detailsInvolvedPartyCityFormatted = ISNULL(@detailsInvolvedPartyCity,'') + STUFF(ISNULL(@detailsInvolvedPartyCityFormatted,''),1,LEN(ISNULL(@detailsInvolvedPartyCity,'')),'');
						
					SELECT
						@flatFileContent = @flatFileContent + @newLine +
						+ @newLine +
						'   Involved Party: ' + ISNULL(@detailsInvolvedPartyDescription,'') + @newLine +
						+ @newLine +
						'             Name: ' + ISNULL(@detailsInvolvedPartyName,'') + @newLine +
						'          Address: ' + ISNULL(@detailsInvolvedPartyAddressFormatted,'') + 'City: ' + ISNULL(@detailsInvolvedPartyCityFormatted,'') + 'State: ' + ISNULL(@detailsInvolvedPartyStateCode,'') + @newLine +
						+ @newLine +
						CASE
							WHEN NULLIF(LTRIM(RTRIM(@detailsInvolvedPartyDOB)),'') IS NOT NULL
							THEN
								'              DOB: ' + @detailsInvolvedPartyDOB + @newLine
							ELSE ''
						END +
						CASE
							WHEN NULLIF(LTRIM(RTRIM(@detailsInvolvedPartySSN)),'') IS NOT NULL
							THEN
								'              SSN: ' + @detailsInvolvedPartySSN + @newLine /*+ '        ' + ISNULL(@detailsInvolvedPartySSNIssuedNote,'') + @newLine*/
								/*'              SSN: *Obfuscated*' + @newLine*/
							ELSE ''
						END +
						CASE
							WHEN NULLIF(LTRIM(RTRIM(@detailsInvolvedPartyPhone)),'') IS NOT NULL
							THEN
								'            Phone: ' + @detailsInvolvedPartyPhone + @newLine
							ELSE ''
						END +
						CASE
							WHEN NULLIF(LTRIM(RTRIM(@detailsIsIncendiaryFire)),'') IS NOT NULL
							THEN
								'                Incendiary Fire: ' + @detailsIsIncendiaryFire + @newLine
							ELSE ''
						END +
						CASE
							WHEN NULLIF(LTRIM(RTRIM(@detailsIsUnderSIUInvestigation)),'') IS NOT NULL
							THEN
								'  Party Under SIU Investigation: ' + @detailsIsUnderSIUInvestigation + @newLine
							ELSE ''
						END;
----------/*Coverage level*/----------
					DECLARE CU_FireClaimCovCursor CURSOR
					FOR SELECT
						detailsCoverageDescription,
						coverageDetailsPolicyAmountBuild,
						coverageDetailsPolicyAmountContents,
						coverageDetailsPolicyAmountStock,
						coverageDetailsPolicyAmountOccupancy,
						coverageDetailsPolicyAmountOther,
						coverageDetailsEstimatedLossBuild,
						coverageDetailsEstimatedLossContents,
						coverageDetailsEstimatedLossStock,
						coverageDetailsEstimatedLossOccupancy,
						coverageDetailsEstimatedLossOther,
						coverageDetailsSettlementAmountBuild,
						coverageDetailsSettlementAmountContents,
						coverageDetailsSettlementAmountStock,
						coverageDetailsSettlementAmountOccupancy,
						coverageDetailsSettlementAmountOther,
						coverageDetailsVin,
						coverageDetailsVehicleYear,
						coverageDetailsVehicleStyle,
						coverageDetailsVehicleMake,
						coverageDetailsVehicleModel,
						coverageDetailsVehicleType,
						coverageDetailsLicencePlate,
						coverageDetailsLicenceState,
						coverageDetailsLicenceYear,
						coverageDetailsLicenceType
					FROM
						#TempFMDataTableCoverage
					WHERE
						#TempFMDataTableCoverage.detailsClaimNumber = @detailsFileNumber
						AND #TempFMDataTableCoverage.involvedPartyID = @involvedPartyID;
						
					OPEN CU_FireClaimCovCursor;
					FETCH NEXT FROM CU_FireClaimCovCursor
					INTO
						@detailsCoverageDescription,
						@coverageDetailsPolicyAmountBuild,
						@coverageDetailsPolicyAmountContents,
						@coverageDetailsPolicyAmountStock,
						@coverageDetailsPolicyAmountOccupancy,
						@coverageDetailsPolicyAmountOther,
						@coverageDetailsEstimatedLossBuild,
						@coverageDetailsEstimatedLossContents,
						@coverageDetailsEstimatedLossStock,
						@coverageDetailsEstimatedLossOccupancy,
						@coverageDetailsEstimatedLossOther,
						@coverageDetailsSettlementAmountBuild,
						@coverageDetailsSettlementAmountContents,
						@coverageDetailsSettlementAmountStock,
						@coverageDetailsSettlementAmountOccupancy,
						@coverageDetailsSettlementAmountOther,
						@coverageDetailsVin,
						@coverageDetailsVehicleYear,
						@coverageDetailsVehicleStyle,
						@coverageDetailsVehicleMake,
						@coverageDetailsVehicleModel,
						@coverageDetailsVehicleType,
						@coverageDetailsLicencePlate,
						@coverageDetailsLicenceState,
						@coverageDetailsLicenceYear,
						@coverageDetailsLicenceType;
								
					WHILE @@FETCH_STATUS = 0
						BEGIN
							/**Amount to String comma formatting**/
							SELECT
								@coverageDetailsPolicyAmountBuildFormatted = 
									CASE
										WHEN
											@coverageDetailsPolicyAmountBuild IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsPolicyAmountBuild) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsPolicyAmountBuild AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountBuild)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountBuild)-5,0,',')
										WHEN
											LEN(@coverageDetailsPolicyAmountBuild) > 3
										THEN
											STUFF(CAST(@coverageDetailsPolicyAmountBuild AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountBuild)-2,0,',')
										ELSE
											CAST(@coverageDetailsPolicyAmountBuild AS VARCHAR(15))
									END,
								@coverageDetailsPolicyAmountContentsFormatted =
									CASE
										WHEN
											@coverageDetailsPolicyAmountContents IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsPolicyAmountContents) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsPolicyAmountContents AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountContents)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountContents)-5,0,',')
										WHEN
											LEN(@coverageDetailsPolicyAmountContents) > 3
										THEN
											STUFF(CAST(@coverageDetailsPolicyAmountContents AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountContents)-2,0,',')
										ELSE
											CAST(@coverageDetailsPolicyAmountContents AS VARCHAR(15))
									END,
								@coverageDetailsPolicyAmountStockFormatted =
									CASE
										WHEN
											@coverageDetailsPolicyAmountStock IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsPolicyAmountStock) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsPolicyAmountStock AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountStock)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountStock)-5,0,',')
										WHEN
											LEN(@coverageDetailsPolicyAmountStock) > 3
										THEN
											STUFF(CAST(@coverageDetailsPolicyAmountStock AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountStock)-2,0,',')
										ELSE
											CAST(@coverageDetailsPolicyAmountStock AS VARCHAR(15))
									END,
								@coverageDetailsPolicyAmountOccupancyFormatted =
									CASE
										WHEN
											@coverageDetailsPolicyAmountOccupancy IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsPolicyAmountOccupancy) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsPolicyAmountOccupancy AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountOccupancy)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountOccupancy)-5,0,',')
										WHEN
											LEN(@coverageDetailsPolicyAmountOccupancy) > 3
										THEN
											STUFF(CAST(@coverageDetailsPolicyAmountOccupancy AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountOccupancy)-2,0,',')
										ELSE
											CAST(@coverageDetailsPolicyAmountOccupancy AS VARCHAR(15))
									END,
								@coverageDetailsPolicyAmountOtherFormatted =
									CASE
										WHEN
											@coverageDetailsPolicyAmountOther IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsPolicyAmountOther) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsPolicyAmountOther AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountOther)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountOther)-5,0,',')
										WHEN
											LEN(@coverageDetailsPolicyAmountOther) > 3
										THEN
											STUFF(CAST(@coverageDetailsPolicyAmountOther AS VARCHAR(15)),LEN(@coverageDetailsPolicyAmountOther)-2,0,',')
										ELSE
											CAST(@coverageDetailsPolicyAmountOther AS VARCHAR(15))
									END,
								@coverageDetailsEstimatedLossBuildFormatted =
									CASE
										WHEN
											@coverageDetailsEstimatedLossBuild IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsEstimatedLossBuild) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsEstimatedLossBuild AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossBuild)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossBuild)-5,0,',')
										WHEN
											LEN(@coverageDetailsEstimatedLossBuild) > 3
										THEN
											STUFF(CAST(@coverageDetailsEstimatedLossBuild AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossBuild)-2,0,',')
										ELSE
											CAST(@coverageDetailsEstimatedLossBuild AS VARCHAR(15))
									END,
								@coverageDetailsEstimatedLossContentsFormatted =
									CASE
										WHEN
											@coverageDetailsEstimatedLossContents IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsEstimatedLossContents) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsEstimatedLossContents AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossContents)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossContents)-5,0,',')
										WHEN
											LEN(@coverageDetailsEstimatedLossContents) > 3
										THEN
											STUFF(CAST(@coverageDetailsEstimatedLossContents AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossContents)-2,0,',')
										ELSE
											CAST(@coverageDetailsEstimatedLossContents AS VARCHAR(15))
									END,
								@coverageDetailsEstimatedLossStockFormatted =
									CASE
										WHEN
											@coverageDetailsEstimatedLossStock IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsEstimatedLossStock) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsEstimatedLossStock AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossStock)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossStock)-5,0,',')
										WHEN
											LEN(@coverageDetailsEstimatedLossStock) > 3
										THEN
											STUFF(CAST(@coverageDetailsEstimatedLossStock AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossStock)-2,0,',')
										ELSE
											CAST(@coverageDetailsEstimatedLossStock AS VARCHAR(15))
									END,
								@coverageDetailsEstimatedLossOccupancyFormatted =
									CASE
										WHEN
											@coverageDetailsEstimatedLossOccupancy IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsEstimatedLossOccupancy) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsEstimatedLossOccupancy AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossOccupancy)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossOccupancy)-5,0,',')
										WHEN
											LEN(@coverageDetailsEstimatedLossOccupancy) > 3
										THEN
											STUFF(CAST(@coverageDetailsEstimatedLossOccupancy AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossOccupancy)-2,0,',')
										ELSE
											CAST(@coverageDetailsEstimatedLossOccupancy AS VARCHAR(15))
									END,
								@coverageDetailsEstimatedLossOtherFormatted =
									CASE
										WHEN
											@coverageDetailsEstimatedLossOther IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsEstimatedLossOther) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsEstimatedLossOther AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossOther)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossOther)-5,0,',')
										WHEN
											LEN(@coverageDetailsEstimatedLossOther) > 3
										THEN
											STUFF(CAST(@coverageDetailsEstimatedLossOther AS VARCHAR(15)),LEN(@coverageDetailsEstimatedLossOther)-2,0,',')
										ELSE
											CAST(@coverageDetailsEstimatedLossOther AS VARCHAR(15))
									END,
								@coverageDetailsSettlementAmountBuildFormatted =
									CASE
										WHEN
											@coverageDetailsSettlementAmountBuild IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsSettlementAmountBuild) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsSettlementAmountBuild AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountBuild)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountBuild)-5,0,',')
										WHEN
											LEN(@coverageDetailsSettlementAmountBuild) > 3
										THEN
											STUFF(CAST(@coverageDetailsSettlementAmountBuild AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountBuild)-2,0,',')
										ELSE
											CAST(@coverageDetailsSettlementAmountBuild AS VARCHAR(15))
									END,
								@coverageDetailsSettlementAmountContentsFormatted =
									CASE
										WHEN
											@coverageDetailsSettlementAmountContents IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsSettlementAmountContents) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsSettlementAmountContents AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountContents)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountContents)-5,0,',')
										WHEN
											LEN(@coverageDetailsSettlementAmountContents) > 3
										THEN
											STUFF(CAST(@coverageDetailsSettlementAmountContents AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountContents)-2,0,',')
										ELSE
											CAST(@coverageDetailsSettlementAmountContents AS VARCHAR(15))
									END,
								@coverageDetailsSettlementAmountStockFormatted =
									CASE
										WHEN
											@coverageDetailsSettlementAmountStock IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsSettlementAmountStock) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsSettlementAmountStock AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountStock)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountStock)-5,0,',')
										WHEN
											LEN(@coverageDetailsSettlementAmountStock) > 3
										THEN
											STUFF(CAST(@coverageDetailsSettlementAmountStock AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountStock)-2,0,',')
										ELSE
											CAST(@coverageDetailsSettlementAmountStock AS VARCHAR(15))
									END,
								@coverageDetailsSettlementAmountOccupancyFormatted =
									CASE
										WHEN
											@coverageDetailsSettlementAmountOccupancy IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsSettlementAmountOccupancy) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsSettlementAmountOccupancy AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountOccupancy)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountOccupancy)-5,0,',')
										WHEN
											LEN(@coverageDetailsSettlementAmountOccupancy) > 3
										THEN
											STUFF(CAST(@coverageDetailsSettlementAmountOccupancy AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountOccupancy)-2,0,',')
										ELSE
											CAST(@coverageDetailsSettlementAmountOccupancy AS VARCHAR(15))
									END,
								@coverageDetailsSettlementAmountOtherFormatted =
									CASE
										WHEN
											@coverageDetailsSettlementAmountOther IS NULL
										THEN
											NULL
										WHEN
											LEN(@coverageDetailsSettlementAmountOther) > 6
										THEN
											STUFF(CAST(STUFF(CAST(@coverageDetailsSettlementAmountOther AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountOther)-2,0,',') AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountOther)-5,0,',')
										WHEN
											LEN(@coverageDetailsSettlementAmountOther) > 3
										THEN
											STUFF(CAST(@coverageDetailsSettlementAmountOther AS VARCHAR(15)),LEN(@coverageDetailsSettlementAmountOther)-2,0,',')
										ELSE
											CAST(@coverageDetailsSettlementAmountOther AS VARCHAR(15))
									END;
							
							SELECT
								@flatFileContent = @flatFileContent
								+ @newLine +
								'         Coverage: ' + @detailsCoverageDescription + @newLine +
								CASE
									WHEN /**********************    Property CoverageType Section    ************************/
										@detailsCoverageDescription IN ('Property', 'Scheduled Pro')
									THEN
										'                                                             Use &      Other/'  + @newLine +
										'                         Building    Contents       Stock   Occupancy  Scheduled' + @newLine +
										CASE/***********************		PolicyAmount Section **********************************/
											WHEN /****At least one PolicyAmount IsNotNULL*******************/
												@coverageDetailsPolicyAmountBuild IS NOT NULL
												OR @coverageDetailsPolicyAmountContents IS NOT NULL
												OR @coverageDetailsPolicyAmountStock IS NOT NULL
												OR @coverageDetailsPolicyAmountOccupancy IS NOT NULL
												OR @coverageDetailsPolicyAmountOther IS NOT NULL
											THEN
												' Amount of Policy: ' +
												ISNULL(
													STUFF(
														'             ',
														13 - LEN(@coverageDetailsPolicyAmountBuildFormatted)+1,
														LEN(@coverageDetailsPolicyAmountBuildFormatted),
														@coverageDetailsPolicyAmountBuildFormatted
													),'             '
												) +
												ISNULL(
													STUFF(              
														'           ',
														11-LEN(@coverageDetailsPolicyAmountContentsFormatted)+1,
														LEN(@coverageDetailsPolicyAmountContentsFormatted),
														@coverageDetailsPolicyAmountContentsFormatted
													),'           '
												) +
												ISNULL(
													STUFF(
														'            ',
														12-LEN(@coverageDetailsPolicyAmountStockFormatted)+1,
														LEN(@coverageDetailsPolicyAmountStockFormatted),
														@coverageDetailsPolicyAmountStockFormatted
													),'            '
												) +
												ISNULL(
													STUFF(
														'            ',
														12-LEN(@coverageDetailsPolicyAmountOccupancyFormatted)+1,
														LEN(@coverageDetailsPolicyAmountOccupancyFormatted),
														@coverageDetailsPolicyAmountOccupancyFormatted
													),'            '
												) +
												ISNULL(
													STUFF(
														'           ',
														11-LEN(@coverageDetailsPolicyAmountOtherFormatted)+1,
														LEN(@coverageDetailsPolicyAmountOtherFormatted),
														@coverageDetailsPolicyAmountOtherFormatted
													),'           '
												) + @newLine
											ELSE
												''
										END + /*************************     PolicyAmount Section End     *****************************/
										CASE/**************************        EstimatedLoss Section        **************************/
											WHEN /****At least one PolicyAmount IsNotNULL*******************/
												@coverageDetailsEstimatedLossBuildFormatted IS NOT NULL
												OR @coverageDetailsEstimatedLossOccupancyFormatted IS NOT NULL
												OR @coverageDetailsEstimatedLossStockFormatted IS NOT NULL
												OR @coverageDetailsEstimatedLossOccupancyFormatted IS NOT NULL
												OR @coverageDetailsEstimatedLossOtherFormatted IS NOT NULL
											THEN
												' Estimated Loss:   ' +
												ISNULL(
													STUFF(
														'             ',
														13-LEN(@coverageDetailsEstimatedLossBuildFormatted)+1,
														LEN(@coverageDetailsEstimatedLossBuildFormatted),
														@coverageDetailsEstimatedLossBuildFormatted
													),'             '
												) +
												ISNULL(
													STUFF(              
														'           ',
														11-LEN(@coverageDetailsEstimatedLossOccupancyFormatted)+1,
														LEN(@coverageDetailsEstimatedLossOccupancyFormatted),
														@coverageDetailsEstimatedLossOccupancyFormatted
													),'           '
												) +
												ISNULL(
													STUFF(
														'            ',
														12-LEN(@coverageDetailsEstimatedLossStockFormatted)+1,
														LEN(@coverageDetailsEstimatedLossStockFormatted),
														@coverageDetailsEstimatedLossStockFormatted
													),'            '
												) +
												ISNULL(
													STUFF(
														'            ',
														12-LEN(@coverageDetailsEstimatedLossOccupancyFormatted)+1,
														LEN(@coverageDetailsEstimatedLossOccupancyFormatted),
														@coverageDetailsEstimatedLossOccupancyFormatted
													),'            '
												) +
												ISNULL(
													STUFF(
														'           ',
														11-LEN(@coverageDetailsEstimatedLossOtherFormatted)+1,
														LEN(@coverageDetailsEstimatedLossOtherFormatted),
														@coverageDetailsEstimatedLossOtherFormatted
													),'           '
												) + @newLine
											ELSE
												''
										END + /*************************     EstimatedLoss Section End     *****************************/
										CASE/***************************        Settlement Section         *****************************/
											WHEN /****At least one PolicyAmount IsNotNULL*******************/
												@coverageDetailsSettlementAmountBuildFormatted IS NOT NULL
												OR @coverageDetailsSettlementAmountContentsFormatted IS NOT NULL
												OR @coverageDetailsSettlementAmountStockFormatted IS NOT NULL
												OR @coverageDetailsSettlementAmountOccupancyFormatted IS NOT NULL
												OR @coverageDetailsSettlementAmountOtherFormatted IS NOT NULL
											THEN
												' Settlement amount: ' +
												ISNULL(
													STUFF(
														'            ',
														12-LEN(@coverageDetailsSettlementAmountBuildFormatted)+1,
														LEN(@coverageDetailsSettlementAmountBuildFormatted),
														@coverageDetailsSettlementAmountBuildFormatted
													),'            '
												) +
												ISNULL(
													STUFF(              
														'           ',
														11-LEN(@coverageDetailsSettlementAmountContentsFormatted)+1,
														LEN(@coverageDetailsSettlementAmountContentsFormatted),
														@coverageDetailsSettlementAmountContentsFormatted
													),'           '
												) +
												ISNULL(
													STUFF(
														'            ',
														12-LEN(@coverageDetailsSettlementAmountStockFormatted)+1,
														LEN(@coverageDetailsSettlementAmountStockFormatted),
														@coverageDetailsSettlementAmountStockFormatted
													),'            '
												) +
												ISNULL(
													STUFF(
														'            ',
														12-LEN(@coverageDetailsSettlementAmountOccupancyFormatted)+1,
														LEN(@coverageDetailsSettlementAmountOccupancyFormatted),
														@coverageDetailsSettlementAmountOccupancyFormatted
													),'            '
												) +
												ISNULL(
													STUFF(
														'           ',
														11-LEN(@coverageDetailsSettlementAmountOtherFormatted)+1,
														LEN(@coverageDetailsSettlementAmountOtherFormatted),
														@coverageDetailsSettlementAmountOtherFormatted
													),'           '
												) + @newLine
											ELSE
												'' + @newLine
										END /*************************     EstimatedLoss Section End     *****************************/
									/**********************  Property CoverageType Section End  ************************/
									WHEN /**********************    Comprehensive CoverageType Section    ************************/
										@detailsCoverageDescription = 'Comprehensive'
									THEN
										ISNULL(
											' Estimated Loss: ' +
											STUFF(
												'               ',
												15-LEN(@coverageDetailsEstimatedLossOtherFormatted)+1,
												LEN(@coverageDetailsEstimatedLossOtherFormatted),
												@coverageDetailsEstimatedLossOtherFormatted
											) + @newLine,''
										) +
										ISNULL(
											' Settlement Amount: ' +
											STUFF(
												'            ',
												12-LEN(@coverageDetailsSettlementAmountOtherFormatted)+1,
												LEN(@coverageDetailsSettlementAmountOtherFormatted),
												@coverageDetailsSettlementAmountOtherFormatted
											) + @newLine,''
										) +
										+ @newLine +
										'              VIN: ' +
										ISNULL(
											STUFF(
												'                           ',
												1,
												LEN(@coverageDetailsVin),
												@coverageDetailsVin
											),'                           '
										) +
										' VYR:  ' +
										ISNULL(
											STUFF(              
												'           ',
												1,
												LEN(@coverageDetailsVehicleYear),
												@coverageDetailsVehicleYear
											),'           '
										) +
										+ ' VST:' + ISNULL('  '+ @coverageDetailsVehicleStyle,'') + @newLine +
										'              VMA: ' +
										ISNULL(
											STUFF(
												'                           ',
												1,
												LEN(@coverageDetailsVehicleMake),
												@coverageDetailsVehicleMake
											),'                           '
										) +
										' VMO: ' +
										ISNULL(
											STUFF(
												'            ',
												1,
												LEN(@coverageDetailsVehicleModel),
												@coverageDetailsVehicleModel
											),'            '
										) +
										+ ' VTP:' + ISNULL('  '+ @coverageDetailsVehicleType,'') + @newLine +
										CASE/***************************        License Section         *****************************/
											WHEN /****At least one License Value IsNotNULL*******************/
												@coverageDetailsLicencePlate IS NOT NULL
												OR @coverageDetailsLicenceState IS NOT NULL
												OR @coverageDetailsLicenceYear IS NOT NULL
												OR @coverageDetailsLicenceType IS NOT NULL
											THEN
											
										
												'        Lic Plate: ' +
												ISNULL(
													STUFF(
														'              ',
														1,
														LEN(@coverageDetailsLicencePlate),
														@coverageDetailsLicencePlate
													),'              '
												) +
												' Lic State: ' +
												ISNULL(
													STUFF(              
														'      ',
														1,
														LEN(@coverageDetailsLicenceState),
														@coverageDetailsLicenceState
													),'      '
												) +
												' Lic Year: ' +
												ISNULL(
													STUFF(
														'    ',
														1,
														LEN(@coverageDetailsLicenceYear),
														@coverageDetailsLicenceYear
													),'    '
												) +
												' Lic Type:' +
												ISNULL(' ' + @coverageDetailsLicenceType,'') + @newLine
											ELSE
												'' + @newLine
										END /*************************     License Section End     *****************************/
									/**********************  Comprehensive CoverageType Section End  ************************/
									ELSE /**********************    [Prop. Other] CoverageType Section    ************************/
										ISNULL(
											' Estimated Loss: ' +
											STUFF(
												'               ',
												15-LEN(@coverageDetailsEstimatedLossOtherFormatted)+1,
												LEN(@coverageDetailsEstimatedLossOtherFormatted),
												@coverageDetailsEstimatedLossOtherFormatted
											) + @newLine,''
										) +
										ISNULL(
											' Settlement Amount: ' +
											STUFF(
												'            ',
												12-LEN(@coverageDetailsSettlementAmountOtherFormatted)+1,
												LEN(@coverageDetailsSettlementAmountOtherFormatted),
												@coverageDetailsSettlementAmountOtherFormatted
											) + @newLine,''
										)
									/**********************  [Prop. Other] CoverageType Section End  ************************/
								END

							SELECT
								@detailsCoverageDescription = NULL,
								@coverageDetailsPolicyAmountBuild = NULL,
								@coverageDetailsPolicyAmountContents = NULL,
								@coverageDetailsPolicyAmountStock = NULL,
								@coverageDetailsPolicyAmountOccupancy = NULL,
								@coverageDetailsPolicyAmountOther = NULL,
								@coverageDetailsEstimatedLossBuild = NULL,
								@coverageDetailsEstimatedLossContents = NULL,
								@coverageDetailsEstimatedLossStock = NULL,
								@coverageDetailsEstimatedLossOccupancy = NULL,
								@coverageDetailsEstimatedLossOther = NULL,
								@coverageDetailsSettlementAmountBuild = NULL,
								@coverageDetailsSettlementAmountContents = NULL,
								@coverageDetailsSettlementAmountStock = NULL,
								@coverageDetailsSettlementAmountOccupancy = NULL,
								@coverageDetailsSettlementAmountOther = NULL,
								@coverageDetailsPolicyAmountBuildFormatted = NULL,
								@coverageDetailsPolicyAmountContentsFormatted = NULL,
								@coverageDetailsPolicyAmountStockFormatted = NULL,
								@coverageDetailsPolicyAmountOccupancyFormatted = NULL,
								@coverageDetailsPolicyAmountOtherFormatted = NULL,
								@coverageDetailsEstimatedLossBuildFormatted = NULL,
								@coverageDetailsEstimatedLossOccupancyFormatted = NULL,
								@coverageDetailsEstimatedLossStockFormatted = NULL,
								@coverageDetailsEstimatedLossOccupancyFormatted = NULL,
								@coverageDetailsEstimatedLossOtherFormatted = NULL,
								@coverageDetailsSettlementAmountBuildFormatted = NULL,
								@coverageDetailsSettlementAmountContentsFormatted = NULL,
								@coverageDetailsSettlementAmountStockFormatted = NULL,
								@coverageDetailsSettlementAmountOccupancyFormatted = NULL,
								@coverageDetailsSettlementAmountOtherFormatted = NULL,
								@coverageDetailsVin = NULL,
								@coverageDetailsVehicleYear = NULL,
								@coverageDetailsVehicleStyle = NULL,
								@coverageDetailsVehicleMake = NULL,
								@coverageDetailsVehicleModel = NULL,
								@coverageDetailsVehicleType = NULL,
								@coverageDetailsLicencePlate = NULL,
								@coverageDetailsLicenceState = NULL,
								@coverageDetailsLicenceYear = NULL,
								@coverageDetailsLicenceType = NULL;
								
							FETCH NEXT FROM CU_FireClaimCovCursor
							INTO
								@detailsCoverageDescription,
								@coverageDetailsPolicyAmountBuild,
								@coverageDetailsPolicyAmountContents,
								@coverageDetailsPolicyAmountStock,
								@coverageDetailsPolicyAmountOccupancy,
								@coverageDetailsPolicyAmountOther,
								@coverageDetailsEstimatedLossBuild,
								@coverageDetailsEstimatedLossContents,
								@coverageDetailsEstimatedLossStock,
								@coverageDetailsEstimatedLossOccupancy,
								@coverageDetailsEstimatedLossOther,
								@coverageDetailsSettlementAmountBuild,
								@coverageDetailsSettlementAmountContents,
								@coverageDetailsSettlementAmountStock,
								@coverageDetailsSettlementAmountOccupancy,
								@coverageDetailsSettlementAmountOther,
								@coverageDetailsVin,
								@coverageDetailsVehicleYear,
								@coverageDetailsVehicleStyle,
								@coverageDetailsVehicleMake,
								@coverageDetailsVehicleModel,
								@coverageDetailsVehicleType,
								@coverageDetailsLicencePlate,
								@coverageDetailsLicenceState,
								@coverageDetailsLicenceYear,
								@coverageDetailsLicenceType;
							
						END
					CLOSE CU_FireClaimCovCursor;
					DEALLOCATE CU_FireClaimCovCursor;
						
					SELECT
						@detailsInvolvedPartyDescription = NULL,
						@detailsInvolvedPartyName = NULL,
						@detailsInvolvedPartyAddress = NULL,
						@detailsInvolvedPartyAddressFormatted = NULL,
						@detailsInvolvedPartyCity = NULL,
						@detailsInvolvedPartyCityFormatted = NULL,
						@detailsInvolvedPartyStateCode = NULL,
						@detailsInvolvedPartyDOB = NULL,
						@detailsInvolvedPartySSN = NULL,
						@detailsInvolvedPartySSNIssuedNote = NULL,
						@detailsInvolvedPartyPhone = NULL,
						@detailsIsIncendiaryFire = NULL,
						@detailsIsUnderSIUInvestigation = NULL;
						
					FETCH NEXT FROM CU_FireClaimIPCursor
					INTO
						@involvedPartyID,
						@detailsInvolvedPartyDescription,
						@detailsInvolvedPartyName,
						@detailsInvolvedPartyAddress,
						@detailsInvolvedPartyCity,
						@detailsInvolvedPartyStateCode,
						@detailsInvolvedPartyDOB,
						@detailsInvolvedPartySSN,
						@detailsInvolvedPartySSNIssuedNote,
						@detailsInvolvedPartyPhone,
						@detailsIsIncendiaryFire,
						@detailsIsUnderSIUInvestigation;
				END
			CLOSE CU_FireClaimIPCursor;
			DEALLOCATE CU_FireClaimIPCursor;
				
			SELECT
				@detailsClaimType = NULL,
				@detailsClaimTypeFormatted = '                      ',
				@detailsClaimDate = NULL,
				@detailsFileNumber = NULL,
				@detailsLossType = NULL,
				@detailsLossTypeFormatted = '                                  FIRE',
				@detailsCauseOfLossDesc = NULL,
				@detailsTypeOfPropertyDesc = NULL,
				@detailsLocationOfLossStreet = NULL,
				@detailsLocationOfLossCity = NULL,
				@detailsLocationOfLossCityFormatted = NULL,
				@detailsLocationOfLossStateCode = NULL,
				@detailsCompanyName = NULL,
				@detailsCompanyNameFormatted = '                                           ',
				@detailsCompanyPhone = NULL,
				@detailsContactName = NULL,
				@detailsContactNameFormatted = '                                           ',
				@detailsContactPhone = NULL,
				@detailsAdjCompanyName = NULL,
				@detailsAdjCompanyAddress = NULL,
				@detailsAdjCompanyCity = NULL,
				@detailsAdjCompanyCityFormatted = NULL,
				@detailsAdjCompanyStateCode = NULL,
				@detailsAdjCompanyZip = NULL,
				@detailsClaimNumber = NULL,
				@detailsPolicyNumber = NULL
				
				
			FETCH NEXT FROM CU_FireClaimCursor
			INTO
				@detailsClaimType,
				@detailsClaimDate,
				@detailsFileNumber,
				@detailsLossType,
				@detailsCauseOfLossDesc,
				@detailsTypeOfPropertyDesc,
				@detailsLocationOfLossStreet,
				@detailsLocationOfLossCity,
				@detailsLocationOfLossStateCode,
				@detailsCompanyName,
				@detailsCompanyPhone,
				@detailsContactName,
				@detailsContactPhone,
				@detailsAdjCompanyName,
				@detailsAdjCompanyAddress,
				@detailsAdjCompanyCity,
				@detailsAdjCompanyStateCode,
				@detailsAdjCompanyZip,
				@detailsClaimNumber,
				@detailsPolicyNumber;		
	END
CLOSE CU_FireClaimCursor;
DEALLOCATE CU_FireClaimCursor;

PRINT @flatFileContent

ROLLBACK TRANSACTION