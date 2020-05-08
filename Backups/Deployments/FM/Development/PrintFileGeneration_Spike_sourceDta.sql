detailsClaimType - tab1
detailsClaimDate\lossdate - tab1
(IL-CLAIMNUMBER)\\detailsFileNumber - tab1

detailsLossType , - tab14      \\ replace with __newprocess dan stuff
detailsCauseOfLossDesc, - tab1

??? detailsTypeOfPropertyDesc,    - examples: Hay \ farm \ commercian

								--tab1
detailsLocationOfLossStreet,   
detailsLocationOfLossCity,
detailsLocationOfLossStateCode,
detailsCompanyName,
detailsCompanyPhone,
detailsContactName,
detailsContactPhone,

							Tab1\ __INSSM \ INSO_ mem-mangement \\tab2
detailsAdjCompanyName,
detailsAdjCompanyAddress,
detailsAdjCompanyCity,
detailsAdjCompanyStateCode,
detailsAdjCompanyZip,


							--- tab1
detailsClaimNumber,
detailsPolicyNumber,


detailsInvolvedPartyDescription,			---tab4

detailsInvolvedPartyName,				---tab4   ***on insert into temp table remove aliases tab6, service 
detailsInvolvedPartyAddress,	---tab4
detailsInvolvedPartyCity,
detailsInvolvedPartyStateCode,
detailsInvolvedPartyDOB,			---tab4

???? detailsInvolvedPartySSN,	 --- tab7 token
???? detailsInvolvedPartySSNIssuedNote,  --- possibly have to be removed
detailsInvolvedPartyPhone,				--- tab9


detailsIsIncendiaryFire,			-- tab17 /tab18
detailsIsUnderSIUInvestigation		-- /tab4

detailsClaimNumber VARCHAR(40) NULL,
--	detailsCoverage VARCHAR(100) NULL,

						TAB17
--	coverageDetailsPolicyAmountBuild VARCHAR(15) NULL,
--	coverageDetailsPolicyAmountContents VARCHAR(15) NULL,
--	coverageDetailsPolicyAmountStock VARCHAR(15) NULL,
--	coverageDetailsPolicyAmountOccupancy VARCHAR(15) NULL,
--	coverageDetailsPolicyAmountOther VARCHAR(15) NULL,
--	coverageDetailsEstimatedLossBuild VARCHAR(15) NULL,
--	coverageDetailsEstimatedLossContents VARCHAR(15) NULL,
--	coverageDetailsEstimatedLossStock VARCHAR(15) NULL,
--	coverageDetailsEstimatedLossOccupancy VARCHAR(15) NULL,
--	coverageDetailsEstimatedLossOther VARCHAR(15) NULL,
--	coverageDetailsSettlementAmountBuild VARCHAR(15) NULL,
--	coverageDetailsSettlementAmountContents VARCHAR(15) NULL,
--	coverageDetailsSettlementAmountStock VARCHAR(15) NULL,
--	coverageDetailsSettlementAmountOccupancy VARCHAR(15) NULL,
--	coverageDetailsSettlementAmountOther VARCHAR(15) NULL,

							TAB61
--	coverageDetailsVin
--	coverageDetailsVehicleYear
--	coverageDetailsVehicleMake
--	coverageDetailsVehicleModel
--	coverageDetailsLicencePlate
--	coverageDetailsLicenceState
--	coverageDetailsLicenceYear
--	coverageDetailsLicenceType


EXEC sp_help 'dbo.CLT00061'

N_LIC_PLT	C_ST_ALPH	D_LIC_YR	C_LIC_PLT_TYP
SELECT TOP 100 *
FROM dbo.CLT00061	

SELECT TOP 100
	*
FROM
	dbo.CLT00004
	
	
/*Good Test cases:
SELECT TOP 100
	*
FROM
	dbo.V_Extract_FM_V1
WHERE
	V_Extract_FM_V1.I_ALLCLM IN
	(
		'0A002497479',
		'0A002870334',
		'0A003051424',
		'0A003224087'
	)
ORDER BY
	I_ALLCLM
	
SELECT *
FROM dbo.CLT00001
WHERE
	I_ALLCLM = '0A003224087'

SELECT *
FROM dbo.CLT00004
WHERE
	I_ALLCLM = '0A003224087'

SELECT *
FROM dbo.CLT00017
WHERE
	I_ALLCLM = '0A003224087'
	

detailsIsIncendiaryFire,			-- tab17 /tab18
detailsIsUnderSIUInvestigation		-- /tab4

coverage
TAB17
*/
	