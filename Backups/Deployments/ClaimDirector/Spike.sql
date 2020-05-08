/*
ClaimDirector Plat
USE CLAIMSEARCH_DEV

Also here are some table names that tie back to the auto equivalent of this data. I do not believe you would need these or at least not immediately, but good to have.
[CSDataScience].[CDP].[FS_PA_Claim_UAT],
[CSDataScience].[CDP].[FS_PA_Matching_VIN_UAT],
[CSDataScience].[CDP].[FS_PA_Match_joined_Intellicorp_UAT],
[CSDataScience].[CDP].[FS_PA_Matching_DL_UAT],
[CSDataScience].[CDP].[FS_PA_Matching_Entity_UAT],
[CSDataScience].[CDP].[FS_PA_Matching_SSN_UAT]

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
Columns for second "example file"; titled: "FS_HO_ModelVisualization_IP"
	Ukn - ClaimID
		?????
	KN - ISO File Number
		CLT00004.I_ALLCLM
	KN- Claim Number
		CLT00001.N_CLM
	KN- Policy Number
		CLT00001.N_POL
	KN- Loss Date
		CLT00001.D_OCUR
	KN- LossType (code)
		CLT00001.C_LOSS_TYP
	KN- Policy Type (code)
		CLT00001.C_POL_TYP
	Myb - Scored Date
		Could be CLT00001.Date_Insert
			OR CLT00004.Date_Insert
	Ukn - Score
		?????
	Myb - Involved Party Index
		CLT00004.I_NMADR
	KN - Role Code
		CLT00004.C_ROLE
	KN - Role (description)
		dbo.Lookup_Rolecode
			(join on rolecode from CLT00004.C_ROLE)
	SSN (obfuscated)
	Diver License (obfuscated)
	Ukn - Involved Party ID
	KN - MD_FirstName1
	KN - MD_MiddleName1
	KN - MD_LastName1
	Myb - /*The following 7 columns are likely from the melisa scrub tables; due to inclusion of lat&long; join through: 
		TODO: ADD JOIN PATH FROM FM _ADDRESS SPROC; BE SURE TO COPY FROM NONLOL for ACCURATE MAPPING VALUE.
		*/
		Address
		Suite
		City
		State
		Zip
		ResidAddr_Lat
		ResidAddr_Long

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
Columns for first "example file"; titled: "FS_HO_ModelVisualization_CLM"
	Ukn - ISO Claim ID
		?????
	KN - I_CUST
		CLT00001.I_CUST
	KN - ISO File Number
		CLT00001.I_ALLCLM
	KN - Claim Number
		CLT00001.N_CLM
	KN - Policy Number
		CLT00001.N_POL
	KN - Loss Date
		CLT00001.D_OCUR
	KN - Loss Type Code
		CLT00001.C_LOSS_TYP
	Ukn - T_LOSS_TYP_GRP
		?????
	KN - Loss Type
		Dim_Loss_Type.T_LOSS_TYP (lossTypeDescription),
			(join to table on C_LOSS_TYP)
	KN - Policy Type Code
		CLT00001.C_POL_TYP
	Ukn - C_POL_TYP_grp
		?????
	KN - Policy Type
		Could be Dim_Policy_Type.T_POL_TYP (policyTypeDescription)
		OR Lookup_Pol_Type_Code.C_POL_TYP_DESC (policyTypeDescription)
			(join to either table with CLT00001.C_POL_TYP = either Dim_Policy_Type.C_POL_TYP OR Lookup_Pol_Type_Code.C_POL_TYP)
	Myb - Scored Date
		Could be CLT00001.Date_Insert
	Ukn - Score
		?????
	KN - Policy Inception Date
		CLT00001.D_POL_INCP
	KN - Policy Expiration Date
		CLT00001.D_POL_EXPIR
	Myb - First Report Date
		Could be CLT00001.D_INS_CO_RCV
	
	Myb - /*The following 7 columns are likely from the melisa scrub tables; due to inclusion of lat&long; join through: 
		LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Melissa_Address_Mapping_to_CLT00001 WITH (NOLOCK)
			ON CLT00001.ALLCLMROWID = CS_Lookup_Melissa_Address_Mapping_to_CLT00001.ALLCLMROWID
		LEFT OUTER JOIN [ClaimSearch].dbo.CS_Lookup_Unique_Addresses_Melissa_Output WITH (NOLOCK)
			ON CS_Lookup_Melissa_Address_Mapping_to_CLT00001.AddressKey = CS_Lookup_Unique_Addresses_Melissa_Output.AddressKey
		*/
		KN - Address
		KN - Suite
		KN - City
		KN - Loss State
		KN - Zip
		KN - LossAddr_Lat
		KN - LossAddr_Long
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
SELECT * FROM dbo.CLT00001
WHERE
	CLT00001.I_ALLCLM = '1J004142175'
SELECT * FROM dbo.CLT00001
WHERE
	CLT00001.N_POL = 'HCA0328650'
2011-02-08
2017-02-08
"Partial info is from CLT 4, but there’re a few columns which we need to join to other tables first (like coverage type). Also in CLT 4 there is no fraud score by claim Director / smart model"
	...
	CAST(Dim_Coverage_Type.T_CVG_TYP AS VARCHAR(42)) AS coverageTypeDescription,
	CAST(CLT00014.C_CVG_TYP AS CHAR(4)) AS coverageTypeCode,
	...

	C_LOSS_TYP_GRP exists in a dummy/temp table in the Dev db,
		created by Vivek Pandiyan
		
		
		
		
		
*/
--USE ClaimSearch_Prod
--'ClaimSearch_Prod.dbo.Dim_Loss_Type'

--USE ClaimSearch_DEV
--USE CSDataScience
--USE CSDataScience
--USE ClaimSearch_Prod

--SELECT DISTINCT
--	COLUMNS.TABLE_SCHEMA + '.' + COLUMNS.TABLE_NAME
--FROM
--	INFORMATION_SCHEMA.COLUMNS
--WHERE
--	COLUMNS.COLUMN_NAME LIKE '%LOSS_TYP_GRP%'
--ORDER BY
--	COLUMNS.TABLE_SCHEMA + '.' + COLUMNS.TABLE_NAME
	
----'dbo.[i59047_delete2]'



--SELECT TOP 10 * FROM dbo.FS_HO_ModelVisualization_CLM_Test_Set

--SELECT
--	SCHEMA_NAME(all_objects.SCHEMA_ID) + '.' + all_objects.name AS qualifiedObjectName,
--	*
--FROM	
--	sys.all_objects
--WHERE
--	OBJECT_DEFINITION(all_objects.object_id) LIKE '%ASSURANT.FS_HO_ASSURANT_CLT1%'


--SELECT
--	*
--FROM
--	INFORMATION_SCHEMA.TABLES
--WHERE
--	TABLES.TABLE_NAME LIKE '%LOSS%GRP%'
	
--SELECT TOP 100
--	*
--FROM
--	dbo.Lookup_CD_LOB

--'dbo.i59047_delete2'
--'dbo.i59047_delete3'