BEGIN TRANSACTION

	
SELECT-- DISTINCT
	--FS_PA_Matching_Entity.I_ALLCLM,
	--FS_PA_Matching_Entity.I_NM_ADR,
	--FS_PA_Matching_Entity.name,
	--FS_PA_Matching_Entity.Date_Insert,
	--FS_PA_Matching_Entity.C_ROLE,
	--FS_PA_Matching_Entity.C_MTCH_RSN,
	--FS_PA_Matching_Entity.I_ALLCLM_MTCH,
	--FS_PA_Matching_Entity.I_NM_ADR_MTCH,
	--FS_PA_Matching_Entity.matched_party_name,
	--FS_PA_Matching_Entity.Date_Insert_MTCH,
	--FS_PA_Matching_Entity.matched_party_C_ROLE,
	--FS_PA_Matching_Entity.matched_CLM_fraud_tag,
	--FS_PA_Matching_Entity.FIRST_THIRD_PARTY_flag,
	--FS_PA_Matching_Entity.C_POL_TYP,
	--FS_PA_Matching_Entity.Date_Insert_MTCH,
	--FS_PA_Matching_Entity.matched_party_FIRST_THIRD_PARTY_flag,
	--FS_PA_Matching_Entity.matched_CLM_day_diff,
	--FS_PA_Matching_Entity.matched_CLM_C_POL_TYP,
	--FS_PA_Matching_Entity.matched_CLM_C_LOSS_TYP,
	--FS_PA_Matching_Entity.matched_CLM_date_of_loss,
	--FS_PA_Matching_Entity.matched_CLM_I_CUST
FS_PA_Matching_Entity.N_VIN,
		FS_PA_Matching_Entity.I_ALLCLM,
		FS_PA_Matching_Entity.I_NM_ADR,
		FS_PA_Matching_Entity.I_ALLCLM_MTCH,
		FS_PA_Matching_Entity.I_NM_ADR_MTCH,
		FS_PA_Matching_Entity.name,
		FS_PA_Matching_Entity.matched_CLM_C_LOSS_TYP,
		FS_PA_Matching_Entity.matched_party_C_CVG_TYP,*
FROM
	CDP.FS_PA_Matching_VIN_UAT_updated AS FS_PA_Matching_Entity
WHERE
	FS_PA_Matching_Entity.I_ALLCLM = '0C004628155'
	AND FS_PA_Matching_Entity.I_NM_ADR = 1
	AND FS_PA_Matching_Entity.I_ALLCLM_MTCH = '2F002520267'
	AND FS_PA_Matching_Entity.I_NM_ADR_MTCH = 2
	AND FS_PA_Matching_Entity.name = 'GAIL FLOWERS'
	AND FS_PA_Matching_Entity.matched_CLM_C_LOSS_TYP = 'COMP'
	AND FS_PA_Matching_Entity.matched_party_C_CVG_TYP = 'COMP'

/*
I_ALLCLM	I_NM_ADR	I_ALLCLM_MTCH	I_NM_ADR_MTCH	name	matched_CLM_C_LOSS_TYP	matched_party_C_CVG_TYP
0A004742134	1	4B004751692	1	PAUL CRANE	OTAU	COMP
0B004726540	1	6X002730288	1	BRADY JOYNT	COLL	NULL
0C004628155	1	2F002520267	2	GAIL FLOWERS	COMP	COMP
*/

--SELECT TOP 1000 
--	FS_PA_Matching_VIN_UAT_updated.N_VIN,
--		FS_PA_Matching_VIN_UAT_updated.I_ALLCLM,
--		FS_PA_Matching_VIN_UAT_updated.I_NM_ADR,
--		FS_PA_Matching_VIN_UAT_updated.I_ALLCLM_MTCH,
--		FS_PA_Matching_VIN_UAT_updated.I_NM_ADR_MTCH,
--		FS_PA_Matching_VIN_UAT_updated.name,
--		FS_PA_Matching_VIN_UAT_updated.matched_CLM_C_LOSS_TYP,
--		FS_PA_Matching_VIN_UAT_updated.matched_party_C_CVG_TYP,*
--		FROM CDP.FS_PA_Matching_VIN_UAT_updated
--	ORDER BY
--		FS_PA_Matching_VIN_UAT_updated.I_ALLCLM,
--		FS_PA_Matching_VIN_UAT_updated.I_NM_ADR,
--		FS_PA_Matching_VIN_UAT_updated.I_ALLCLM_MTCH,
--		FS_PA_Matching_VIN_UAT_updated.I_NM_ADR_MTCH,
--		FS_PA_Matching_VIN_UAT_updated.name,
--		FS_PA_Matching_VIN_UAT_updated.matched_CLM_C_LOSS_TYP,
--		FS_PA_Matching_VIN_UAT_updated.matched_party_C_CVG_TYP

--SELECT
--	FS_PA_Matching_Entity.I_ALLCLM,
--	FS_PA_Matching_Entity.I_NM_ADR,
--	FS_PA_Matching_Entity.I_ALLCLM_MTCH,
--	FS_PA_Matching_Entity.I_NM_ADR_MTCH,
--	FS_PA_Matching_Entity.name,
--	FS_PA_Matching_Entity.matched_CLM_C_LOSS_TYP,
--	FS_PA_Matching_Entity.matched_party_C_CVG_TYP,
--	COUNT(*)
--FROM
--	CDP.FS_PA_Matching_VIN_UAT_updated AS FS_PA_Matching_Entity
--GROUP BY
--	FS_PA_Matching_Entity.I_ALLCLM,
--	FS_PA_Matching_Entity.I_NM_ADR,
--	FS_PA_Matching_Entity.I_ALLCLM_MTCH,
--	FS_PA_Matching_Entity.I_NM_ADR_MTCH,
--	FS_PA_Matching_Entity.name,
--	FS_PA_Matching_Entity.matched_CLM_C_LOSS_TYP,
--	FS_PA_Matching_Entity.matched_party_C_CVG_TYP
--HAVING
--	COUNT(*) > 1

ROLLBACK TRANSACTION