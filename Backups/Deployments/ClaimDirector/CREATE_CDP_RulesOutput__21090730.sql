SET NOEXEC OFF;

USE CSDataScience;

BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
SELECT
	*
	INTO CDP.ReasonCodeOutput_Filtered_20190726
FROM
	ClaimSearch_Dev.dbo.reason_codes_v1_20190726
WHERE
	reason_codes_v1_20190726.RSN_F_SSN_multiENT = 1
	OR reason_codes_v1_20190726.RSN_X_NLIC_LICEnt_avg = 1
	OR reason_codes_v1_20190726.RSN_X_NLIC_LICEnt_min_fp = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_NLIC_avg = 1
	OR reason_codes_v1_20190726.RSN_X_NSSN_SSNEnt_max_tp = 1
	OR reason_codes_v1_20190726.RSN_N_EFFDT_TO_LOSS = 1
	OR reason_codes_v1_20190726.RSN_NIGHT_FLAG = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_AAA_avg = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_APA_max = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_BODI_max = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_Clmtag_max = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_Clmtag_max_fp = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_CUSPA_max = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_IAA_min_fp = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_SPtag_max = 1
	OR reason_codes_v1_20190726.RSN_NVIN_FRD_fp_max = 1
	OR reason_codes_v1_20190726.RSN_NVIN_FPA_fp_max = 1
	OR reason_codes_v1_20190726.RSN_NVIN_CVGCOLL_fp_max = 1
	OR reason_codes_v1_20190726.RSN_Ent_Count_fp_max = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_D2LSAA_min_fp = 1
	OR reason_codes_v1_20190726.RSN_X_NENT_D2LSPL_min_tp = 1
	OR reason_codes_v1_20190726.RSN_Intelli_Fraud = 1
	OR reason_codes_v1_20190726.RSN_N_CRMN_glty = 1
	OR reason_codes_v1_20190726.RSN_N_CRMN_RT_C = 1
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--EXEC sp_help 'CDP.ReasonCodeOutput_Filtered_20190726'
ALTER TABLE CDP.ReasonCodeOutput_Filtered_20190726
	ADD CONSTRAINT PK_ReasonCodeOutput_I_ALLCLM
		PRIMARY KEY CLUSTERED (I_ALLCLM)
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
COMMIT TRANSACTION
