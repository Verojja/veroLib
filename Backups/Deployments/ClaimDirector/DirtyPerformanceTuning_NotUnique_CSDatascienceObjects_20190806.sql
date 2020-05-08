BEGIN TRANSACTION

	
CREATE NONCLUSTERED INDEX NIX_FS_PA_Matching_DL_IALLCMN
	ON CDP.FS_PA_Matching_DL (I_ALLCLM)
	INCLUDE(
		[I_NM_ADR], [name], [Date_Insert], [C_ROLE], [FIRST_THIRD_PARTY_flag], [C_POL_TYP], [I_ALLCLM_MTCH], [I_NM_ADR_MTCH], [C_MTCH_RSN], [Date_Insert_MTCH],
		[matched_party_name], [matched_party_C_ROLE], [matched_party_FIRST_THIRD_PARTY_flag], [matched_party_fraud_tag], [matched_CLM_day_diff], [matched_CLM_C_POL_TYP], [matched_CLM_C_LOSS_TYP], [matched_CLM_date_of_loss], [matched_CLM_I_CUST], [matched_CLM_fraud_tag],
		[matched_DL_number], [DL_issuing_state]
	);
--EXEC sp_help 'CDP.FS_PA_Matching_DL';

CREATE NONCLUSTERED INDEX NIX_FS_PA_Matching_Entity_IALLCMN
	ON CDP.FS_PA_Matching_Entity (I_ALLCLM)
	INCLUDE(
		[I_NM_ADR], [name], [Date_Insert], [C_ROLE], [FIRST_THIRD_PARTY_flag], [C_POL_TYP], [I_ALLCLM_MTCH], [I_NM_ADR_MTCH], [C_MTCH_RSN], [Date_Insert_MTCH],
		[matched_party_name], [matched_party_C_ROLE], [matched_party_FIRST_THIRD_PARTY_flag], [matched_party_fraud_tag], [matched_CLM_day_diff], [matched_CLM_C_POL_TYP], [matched_CLM_C_LOSS_TYP], [matched_CLM_date_of_loss], [matched_CLM_I_CUST], [matched_CLM_fraud_tag],
		[matched_party_C_CVG_TYP]
	);
--EXEC sp_help 'CDP.FS_PA_Matching_Entity';


CREATE NONCLUSTERED INDEX NIX_FS_PA_Matching_SSN_IALLCMN
	ON CDP.FS_PA_Matching_SSN (I_ALLCLM)
	INCLUDE(
		[I_NM_ADR], [name], [Date_Insert], [C_ROLE], [FIRST_THIRD_PARTY_flag], [C_POL_TYP], [I_ALLCLM_MTCH], [I_NM_ADR_MTCH], [C_MTCH_RSN], [Date_Insert_MTCH],
		[matched_party_name], [matched_party_C_ROLE], [matched_party_FIRST_THIRD_PARTY_flag], [matched_party_fraud_tag], [matched_CLM_day_diff], [matched_CLM_C_POL_TYP], [matched_CLM_C_LOSS_TYP], [matched_CLM_date_of_loss], [matched_CLM_I_CUST], [matched_CLM_fraud_tag],
		[matched_SSN]
	);
--EXEC sp_help 'CDP.FS_PA_Matching_SSN';


CREATE NONCLUSTERED INDEX NIX_FS_PA_Matching_VIN_IALLCMN
	ON CDP.FS_PA_Matching_VIN (I_ALLCLM)
	INCLUDE (
		[I_NM_ADR], [name], [Date_Insert], [C_ROLE], [FIRST_THIRD_PARTY_flag], [C_POL_TYP], [I_ALLCLM_MTCH], [I_NM_ADR_MTCH], [C_MTCH_RSN], [Date_Insert_MTCH],
		[matched_party_name], [matched_party_C_ROLE], [matched_party_FIRST_THIRD_PARTY_flag], [matched_party_fraud_tag], [matched_CLM_day_diff], [matched_CLM_C_POL_TYP], [matched_CLM_C_LOSS_TYP], [matched_CLM_date_of_loss], [matched_CLM_I_CUST], [matched_CLM_fraud_tag],
		[matched_party_C_CVG_TYP], [N_VIN]
	);
EXEC sp_help 'CDP.FS_PA_Matching_VIN';

CREATE NONCLUSTERED INDEX NIX_FS_PA_Claim_IALLCMN
	ON CDP.FS_PA_Claim (I_ALLCLM)
	INCLUDE (
		[I_CUST], [N_POL], [D_OCUR], [H_OCUR], [C_LOL_ZIP], [Date_Insert], [C_LOSS_TYP], [C_POL_TYP], [D_RCV], [D_POL_INCP],
		[D_POL_EXPIR], [T_LOL_STR1], [T_LOL_STR2], [T_LOSS_DSC], [F_POLC_RPT], [F_ACDT_WITN], [F_HIT_AND_RUN], [F_SGL_VEH_ACDT], [C_VEH_OPER_REL], [C_LOL_ST_ALPH_RF],
		[F_fraud_tag], [data_group]
	);
--EXEC sp_help 'CDP.FS_PA_Claim';

CREATE NONCLUSTERED INDEX NIX_FS_PA_Match_joined_Intellicorp_IALLCMN
	ON CDP.FS_PA_Match_joined_Intellicorp (I_ALLCLM)
	INCLUDE(
		[Date_Insert], [name], [PersonID], [CaseNo],
		/*[StateOfRecord] !! can't be included since datatype TEXT,*/
		[FileDate],
		/*[ChargeDesc] !! can't be included since datatype TEXT,*/
		/*[Disposition] !! can't be included since datatype TEXT,*/
		[RecordType], [Intelli_Fraud],
		[guilty]
	);
--EXEC sp_help 'CDP.FS_PA_Match_joined_Intellicorp';

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;