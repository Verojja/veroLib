/***********************************************
WorkItem: ISCCINTEL-ISCCINTELACE-45
Date: 2020-02-11
Author: Animesh Choudhary and Robert David Warner
Description: Mechanism for permissions control for dashboard access.
				/*Consideration: NoHlth*/
				
				SELECT * FROM iso21.v_mbr_ofc LIMIT 100;
				SELECT  *
				--i_cust, i_usr
				FROM natb.pyrtab LIMIT 100;
				SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNS.column_name 
				v_mbr_elig.c_univ_ty = 'ALLOW_CLAIMS_CLOUD'
			AND v_mbr_elig.i_inscomp = var_companycode
			AND v_mbr_elig.c_univ = 'Y';
				ROLLBACK
			Performance: No notes.
			
************************************************/
CREATE VIEW cs_dw.v_vcsusageaccess
AS 
	SELECT
		CompanyDetails.memcomp AS i_cust,
		UserDetails.i_usr
	FROM
		natb.insmtab AS CompanyDetails
		INNER JOIN natb.pyrtab AS UserDetails
			ON CompanyDetails.memcomp = UserDetails.i_cust
	WHERE
		CompanyDetails.c_rtrn_blk_ctg != 'HLT';
DO LANGUAGE plpgsql
$permissions_view$
BEGIN
	IF EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssft'
		)
		THEN
			RAISE NOTICE 'GRANTing SELECT on cs_dw.v_vcsusageaccess TO pidpgcssft';
			GRANT SELECT
			ON cs_dw.v_vcsusageaccess
			TO pidpgcssft;
	ELSEIF
		EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssfa'
		)
		THEN
			RAISE NOTICE 'GRANTing SELECT on cs_dw.v_vcsusageaccess TO pidpgcssfa';
			GRANT SELECT
			ON cs_dw.v_vcsusageaccess
			TO pidpgcssfa;
			GRANT SELECT
			ON FUNCTION cs_dw.v_vcsusageaccess
			TO cs_dw_dev;
	ELSE
		/*EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssfp'
		)
		THEN*/
			RAISE NOTICE 'GRANTing SELECT on cs_dw.v_vcsusageaccess TO pidpgcssfp';
			GRANT SELECT
			ON cs_dw.v_vcsusageaccess
			TO pidpgcssfp;
			GRANT SELECT
			ON FUNCTION cs_dw.v_vcsusageaccess
			TO cs_dw_dev;
	END IF;
END $permissions_view$;