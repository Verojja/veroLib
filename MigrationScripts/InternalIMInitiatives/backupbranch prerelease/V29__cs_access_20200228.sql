BEGIN TRANSACTION;
/* VIEW implimentation solution*/
DO LANGUAGE plpgsql
$viewCreate_bisecurityvalidation$
BEGIN
	IF EXISTS(
		SELECT * FROM information_schema.tables
		WHERE
			tables.table_name = 'v_bisecurityvalidation'
			AND tables.table_schema = 'cs_dw'
	) THEN
		RAISE NOTICE 'VIEW: v_bisecurityvalidation already exists. Dropping for re-create, (recommended) due to how [REPLACE] syntax works.';
		DROP VIEW cs_dw.v_bisecurityvalidation;
	END IF;
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
CREATE VIEW cs_dw.v_bisecurityvalidation
AS 
	SELECT
		/*cmpnyid*/
		/*COUNT(*) /*"78044"*/*/
		i_usr,
		i_cust
	FROM
		natb.pyrtab;
END $viewCreate_bisecurityvalidation$;
DO LANGUAGE plpgsql
$permissions_view$
BEGIN
	IF EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssft'
		)
		THEN
			RAISE NOTICE 'GRANTing SELECT on cs_dw.v_bisecurityvalidation TO pidpgcssft';
			GRANT SELECT
			ON cs_dw.v_bisecurityvalidation
			TO pidpgcssft;
	ELSEIF
		EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssfa'
		)
		THEN
			RAISE NOTICE 'GRANTing SELECT on cs_dw.v_bisecurityvalidation TO pidpgcssfa';
			GRANT SELECT
			ON cs_dw.v_bisecurityvalidation
			TO pidpgcssfa;
	ELSE
		/*EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssfp'
		)
		THEN*/
			RAISE NOTICE 'GRANTing SELECT on cs_dw.v_bisecurityvalidation TO pidpgcssfp';
			GRANT SELECT
			ON cs_dw.v_bisecurityvalidation
			TO pidpgcssfp;
	END IF;
END $permissions_view$;
--ROLLBACK TRANSACTION;
COMMIT TRANSACTION;