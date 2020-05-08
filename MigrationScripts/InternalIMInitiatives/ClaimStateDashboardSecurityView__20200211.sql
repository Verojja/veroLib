BEGIN TRANSACTION;

--DO LANGUAGE plpgsql
--$viewCreate_bisecurityvalidation$
--BEGIN
--	IF EXISTS(
--		SELECT * FROM information_schema.tables
--		WHERE
--			tables.table_name = 'v_bisecurityvalidation'
--			AND tables.table_schema = 'cs_dw'
--	) THEN
--		RAISE NOTICE 'VIEW: v_bisecurityvalidation already exists. Dropping for re-create, (recommended) due to how [REPLACE] syntax works.';
--		DROP VIEW cs_dw.v_bisecurityvalidation;
--	END IF;
/***********************************************
WorkItem: ISCCINTEL-ISCCINTELACE-45
Date: 2020-02-11
Author: Animesh Choudhary and Robert David Warner
Description: Mechanism for permissions control for dashboard access.

			Performance: No notes.

************************************************/
--CREATE VIEW cs_dw.v_bisecurityvalidation
--AS 
--	SELECT
--		v_cust_secur.i_cust,
--		v_cust_secur.jobclass,
--		v_cust_secur.jobcode,
--		v_cust_secur.vehfunc,
--		v_cust_secur.sixfunc
--		INTO /*variables*/
--			var_companycode,
--			var_jobclass,
--			var_jobcode,
--			var_vehfunc,
--			var_sixfunc
--		FROM
--			claims.iso21.v_cust_secur
--		WHERE
--			v_cust_secur.i_usr = param_username;
--END $viewCreate_bisecurityvalidation$;

CREATE OR REPLACE FUNCTION cs_dw.hsp_casecurityaccess (
	param_username VARCHAR(100) /*DEVNOTE:Do you want to have a default value?? IE NULL or something??*/
)
RETURNS TABLE (
	result_companycode VARCHAR(4)
)
LANGUAGE plpgsql
AS $hsp_csa_body$
	DECLARE
		var_userelig INT;
		var_mbrelig INT;
		var_ismaincompany INT;
		var_companycode VARCHAR(4);
		var_secondarycompanycode VARCHAR(4);
		var_jobclass VARCHAR(1);
		var_jobcode VARCHAR(2);
		var_vehfunc VARCHAR(2);
		var_sixfunc VARCHAR(2);
	BEGIN
		SELECT
			v_cust_secur.i_cust,
			v_cust_secur.jobclass,
			v_cust_secur.jobcode,
			v_cust_secur.vehfunc,
			v_cust_secur.sixfunc
			INTO /*variables*/
				var_companycode,
				var_jobclass,
				var_jobcode,
				var_vehfunc,
				var_sixfunc
			FROM
				claims.iso21.v_cust_secur
			WHERE
				v_cust_secur.i_usr = param_username;

		SELECT
			COUNT(1)
			INTO /*variables*/
				var_userelig
		FROM
			/*DEVNOTE:Typically you do NOT want to do 3-part-qualified-objectnames unless you truely do want a cross-database querry.
			IE: iso21.v_usr_elig vs claims.iso21.v_usr_elig
			 please confirm that is the desire here.*/
			claims.iso21.v_usr_elig
		WHERE/*DEV: always consider whether or not your params/etc. are going to be comparing against indexed columns. if they are consider doing an EXPLICIT data convert to take advantage of the performance.*/
			v_usr_elig.c_univ_typ='ALLOW_CLAIMS_CLOUD'
			AND v_usr_elig.i_usr = SUBSTRING(param_username,2,4)
			AND v_usr_elig.c_univ ='Y';

		SELECT
			COUNT(1)
			INTO /*variables*/
				var_mbrelig
		FROM
			/*DEVNOTE:Typically you do NOT want to do 3-part-qualified-objectnames unless you truely do want a cross-database querry.
			IE: iso21.v_mbr_elig vs claims.iso21.v_mbr_elig
			 please confirm that is the desire here.*/
			claims.iso21.v_mbr_elig
		WHERE
			v_mbr_elig.c_univ_ty p = 'ALLOW_CLAIMS_CLOUD'
			AND v_mbr_elig.i_inscomp = var_companycode
			AND v_mbr_elig.c_univ = 'Y';

		IF (var_userelig = 1
			OR (
				var_mbrelig = 1 AND (
					var_vehfunc != 'N'
					OR (
						var_sixfunc != 'NN'
						AND var_sixfunc != 'IN'
					)
				)
			)
		) THEN
			IF (
				var_companycode = 'Z996'
				AND var_jobcode = '92'
			) THEN
				RETURN QUERY
				SELECT
					v_mbr_ofc.memcomp AS companycode
				FROM
					iso21.v_mbr_ofc;
			ELSEIF (
				var_companycode = 'Z996'
				OR (
					var_companycode != 'Z996'
					AND var_jobclass = 'S'
				)
			) THEN
				SELECT
					COUNT(1)
					INTO /*variables*/
						var_ismaincompany
				FROM
					iso21.v_mbr_ofc
				WHERE
					v_mbr_ofc.memcomp=var_companycode
					AND (
						main_affiliate = var_companycode
						OR main_affiliate = ''
					);
				IF (var_ismaincompany = 0)
				THEN
					SELECT
						v_mbr_ofc.main_affiliate
						INTO /*variables*/
						var_secondarycompanycode
					FROM
						iso21.v_mbr_ofc
					WHERE
						v_mbr_ofc.memcomp = var_companycode;

					RETURN QUERY
					SELECT
						v_mbr_ofc.memcomp AS companycode
					FROM
						iso21.v_mbr_ofc
					WHERE
						v_mbr_ofc.main_affiliate = var_secondarycompanycode
					
					UNION
					
					SELECT
						v_mbr_ofc.memcomp AS CompanyCode
					FROM
						iso21.v_mbr_ofc
					WHERE
						v_mbr_ofc.main_affiliate IN (
							select memcomp from ISO21.V_MBR_OFC where main_affiliate=SecondaryCompanyCode
						)
					union
					select memcomp as CompanyCode from ISO21.V_MBR_OFC where main_affiliate in
						(select memcomp from ISO21.V_MBR_OFC where main_affiliate in
						(select memcomp from ISO21.V_MBR_OFC where main_affiliate=SecondaryCompanyCode))
					except
						select SecondaryCompanyCode as CompanyCode;
				ELSE
					return query select CompanyCode as CompanyCode
								union
								select memcomp as CompanyCode from ISO21.V_MBR_OFC where main_affiliate=CompanyCode
								union
								select memcomp as CompanyCode from ISO21.V_MBR_OFC where main_affiliate in
									(select memcomp from ISO21.V_MBR_OFC where main_affiliate=CompanyCode);
				END IF;
			ELSE
				return query select CAST ('Y' AS varchar(4))  as CompanyCode;
			END IF;
		ELSE
			return query select CAST ('X' AS varchar(4)) as CompanyCode;
		END IF;
	END $hsp_csa_body$;
LANGUAGE plpgsql;
/***********************************************
WorkItem: ISCCINTEL-1184
Date: 2020-02-05
Author: Robert David Warner
Description: Dynamic Permissions block for Granting BI role read-access,
				based on PostgreSQL environment
************************************************/
DO LANGUAGE plpgsql
$permissions$
BEGIN
	IF EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssft'
		)
		THEN
			RAISE NOTICE 'GRANTing SELECT on claims.cs_dw.v_insuranceprovideruseraudit TO pidpgcssft';
			GRANT SELECT
			ON claims.cs_dw.v_insuranceprovideruseraudit
			TO pidpgcssft;
	ELSEIF
		EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssfa'
		)
		THEN
			RAISE NOTICE 'GRANTing SELECT on claims.cs_dw.v_insuranceprovideruseraudit TO pidpgcssfa';
			GRANT SELECT
			ON claims.cs_dw.v_insuranceprovideruseraudit
			TO pidpgcssfa;
	ELSE
		/*EXISTS(
			SELECT NULL FROM pg_roles WHERE rolName = 'pidpgcssfp'
		)
		THEN*/
			RAISE NOTICE 'GRANTing SELECT on claims.cs_dw.v_insuranceprovideruseraudit TO pidpgcssfp';
			GRANT SELECT
			ON claims.cs_dw.v_insuranceprovideruseraudit
			TO pidpgcssfp;
	END IF;
END $permissions$;

ROLLBACK TRANSACTION;
--COMMIT TRANSACTION;