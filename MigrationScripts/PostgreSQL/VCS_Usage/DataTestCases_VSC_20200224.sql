/*VCS Test Cases:
	T1: Action-Reason Limit.
		"Data in VCS Dash should NOT match action reasons outside of MATCH_REPORT"
	T2: Username consistency.
		"Username should be consistent with UserId"
	T3: companyname consistency.
		"Companyname should be consistent with UserId"
	T4: officeLocationForDispaly consistency.
		"OfficeLocationForDispaly should be consistent with UserId"
--SELECT * FROM cs_dw.v_insuranceprovideruseraudit LIMIT 1;
*/
DO LANGUAGE plpgsql
$test1_actionreason$
BEGIN
	IF NOT EXISTS(
		SELECT NULL
		FROM cs_dw.v_insuranceprovideruseraudit
		WHERE v_insuranceprovideruseraudit.accesscategory <> 'MATCH_REPORT_S3'
	) THEN
		RAISE NOTICE 'Pass test1: action reasons only MATCH_REPORT.';
	ELSE
		RAISE NOTICE 'FAIL test1: action reasons exist that are NOT MATCH_REPORT:';
	END IF;
END $test1_actionreason$;
DO LANGUAGE plpgsql
$test2_username$
BEGIN
	IF NOT EXISTS(
		SELECT COUNT(DISTINCT username)
		FROM cs_dw.v_insuranceprovideruseraudit
		GROUP BY v_insuranceprovideruseraudit.userid
		HAVING COUNT(DISTINCT username) >1
	) THEN
		RAISE NOTICE 'Pass test2: userName consistent accross all userId.';
	ELSE
		RAISE NOTICE 'FAIL test2: multiple usernames for single userId:';
	END IF;
END $test2_username$;
DO LANGUAGE plpgsql
$test3_compname$
BEGIN
	IF NOT EXISTS(
		SELECT COUNT(DISTINCT companyname)
		FROM cs_dw.v_insuranceprovideruseraudit
		GROUP BY v_insuranceprovideruseraudit.companycode
		HAVING COUNT(DISTINCT companyname) >1
	) THEN
		RAISE NOTICE 'Pass test3: companyname consistent accross all companycode.';
	ELSE
		RAISE NOTICE 'FAIL test3: multiple companyname for single companycode:';
	END IF;
END $test3_compname$;
DO LANGUAGE plpgsql
$test4_officLoc$
BEGIN
	IF NOT EXISTS(
		SELECT COUNT(DISTINCT officelocationfordisplay)
		FROM cs_dw.v_insuranceprovideruseraudit
		GROUP BY
			v_insuranceprovideruseraudit.officecode,
			v_insuranceprovideruseraudit.companycode
		HAVING COUNT(DISTINCT officelocationfordisplay) >1
	) THEN
		RAISE NOTICE 'Pass test4: officelocationfordisplay consistent accross all companycode & officecode.';
	ELSE
		RAISE NOTICE 'FAIL test4: multiple officelocationfordisplay for single companycode & officecode combination:';
	END IF;
END $test4_officLoc$;
