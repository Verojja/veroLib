--SELECT * FROM cs_dw.hsp_vcsusageaccess('CO61Q')
WHERE hsp_vcsusageaccess.i_cust = 'Z996'
SELECT
	UserDetails.i_usr,
	CompanyDetails.memcomp AS i_cust
FROM
	natb.insmtab AS CompanyDetails
	INNER JOIN natb.pyrtab AS UserDetails
		ON CompanyDetails.memcomp = UserDetails.i_cust
WHERE
	CompanyDetails.c_rtrn_blk_ctg != 'HLT'
	AND UserDetails.i_usr = 'CO61Q'
ORDER BY
	UserDetails.i_usr;
	
	
SELECT DISTINCT
	v_insuranceprovideruseraudit.userjobclassification
FROM
	cs_dw.v_insuranceprovideruseraudit;


SELECT
	*
FROM
	INFORMATION_SCHEMA.COLUMNS
WHERE
	COLUMNS.column_name LIKE '%job%class%'
	
	
	
	SELECT
	UserDetails.i_usr,
	CompanyDetails.memcomp AS i_cust
FROM
	natb.insmtab AS CompanyDetails
	INNER JOIN natb.pyrtab AS UserDetails
		ON CompanyDetails.memcomp = UserDetails.i_cust
WHERE
	CompanyDetails.c_rtrn_blk_ctg != 'HLT'
	AND UserDetails.i_usr = 'CO61Q'
ORDER BY
	UserDetails.i_usr;
	
	SELECT cs_dw.hsp_vcsusageaccess('CO61Q')
	
	 

	 SELECT
	pyrtab.jobclass,
	pyrtab.*
FROM
	natb.pyrtab
	INNER JOIN (
		SELECT
			InternalPyrtab.empno,
			InternalPyrtab.jobclass,
			ROW_NUMBER() OVER
			(
				PARTITION BY
					InternalPyrtab.jobclass
				ORDER BY
					InternalPyrtab.empno
			) AS uniqueInstanceNumber
		FROM natb.pyrtab AS InternalPyrtab
	) AS PartitionedPyrtab
		ON pyrtab.empno = PartitionedPyrtab.empno
		AND pyrtab.jobclass = PartitionedPyrtab.jobclass
WHERE
	PartitionedPyrtab.uniqueInstanceNumber = 1
;
