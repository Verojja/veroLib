/***********************************************
WorkItem: ISCCINTEL-3266
Date: 2018-11-26
Author: Robert David Warner
Description: Object for exposing uniform Display layer information for VSC Usage.

			 Performance: No current notes.
************************************************/
CREATE VIEW cs_dw.v_ipu_audit
AS 
	SELECT
		/*--auditid, /*No NULLs*/*/
		userid,/*most are CHAR5, however it's possible that the datatype is for supporting userIds from disperate systems.*/
		username/*Source object is VARCHAR(30) but that seems small*/
		userjobclassification,
		usercompanycode,/*99.96% identical to companyCode*/
		companycode,
		companyname,/*Customer_lvl0 from V_MM_Hierarchy*/
		groupcode,/*lvl3 from V_MM_Hierarchy*/
		groupname,/*Customer_lvl3 from V_MM_Hierarchy*/
		officecode,
		officelocationfordisplay,
		ipaddress,/*largest observed LEN is "15" but I remember something about legit multi-ipAddresses in our searches RDW 20191104*/
		claimsearchid,/*4/5 of the time this field is NULL*/
		reportid,
		accesscategory,/*access description "action"*/
		accessmessage,/*customer facing access message*/
		searchcriteria,
		accessurl, /*MaxObserved 167*/
		accessdate
		/*dateInserted,*/
		/*deltaDate DATE*/
	FROM
		cs_dw.ipu_audit;