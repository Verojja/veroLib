SET NOEXEC OFF;

USE ClaimSearch_Dev
--USE ClaimSearch_Prod

BEGIN TRANSACTION

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2018-12-05
Author: Daniel Ravaglia and Robert David Warner
Description: FM Contact list and controller for Claim-Qualification-Requirement-Set
				and DistributionSubscription bitflags.
			  
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.FireMarshalController
(
	fmState CHAR(2) NOT NULL,
	fmQualificationRequirmentSetId SMALLINT NOT NULL,
	fmStateStatusCode CHAR(1) NOT NULL
	/*
		A - Active Participating State (Requires FireMarshal to recieve Fire claims in some format; Print,email,FTP, dash),
		I - Inactive (No current requirement or FireMarshal subscription. New states would need to have their projectedGenerationDate\frequency columns updated in this table),
		P - Passive Participating State (Simply doing business with Verisk\ClaimSearch satisfies any requirements by the FireMarshal)
	*/,
	frequencyCode CHAR(1) NOT NULL
	/*
		D - Daily
		W - Weekly
		M - Monthly
		Q - Quarterly
		Y - Yearly
	*/,
	projectedGenerationDate DATE NULL,
	
	receivesPrint BIT NOT NULL,
	receivesFTP BIT NOT NULL,
	receivesEmail BIT NOT NULL,
	
	fmContactFirstName VARCHAR(100) NULL,
	fmContactMiddleName VARCHAR(100) NULL,
	fmContactLastName VARCHAR(100) NULL,
	fmContactSuffixName VARCHAR(15) NULL,
	fmContactDeptartmentName VARCHAR(100) NULL,
	fmContactDivisionName VARCHAR(100) NULL,
	fmContactDeliveryAddressLine1 VARCHAR(100) NULL,
	fmContactDeliveryAddressLine2 VARCHAR(100) NULL,
	fmContactDeliveryCity VARCHAR(100) NULL,
	fmContactDeliveryStateCode CHAR(2) NULL,
	fmContactZipCode VARCHAR(10) NULL,
	fmContactTitleName VARCHAR(50) NULL,
	fmContactSalutation VARCHAR(15) NULL,
	
	dateInserted DATETIME2(0) NOT NULL, 
	endDate DATETIME2(0) NULL
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

DECLARE @dateInserted DATETIME2(0) = GETDATE();

SELECT
	CAST(UniqueInstanceStateStatus.[state] AS CHAR(2)) AS fmState,
	CAST(ISNULL(FMContactListValueSet.fmQualificationRequirmentSetId, 0) AS SMALLINT) AS fmQualificationRequirmentSetId,
	CASE 
		WHEN
			UniqueInstanceStateStatus.F_FM = 'A'
		THEN
			CAST('A' AS CHAR(1)) /*A - Active Participating State (Requires FireMarshal to recieve Fire claims in some format; Print,email,FTP, dash)*/
		WHEN
			UniqueInstanceStateStatus.F_FM = 'I'
		THEN
			CAST('I' AS CHAR(1)) /*I - Inactive (No current requirement or FireMarshal subscription. New states would need to have their projectedGenerationDate\frequency columns updated in this table)*/
		WHEN
			UniqueInstanceStateStatus.F_FM = 'P'
		THEN
			CAST('P' AS CHAR(1)) /*P - Passive Participating State (Simply doing business with Verisk\ClaimSearch satisfies any requirements by the FireMarshal)*/
		ELSE
			CAST(NULL AS CHAR(1)) /*Not a valid state for consideration*/
	END AS fmStateStatusCode,
	CAST(ISNULL(FMContactListValueSet.frequencyCode,'M') AS CHAR(1))AS frequencyCode,
	
	ISNULL(FMContactListValueSet.receivesPrint, 0) AS receivesPrint,
	ISNULL(FMContactListValueSet.receivesFTP, 0) AS receivesFTP,
	ISNULL(FMContactListValueSet.receivesEmail, 0) AS receivesEmail,
	
	FMContactListValueSet.firstName AS fmContactFirstName,
	FMContactListValueSet.middleName AS fmContactMiddleName,
	FMContactListValueSet.lastName AS fmContactLastName,
	FMContactListValueSet.suffixName AS fmContactSuffixName,
	FMContactListValueSet.dept AS fmContactDeptartmentName,
	FMContactListValueSet.div AS fmContactDivisionName,
	FMContactListValueSet.fMDeliveryAddressLine1 AS fmContactDeliveryAddressLine1,
	FMContactListValueSet.fMDeliveryAddressLine2 AS fmContactDeliveryAddressLine2,
	FMContactListValueSet.fMDeliveryCity AS fmContactDeliveryCity,
	FMContactListValueSet.fMDeliveryStateCode AS fmContactDeliveryStateCode,
	FMContactListValueSet.zipCode AS fmContactZipCode,
	FMContactListValueSet.title AS fmContactTitleName,
	FMContactListValueSet.salutation AS fmContactSalutation,
	
	@dateInserted AS dateInserted,
	CAST(NULL AS DATETIME2(0)) AS endDate
	INTO #FireMarshalControllerDataToInsert
FROM
	(
		SELECT
			Dashboard_COM_State.[state],
			Dashboard_COM_State.F_FM,
			ROW_NUMBER() OVER
			(
				PARTITION BY
					LTRIM(RTRIM(Dashboard_COM_State.[state]))
				ORDER BY
					Dashboard_COM_State.DAte_Insert DESC
			) AS uniqueInstanceValue
		FROM
			[ClaimSearch_Prod].dbo.Dashboard_COM_State
	) AS UniqueInstanceStateStatus
	LEFT OUTER JOIN (
		VALUES
			/*excelformula: ="("&IF(ISBLANK(A2),"NULL","'"&A2&"'")&", "&IF(ISBLANK(B2),"NULL","'"&B2&"'")&", "&IF(ISBLANK(C2),"NULL","'"&C2&"'")&", "&IF(ISBLANK(D2),"NULL",*/
			('AK', 'David', NULL, 'Tyler', NULL, 'DEPARTMENT OF PUBLIC SAFETY', 'DIVISION OF FIRE PREVENTION ', '5700 E, TUDOR ROAD', NULL, 'ANCHORAGE', 'AK', '99507', 'FIRE MARSHALL', 'MR', 0, 0, 1, 1, /*frequencyCode*/ 'M'),
			('AL', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, /*reqSetId*/0, 0, 1, 1, /*frequencyCode*/ 'Q'),
			/*NOTE: I removed 'CHIEF' from dept col for AZ*/('AZ', 'BOBBY', NULL, 'RUIZ', NULL, NULL, 'CITY OF PHOENIX FIRE DEPT', '520 W VAN BUREN AVE', NULL, 'PHOENIX', 'AZ', '85003', 'CHIEF', 'MR', /*reqSetId*/0, 0, 0, 0, /*frequencyCode*/ NULL), 
			('DE', 'GROVER', 'P', 'INGLE', NULL, 'OFFICE OF STATE FIRE MARSHAL', NULL, 'RD 2  BOX 166A', NULL, 'DOVER', 'DE', '19901', NULL, 'MR', /*reqSetId*/0, 1, 0, 0, /*frequencyCode*/ 'M'),
			('GA', 'DOUGLAS', 'R', 'BURDICK', NULL, 'PROPERTY INSURANCE LOSS REGISTER', NULL, '700 NEW BRUNSWICK AVENUE', NULL, 'RAHWAY', 'NJ', '7065', NULL, 'MR', /*reqSetId*/14, 0, 1, 0, /*frequencyCode*/ 'Q'),
			('ID', 'DON', NULL, 'MC''COY', NULL, 'OFFICE OF THE STATE FIRE MARSHALL', 'DEPARTMENT OF INSURANCE', '700 WEST STATE STREET', NULL, 'BOISE', 'ID', '83720', NULL, 'MR', /*reqSetId*/0, 0, 1, 0, /*frequencyCode*/ 'M'),
			('KS', 'GALE', NULL, 'HAAG', NULL, 'STATE FIRE MARSHAL', NULL, '700 SW JACKSON', 'SUITE 600', 'TOPEKA', 'KS', '66603-3714', NULL, 'MR', /*reqSetId*/21, 1, 0, 0, /*frequencyCode*/ 'M'),
			('KY', 'DAVID', 'L', 'MANLEY', NULL, 'CHIEF DEPUTY FIRE MARSHAL', 'DEPARTMENT OF HOUSING', '127 OFFICE BUILDING', 'U.S. 127 SOUTH', 'FRANKFORT', 'KY', '40601', NULL, 'MR', /*reqSetId*/0, 0, 1, 0, /*frequencyCode*/ 'M'),
			('MD', 'ROBERT', 'B', 'THOMAS', 'JR', 'DEPUTY CHIEF STATE FIRE MARSHAL', NULL, '1201 REISTERSTOWN ROAD', NULL, 'PIKESVILLE', 'MD', '21208-3899', NULL, 'MR', /*reqSetId*/0, 0, 0, 0, /*frequencyCode*/ NULL),
			('MA', 'JOSEPH', 'A', 'O''KEEFE', 'PE', 'OFFICE OF STATE FIRE MARSHAL', NULL, '1010 COMMONWEALTH AVENUE', NULL, 'BOSTON', 'MA', '2215', NULL, 'MR', /*reqSetId*/0, 0, 1, 0, /*frequencyCode*/ 'M'),
			('MT', NULL, NULL, NULL, NULL, 'FIRE PREVENTION AND INVESTIGATION BUREAU', 'DEPARTMENT OF JUSTICE', 'P.O. BOX 201417', NULL, 'HELENA', NULL, '201417', NULL, NULL, /*reqSetId*/0, 1, 0, 0, /*frequencyCode*/ 'M'),
			('ND', 'ROBERT', NULL, 'ALLEN', NULL, 'OFFICE OF FIRE MARSHALL', NULL, '1929 NORTH WASHINGTON', 'NORTHBROOK MALL', 'BISMARK', 'ND', '58505', NULL, 'MR', /*reqSetId*/0, 1, 0, 0, /*frequencyCode*/ 'M'),
			('NE', 'WALLACE', 'M', 'BARNETT', NULL, 'STATE FIRE MARSHALL', 'STATE FIRE MARSHAL''S OFFICE', 'P.O. BOX 94677', NULL, 'LINCOLN', 'NE', '68509', NULL, 'MR', /*reqSetId*/0, 0, 1, 0, /*frequencyCode*/ 'M'),
			/*NOTE: I moved 'MARTEL' from suffixName to lastName for NH*/('NH', 'BARBARA', 'J', 'MARTEL', NULL, 'STATE FIRE MARSHAL', 'DEPARTMENT OF SAFETY', 'J. H. HAYES BUILDING', NULL, 'CONCORD', 'NH', '3301', NULL, 'MS', /*reqSetId*/0, 0, 1, 0, /*frequencyCode*/ 'M'),
			('NM', 'JAMES', NULL, 'MAXON', NULL, 'FIRE INVESTIGATOR', NULL, '142 WEST PALACE 2ND FLOOR', 'BOKUM BUILDING INVESTIGATION DIV', 'SANTA FE', 'NM', '87501', NULL, 'MR', /*reqSetId*/0, 1, 0, 0, /*frequencyCode*/ 'M'),
			('TN', NULL, NULL, NULL, NULL, 'STATE FIRE MARSHALL', NULL, '500 JAMES ROBERTSON PKWY', 'THIRD FLOOR, VOLUNTEER PLAZA', 'NASHVILLE', 'TN', '37219', NULL, 'SIR', /*reqSetId*/0, 0, 1, 0, /*frequencyCode*/ 'M'),
			('VT', 'BRUCE', NULL, 'LANG', NULL, 'ASSISTANT DEPUTY FIRE MARSHAL', 'OFFICE OF STATE FIRE MARSHAL', 'DEPARTMENT OF PUBLIC SAFETY', '103 SOUTH MAIN STREET', 'WATERBURY', 'VT', '5676', 'LT', 'MR', /*reqSetId*/0, 0, 0, 0, /*frequencyCode*/ NULL),
			('WA', 'R', 'G', 'MARQUARDT', NULL, 'OFFICE OF INSURANCE COMMISSIONER AND STATE FIRE MARSHAL', NULL, 'INSURANCE BUILDING AQ-21', NULL, 'OLYMPIA', 'WA', '98504', 'COMMISSIONER', NULL, /*reqSetId*/0, 0, 1, 0, /*frequencyCode*/ 'M'),
			('WV', 'STERLING', NULL, 'LEWIS', 'JR', 'WV STATE FIRE COMMISSION', NULL, '1207 QUARRIER STREET ', 'SUITE 202', 'CHARLESTON', 'WV', '25301', NULL, 'MR', /*reqSetId*/0, 1, 0, 0, /*frequencyCode*/ 'M')	
	) AS FMContactListValueSet (fmState, firstName, middleName, lastName, suffixName, dept, div, fMDeliveryAddressLine1, fMDeliveryAddressLine2, fMDeliveryCity, fMDeliveryStateCode, zipCode, title, salutation, fmQualificationRequirmentSetId, receivesPrint, receivesFTP, receivesEmail, frequencyCode)
		ON UniqueInstanceStateStatus.[state] = FMContactListValueSet.fmState
WHERE
	UniqueInstanceStateStatus.uniqueInstanceValue = 1;
	
INSERT INTO dbo.FireMarshalController
	(
		fmState,
		fmQualificationRequirmentSetId,
		fmStateStatusCode,
		frequencyCode,
		receivesPrint,
		receivesFTP,
		receivesEmail,
		fmContactFirstName,
		fmContactMiddleName,
		fmContactLastName,
		fmContactSuffixName,
		fmContactDeptartmentName,
		fmContactDivisionName,
		fmContactDeliveryAddressLine1,
		fmContactDeliveryAddressLine2,
		fmContactDeliveryCity,
		fmContactDeliveryStateCode,
		fmContactZipCode,
		fmContactTitleName,
		fmContactSalutation,
		dateInserted,
		endDate		
	)
SELECT
	#FireMarshalControllerDataToInsert.fmState,
	#FireMarshalControllerDataToInsert.fmQualificationRequirmentSetId,
	#FireMarshalControllerDataToInsert.fmStateStatusCode,
	#FireMarshalControllerDataToInsert.frequencyCode,
	#FireMarshalControllerDataToInsert.receivesPrint,
	#FireMarshalControllerDataToInsert.receivesFTP,
	#FireMarshalControllerDataToInsert.receivesEmail,
	#FireMarshalControllerDataToInsert.fmContactFirstName,
	#FireMarshalControllerDataToInsert.fmContactMiddleName,
	#FireMarshalControllerDataToInsert.fmContactLastName,
	#FireMarshalControllerDataToInsert.fmContactSuffixName,
	#FireMarshalControllerDataToInsert.fmContactDeptartmentName,
	#FireMarshalControllerDataToInsert.fmContactDivisionName,
	#FireMarshalControllerDataToInsert.fmContactDeliveryAddressLine1,
	#FireMarshalControllerDataToInsert.fmContactDeliveryAddressLine2,
	#FireMarshalControllerDataToInsert.fmContactDeliveryCity,
	#FireMarshalControllerDataToInsert.fmContactDeliveryStateCode,
	#FireMarshalControllerDataToInsert.fmContactZipCode,
	#FireMarshalControllerDataToInsert.fmContactTitleName,
	#FireMarshalControllerDataToInsert.fmContactSalutation,
	#FireMarshalControllerDataToInsert.dateInserted,
	#FireMarshalControllerDataToInsert.endDate
FROM
	#FireMarshalControllerDataToInsert
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*
SELECT * FROM dbo.FireMarshalController
--WHERE
--	FireMarshalController.fmStateStatusCode IN 
--	(
--		'A',
--		'P'
--	)
ORDER BY
	fmStateStatusCode,
	fmState
--*/

--EXEC sp_help 'dbo.FireMarshalController'
--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;
/*

(54 row(s) affected)

(54 row(s) affected)
COMMIT

*/