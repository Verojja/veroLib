USE ClaimSearch_Dev
SELECT
	DISTINCT [Reporting Status]
FROM
	[dbo].[FMClaimDashboardExtract2]

EXEC sp_help '[dbo].[FMClaimDashboardExtract2]'
USE ClaimSearch_Dev
ISO File Number

Reporting Status
FM Status

Exception Description

FM Date

Open/Closed Status

Date Submitted to ISO

Claim Number

Policy Number

Office Code

Company Name

Customer Code

Affiliate 1 Name

Affiliate 1 Code

Affiliate 2 Name

Affiliate 2 Code


groupDisplayName

groupCode

Loss Address
Loss City
Loss State Full
Loss State
Loss Zip

Loss Description
Date of Loss
Loss Type
Loss Type abbr

Policy Type
Policy Type abbr

Coverage Type
Coverage Type abbr

Estimated Loss
Settlement Amount

Policy Amount

Fire Indicator

SIU Investigation Flag
SIU Company Name
SIU Full Name
SIU Work Number
SIU Cell Number

SELECT 
ISO File Number	Reporting Status	FM Status	Exception Description	FM Date	Open/Closed Status	Date Submitted to ISO	Claim Number	Policy Number	Office Code	Company Name
3H002607767	Sent	Sent		Sep 2009	Claim Open	2009-12-31	03201394EXP1	xxxxxxxxxx159	00018	KENTUCKY FARM BUREAU MUTUAL INSURANCE CO (K076)

SELECT TOP 1000 *
 FROM [dbo].[FMClaimDashboardExtract2]
WHERE
[FMClaimDashboardExtract2].[ISO File Number] IN


SELECT DISTINCT
	FMClaimDashboardExtract2.[Reporting Status]
FROM
	dbo.FMClaimDashboardExtract2
Sent

(


'4Q004384277',
'6I004184724',
'0S004437891',
'3H002607767',
'4L004415942',
'3L004277760')

Pending
Passive
Exception
Inactive
Sent


BEGIN TRANSACTION


UPDATE dbo.FireMarshalController
	SET
		FireMarshalController.projectedGenerationDate =
		
		COALESCE(RecentFireMarshalExport.latestExportGenerationDate,FireMarshalController.projectedGenerationDate
		
		CASE
			WHEN
				dbo.FireMarshalController
			THEN
			ELSE
		END
FROM
	dbo.FireMarshalController
	(
		SELECT
			FireMarshalClaimSendHistory.lossStateCode,
			MAX(FireMarshalClaimSendHistory.fireMarshallDate) AS latestExportGenerationDate
		FROM
			dbo.FireMarshalClaimSendHistory
		GROUP BY
			FireMarshalClaimSendHistory.lossStateCode
	) AS RecentFireMarshalExport
	

SELECT
	CASE
		WHEN
			FireMarshalController.fmStateStatusCode = 'I'
		THEN
			'Inactive'
		WHEN
			FireMarshalController.fmStateStatusCode = 'P'
		THEN
			'Passive'
		WHEN
			FireMarshalController.fmStateStatusCode = 'A'
			AND FireMarshalController.fmQualificationRequirmentSetId IN (
				0 /*All claims are considered GOOD*/,
				4 /*CLOSED Claims are removed from FM-Send (as though BAD)*/
			)
		THEN
			'Pending ' + DATENAME(month,FireMarshalController.projectedGenerationDate)
		ELSE
			'Exception'
		/*''Sent'' reporting status is hardcoded default for FM Claim Rows in Historic object.*/
	END AS reportingStatus,
	FireMarshalController.*
	
FROM
	dbo.FireMarshalController
	

ROLLBACK TRANSACTION