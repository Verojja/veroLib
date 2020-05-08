BEGIN TRANSACTION

USE ClaimSearch_Dev
--DECLARE @todayDate DATE = CAST('20191101' AS DATE);
DECLARE @todayDate DATE = GETDATE();

UPDATE dbo.FireMarshalController
	SET
		FireMarshalController.projectedGenerationDate = CAST('20190501' AS DATE)
WHERE
	FireMarshalController.fmStateCode IN
	(
		'NM',
		'ND',
		'MA',
		'WV',
		'AK',
		'MI',
		'MT',
		'KY',
		'KS',
		'TN',
		'ID',
		'DE',
		'NH',
		'NE',
		'WA'
	);
	
UPDATE dbo.FireMarshalController
	SET
		FireMarshalController.projectedGenerationDate = CAST('20190401' AS DATE)
WHERE
	FireMarshalController.fmStateCode IN
	(
		'AL',
		'GA'
		
	);
		
SELECT
	FireMarshalController.fmState,
	FireMarshalController.projectedGenerationDate,
	FireMarshalController.frequencyCode
FROM
	dbo.FireMarshalController
WHERE
	FireMarshalController.fmStateStatusCode = 'A'

	
SELECT
	FireMarshalController.fmState,
	FireMarshalController.projectedGenerationDate,
	FireMarshalController.frequencyCode,
	ProjectedDateToIncrementApply.projectedDateIncrementValue,
	DATEADD(
		MONTH,
		ProjectedDateToIncrementApply.projectedDateIncrementValue,
		COALESCE(PreviousDate.mostRecentGenerationDate, FireMarshalController.projectedGenerationDate)
	) AS newDateTest
FROM
	dbo.FireMarshalController
	CROSS APPLY 
	(
		SELECT /*BehaviorLogic for daily\weekly\yearly NOT YET IMPLIMENTED*/
			CASE
				WHEN
					FireMarshalController.frequencyCode = 'Q'
					AND MONTH(FireMarshalController.projectedGenerationDate) < 4 
				THEN
					3
				WHEN
					FireMarshalController.frequencyCode = 'Q'
				THEN
					4
				WHEN
					FireMarshalController.frequencyCode = 'M'
				THEN
					1
				ELSE
					1
			END AS projectedDateIncrementValue
	) AS ProjectedDateToIncrementApply
	LEFT OUTER JOIN (
		SELECT
			FireMarshalClaimSendHistory.lossStateCode AS fmState,
			MAX(FireMarshalClaimSendHistory.fireMarshallDate) AS mostRecentGenerationDate
		FROM
			dbo.FireMarshalClaimSendHistory
		GROUP BY
			FireMarshalClaimSendHistory.lossStateCode
	) PreviousDate
		ON FireMarshalController.fmState = PreviousDate.fmState
WHERE
	ISNULL(FireMarshalController.projectedGenerationDate,CAST('99990101' AS DATE))< @todayDate
	AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('00010101' AS DATE)) <> DATEADD(
		MONTH,
		ProjectedDateToIncrementApply.projectedDateIncrementValue,
		COALESCE(PreviousDate.mostRecentGenerationDate, FireMarshalController.projectedGenerationDate)
	)

--DECLARE @FireMarshalControllerRowsToUpdate TABLE
--	(
--		fmState CHAR(2),
--		fmQualificationRequirmentSetId SMALLINT,
--		fmStateStatusCode CHAR(1),
--		frequencyCode CHAR(1),
--		projectedGenerationDate DATE,
--		receivesPrint BIT,
--		receivesFTP BIT,
--		receivesEmail BIT,
--		fmContactFirstName VARCHAR(100),
--		fmContactMiddleName VARCHAR(100),
--		fmContactLastName VARCHAR(100),
--		fmContactSuffixName VARCHAR(15),
--		fmContactDeptartmentName VARCHAR(100),
--		fmContactDivisionName VARCHAR(100),
--		fmContactDeliveryAddressLine1 VARCHAR(100),
--		fmContactDeliveryAddressLine2 VARCHAR(100),
--		fmContactDeliveryCity VARCHAR(100),
--		fmContactDeliveryStateCode CHAR(2),
--		fmContactZipCode VARCHAR(10),
--		fmContactTitleName VARCHAR(50),
--		fmContactSalutation VARCHAR(15),
--		dateInserted DATETIME2(0),
--		endDate DATETIME2(0)
--	);
	DECLARE @dateInserted DATETIME2(0) = GETDATE();
	UPDATE dbo.FireMarshalController
		SET
			FireMarshalController.endDate = @dateInserted	
		OUTPUT
			DELETED.fmState,
			DELETED.fmQualificationRequirmentSetId,
			DELETED.fmStateStatusCode,
			DELETED.frequencyCode,
			DATEADD(
				MONTH,
				ProjectedDateToIncrementApply.projectedDateIncrementValue,
				COALESCE(PreviousDate.mostRecentGenerationDate, DELETED.projectedGenerationDate)
			) /*new projectedGenerationDate*/,
			DELETED.receivesPrint,
			DELETED.receivesFTP,
			DELETED.receivesEmail,
			DELETED.fmContactFirstName,
			DELETED.fmContactMiddleName,
			DELETED.fmContactLastName,
			DELETED.fmContactSuffixName,
			DELETED.fmContactDeptartmentName,
			DELETED.fmContactDivisionName,
			DELETED.fmContactDeliveryAddressLine1,
			DELETED.fmContactDeliveryAddressLine2,
			DELETED.fmContactDeliveryCity,
			DELETED.fmContactDeliveryStateCode,
			DELETED.fmContactZipCode,
			DELETED.fmContactTitleName,
			DELETED.fmContactSalutation,
			@dateInserted,
			DELETED.endDate
		INTO dbo.FireMarshalController
		(
			fmState,
			fmQualificationRequirmentSetId,
			fmStateStatusCode,
			frequencyCode,
			projectedGenerationDate,
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
	FROM
		dbo.FireMarshalController
		CROSS APPLY 
		(
			SELECT /*BehaviorLogic for daily\weekly\yearly NOT YET IMPLIMENTED*/
				CASE
					WHEN
						FireMarshalController.frequencyCode = 'Q'
						AND MONTH(FireMarshalController.projectedGenerationDate) < 4 
					THEN
						3
					WHEN
						FireMarshalController.frequencyCode = 'Q'
					THEN
						4
					WHEN
						FireMarshalController.frequencyCode = 'M'
					THEN
						1
					ELSE
						1
				END AS projectedDateIncrementValue
		) AS ProjectedDateToIncrementApply
		LEFT OUTER JOIN (
			SELECT
				FireMarshalClaimSendHistory.lossStateCode AS fmState,
				MAX(FireMarshalClaimSendHistory.fireMarshallDate) AS mostRecentGenerationDate
			FROM
				dbo.FireMarshalClaimSendHistory
			GROUP BY
				FireMarshalClaimSendHistory.lossStateCode
		) PreviousDate
			ON FireMarshalController.fmState = PreviousDate.fmState
	WHERE
		FireMarshalController.endDate IS NULL
		AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('99990101' AS DATE))< @dateInserted
		AND ISNULL(FireMarshalController.projectedGenerationDate,CAST('00010101' AS DATE)) <> DATEADD(
			MONTH,
			ProjectedDateToIncrementApply.projectedDateIncrementValue,
			COALESCE(PreviousDate.mostRecentGenerationDate, FireMarshalController.projectedGenerationDate)
		);
	
SELECT
	*
FROM
	dbo.FireMarshalController
WHERE
	FireMarshalController.fmStateStatusCode = 'A'
ORDER BY
	FireMarshalController.fmState,
	FireMarshalController.dateInserted
ROLLBACK TRANSACTION