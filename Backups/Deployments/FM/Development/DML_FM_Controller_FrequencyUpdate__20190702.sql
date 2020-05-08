BEGIN TRANSACTION

--USE ClaimSearch_Dev;
USE ClaimSearch_Prod;

DECLARE @dateInserted DATETIME2(0) = GETDATE();

--SELECT * FROM dbo.FireMarshalController
--WHERE
--	FireMarshalController.endDate IS NULl
--	AND FireMarshalController.fmStateCode IN ('AL','AK')

UPDATE dbo.FireMarshalController
SET
	FireMarshalController.endDate = @dateInserted
OUTPUT
	deleted.fmStateCode,
	deleted.fmQualificationRequirmentSetId,
	deleted.fmStateStatusCode,
	ListSet.newFrequencyCode AS frequencyCode,
	ListSet.newProjectedGenerationDate AS projectedGenerationDate,
	deleted.receivesPrint,
	deleted.receivesFTP,
	deleted.receivesEmail,
	deleted.fmContactFirstName,
	deleted.fmContactMiddleName,
	deleted.fmContactLastName,
	deleted.fmContactSuffixName,
	deleted.fmContactDeptartmentName,
	deleted.fmContactDivisionName,
	deleted.fmContactDeliveryAddressLine1,
	deleted.fmContactDeliveryAddressLine2,
	deleted.fmContactDeliveryCity,
	deleted.fmContactDeliveryStateCode,
	deleted.fmContactZipCode,
	deleted.fmContactTitleName,
	deleted.fmContactSalutation,
	@dateInserted AS dateInserted,
	NULL AS endDate
	INTO dbo.FireMarshalController
FROM
	(
		VALUES
			('AL','2019-07-05','M'),
			('AK','2019-08-05','Q')
	) AS ListSet (fmStateCode, newProjectedGenerationDate, newFrequencyCode)
WHERE
	FireMarshalController.fmStateCode = ListSet.fmStateCode
	AND FireMarshalController.endDate IS NULL

--SELECT * FROM dbo.FireMarshalController
--WHERE
--	FireMarshalController.endDate IS NULl
--	AND FireMarshalController.fmStateCode IN ('AL','AK')
	
--PRINT 'ROLLBACK'; ROLLBACK TRANSACTION;
PRINT 'COMMIT'; COMMIT TRANSACTION