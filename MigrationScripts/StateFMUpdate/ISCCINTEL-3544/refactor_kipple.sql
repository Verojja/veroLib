***********************************************
WorkItem: ISCCINTELACE-99
Date: 2020-02-13
Author: Robert David Warner
Description: Included in the post-StateFM deploy is a refactor for how GIM rows are uniquely identified.
				Refactor proposes use of Verisk [I_ALLCLM and I_NMADR combination] (composite key) to match records across tables.
					Additionally, structural reorganization of the table(s) or view(s) for clarity.
			
			Performance: No current notes.
************************************************/ 

/*Claim.claimId*/
/*Claim.isoClaimId*/
/*Claim.isActive*/
/*dateInserted*/
existingElementalClaimId

Claim.deltaDate = @dateInserted

					InvolvedParty.deltaDate = @dateInserted /*guaranteed update, but better solution not realistic at this time.*/
 Claim.deltaDate <> @dateInserted /*guaranteed update, but better solution not realistic at this time.*/



 #IPAMData
 /*Set Logging Variables for Current Step_Start*/
			SELECT
				@stepId = 4,
				@stepDescription = 'DeprecateOrphanISOClaimData_LOL',
				@stepStartDateTime = GETDATE();

			UPDATE dbo.Address WITH (TABLOCKX)
				SET
					Address.isActive = CAST(0 AS BIT),
					Address.deltaDate = @dateInserted
			FROM
				#LocationOfLossData AS SOURCE
			WHERE
				Address.isoClaimId = SOURCE.isoClaimId
				AND Address.isLocationOfLoss = CAST(1 AS BIT)
				AND Address.isActive = CAST(1 AS BIT)
				AND Address.deltaDate <> @dateInserted;

			/*Set Logging Variables for Current Step_End_Success*/
			SELECT
				@stepEndDateTime = GETDATE(),
				@recordsAffected = ROWCOUNT_BIG(),
				@isSuccessful = 1,
				@stepExecutionNotes = NULL;

			/*Log Activity*/
			INSERT INTO dbo.AddressActivityLog
			(
				productCode,
				sourceDateTime,
				executionDateTime,
				stepId,
				stepDescription,
				stepStartDateTime,
				stepEndDateTime,
				recordsAffected,
				isSuccessful,
				stepExecutionNotes
			)
			SELECT
				@productCode,
				@sourceDateTime,
				@executionDateTime,
				@stepId,
				@stepDescription,
				@stepStartDateTime,
				@stepEndDateTime,
				@recordsAffected,
				@isSuccessful,
				@stepExecutionNotes;




				SELECT TOP 10
	FireMarshalExtract.isoFileNumber,
	COUNT(*)
FROM
	dbo.FireMarshalExtract
GROUP BY
	FireMarshalExtract.isoFileNumber
--HAVING
--	COUNT(*) > 1
ORDER BY
	COUNT(*) DESC
	
	/*
	isoFileNumber	(No column name)
7D003630389	13
7E004302170	11
5I004241534	10
1E004059621	7
5F003908554	7
6O004339433	7
9C004403283	7
3B004271867	6
1H004523331	6
3G004833241	6
	*/