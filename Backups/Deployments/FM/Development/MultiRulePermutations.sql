SELECT
				#PendingClaimDataToInsert.elementalClaimId,
				#PendingClaimDataToInsert.uniqueInstanceValue, 
				CASE
					WHEN /*includes 5; CLOSED-Claim-Toggle*/
						fireMarshallStatus.fmQualificationRequirmentSetId IN (
							7,8, 10,11, 14,15, 16,17, 18,19, 22,23, 24,25, 26,27,
							28,29, 30,31, 32,33, 34,35, 37,38
						)
					THEN
						CASE
							WHEN 
								#PendingClaimDataToInsert.claimiIsOpenByCoverage = 1
							THEN/*ClaimIsOpen, observe 5-grouped-rule*/
								CASE
									WHEN /*Observe LossDescription rule AND estimate$\Stlment$ rule*/
										fireMarshallStatus.fmQualificationRequirmentSetId BETWEEN 20 and 35
									THEN
										CASE
											WHEN
												ISNULL(InnerCLT0001A.T_LOSS_DSC,'Fire') NOT IN (
													'Fire',
													'',
													'blank'
												)
												AND ISNULL(InnerCLT0001A.T_LOSS_DSC_EXT,'Fire') NOT IN (
													'Fire',
													'',
													'blank'
												)
											THEN
												1
											WHEN
												fireMarshallStatus.fmQualificationRequirmentSetId BETWEEN 20 and 35
											THEN
												1
											ELSE
												1
										END
									WHEN /*Observe LossDescription rule*/
										fireMarshallStatus.fmQualificationRequirmentSetId BETWEEN 37 and 38
									THEN
										CASE
											WHEN
									ELSE
										1
								END
											
							ELSE/*ClaimIsClosed, ignore 5-grouped-rule*/
								'Pending '
						END
					ELSE
						1
				END AS fireMarshallStatus
				INTO #PendingClaimWithException
			FROM
				#PendingClaimDataToInsert
				INNER JOIN dbo.FireMarshalController
					ON #PendingClaimDataToInsert.lossStateCode = FireMarshalController.fmState
			WHERE
				FireMarshalController.fmStateStatusCode = 'A'
				AND FireMarshalController.fmQualificationRequirmentSetId NOT IN
				(
					0,4
				)
			