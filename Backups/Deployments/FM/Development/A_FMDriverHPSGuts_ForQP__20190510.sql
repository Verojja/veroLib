SELECT @@TRANCOUNT
DECLARE @internalTransactionCount TINYINT = 0;
IF (@@TRANCOUNT = 0)
BEGIN
	BEGIN TRANSACTION;
	SET @internalTransactionCount = 1;
END
DECLARE
	@dateFilterParam DATETIME2(0) = NULL,
	@dateInserted DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now*/
	@executionDateTime DATETIME2(0) = GETDATE(), /*This value remains consistent for all steps, so it can be set now. Identical to @dateInserted, but using a different name to benefit conceptual intuitiveness*/
	@productCode VARCHAR(50) = 'FM', /*This value remains consistent for all steps, so it can be set now*/
	@sourceDateTime DATETIME2(0), /*This value remains consistent for all steps, but it's value is set in the next section*/
	@defaultNumberOfYearsToLookBack TINYINT = 5, /*This value remains consistent for variableInit step, initialize now*/
		
	@stepId TINYINT,
	@stepDescription VARCHAR(1000),
	@stepStartDateTime DATETIME2(0),
	@stepEndDateTime DATETIME2(0),
	@recordsAffected BIGINT,
	@isSuccessful BIT,
	@stepExecutionNotes VARCHAR(1000);

/*Set Logging Variables for execution*/
SELECT
	@dateFilterParam = CASE
		WHEN
			CAST(CAST(YEAR(GETDATE())-@defaultNumberOfYearsToLookBack AS CHAR(4)) + '0101' AS DATE) < ISNULL(@dateFilterParam,CAST('99990101' AS DATE))
		THEN
			CAST(CAST(YEAR(GETDATE())-@defaultNumberOfYearsToLookBack AS CHAR(4)) + '0101' AS DATE)
		ELSE
			@dateFilterParam
	END
/* Deprecated old style for Setting Logging Variables for execution
SELECT
	@dateFilterParam = CAST /*Casting as Date currently necesary due to system's datatype inconsistancy*/
	(
		COALESCE
		(
			@dateFilterParam, /*always prioritize using a provided dateFilterParam*/
			MAX(FireMarshalDriverActivityLog.executionDateTime), /*in the absence of a provided dateFilterParam, use the last successful ExecutionDateTime*/
			CAST('2008-01-01' AS DATETIME2(0)) /*if the log table is empty (IE: first run), use the earliest recorded date for address data*/
		) AS DATE
	)
FROM
	dbo.FireMarshalDriverActivityLog
WHERE
	FireMarshalDriverActivityLog.stepId = 100 /*Default stepId for finalStep of UpdateInsert HSP*/
	AND FireMarshalDriverActivityLog.isSuccessful = 1;
*/
SET @sourceDateTime = @dateFilterParam;

/*Set Logging Variables for Current Step_Start*/
SELECT
	@stepId = 1,
	@stepDescription = 'CaptureFMClaimDataToImport',
	@stepStartDateTime = GETDATE();

SELECT
	UniqueFireMarshalClaim.I_ALLCLM AS isoClaimId,
	/*
		FLAG_LEG_FIRE AS isLegacyFire,
		FLAG_UF_FIRE AS isUFFire,
		FLAG_UF_LightExpl AS isUFLightExplosion,
		FLAG_UF_AUTO AS isUFAuto,
	*/
	CASE
		WHEN
			UniqueFireMarshalClaim.FLAG_VALID_LOSS_DESC > 0
		THEN
			CAST(1 AS BIT)
		ELSE
			CAST(0 AS BIT)
	END AS adhocIncludesValidLossDescription,
	CASE
		WHEN
			UniqueFireMarshalClaim.FLAG_VALID_LOSS_DESC_LENGTH > 0
		THEN
			CAST(1 AS BIT)
		ELSE
			CAST(0 AS BIT)
	END AS adhocIncludesValidLossDescriptionLength,
	CASE
		WHEN
			UniqueFireMarshalClaim.FLAG_VALID_LOSS_ESTIMATE > 0
		THEN
			CAST(1 AS BIT)
		ELSE
			CAST(0 AS BIT)
	END AS adhocIncludesValidLossEstimate,
	CASE
		WHEN
			UniqueFireMarshalClaim.FLAG_VALID_LOSS_SETTLEMENT > 0
		THEN
			CAST(1 AS BIT)
		ELSE
			CAST(0 AS BIT)
	END AS adhocIncludesValidLossSettlement,
	
	CASE
		WHEN
			(
				UniqueFireMarshalClaim.[FLAG_LEG_FIRE] > 0 /*is LegacyFire*/
				OR UniqueFireMarshalClaim.[FLAG_UF_FIRE] > 0 /*is UFFire*/
			) 
		THEN
			CASE
				WHEN
					UniqueFireMarshalClaim.[FLAG_UF_AUTO] = 0 /*NOT UF Auto*/
					AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] > 0 /*is UF Light\Exp*/
				THEN
					CAST(2 AS TINYINT) /*Basic + Lighting/Fire*/
				WHEN
					UniqueFireMarshalClaim.[FLAG_UF_AUTO] > 0 /*is UF Auto*/
					AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] = 0 /*NOT UF Light\Exp*/
				THEN
					CAST(3 AS TINYINT) /*Basic + Auto*/
				WHEN
					UniqueFireMarshalClaim.[FLAG_UF_AUTO] > 0 /*is UF Auto*/
					AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] > 0 /*is UF Light\Exp*/
				THEN
					CAST(4 AS TINYINT) /*Basic + Auto + Lighting/Fire*/
				ELSE
					/*Implicitly:
						UniqueFireMarshalClaim.[FLAG_UF_AUTO] = 0 /*NOT UF Auto*/ 
						AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] = 0 /*NOT UF Light\Exp*/
					*/	
					CAST(1 AS TINYINT) /*ONLY Basic*/
			END
		WHEN /*Implicitly NOT Basic*/
			UniqueFireMarshalClaim.[FLAG_UF_AUTO] = 0
			AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] > 0
		THEN
			CAST(5 AS TINYINT) /*Lighting/Fire only*/
		WHEN
			UniqueFireMarshalClaim.[FLAG_UF_AUTO] > 0
			AND UniqueFireMarshalClaim.[FLAG_UF_LightExpl] = 0
		THEN
			CAST(6 AS TINYINT) /*Auto Fire only*/
		ELSE
			CAST(NULL AS TINYINT)
	END AS adhocFMPerspectiveTypeId,
	CASE 
		WHEN
			UniqueInstanceStateStatus.F_FM = 'A'
		THEN
			CAST('A' AS CHAR(1)) /*A - Active Participating State (Requires FireMarshal to recieve Fire claims in some format; Print,email,FTP, dash)*/
		WHEN
			UniqueInstanceStateStatus.F_FM = 'I'
		THEN
			CAST('I' AS CHAR(1)) /*I - Inactive (No current requirement or FireMarshal subscription. New states would need to be added to the FMSchedule helper table)*/
		WHEN
			UniqueInstanceStateStatus.F_FM = 'P'
		THEN
			CAST('P' AS CHAR(1)) /*P - Passive Participating State (Simply doing business with Verisk\ClaimSearch satisfies any requirements by the FireMarshal)*/
		ELSE
			CAST(NULL AS CHAR(1)) /*Not a valid state for consideration*/
	END AS adhocFMStateStatusCode
	INTO #FMClaimData
FROM
	(
		SELECT
			InnerCLT0001A.I_ALLCLM,
			InnerCLT0001A.C_LOL_ST_ALPH,
			/*1*/SUM(
				CASE
					WHEN
						InnerCLT0001A.C_CLM_SRCE = 'P'
						AND InnerCLT0001A.C_LOSS_TYP = 'FIRE'
					THEN
						1
					ELSE
						0
				END
			) AS FLAG_LEG_FIRE,
			/*2*/SUM(
				CASE
					WHEN
						InnerCLT0001A.C_CLM_SRCE = 'U'
						AND InnerCLT00014.C_LOSS_TYP = 'FIRE'
						AND InnerCLT0001A.F_AUTO = 'N'
						AND InnerCLT0001A.F_APD = 'N'
					THEN
						1
					ELSE
						0
				END
			) AS FLAG_UF_FIRE,
			/*3*/SUM(
				CASE
					WHEN
						InnerCLT0001A.C_CLM_SRCE = 'U'
						AND InnerCLT00014.C_LOSS_TYP IN ('LGHT','EXPL')
					THEN
						1
					ELSE
						0
				END
			) AS FLAG_UF_LightExpl,
			/*4*/SUM(
				CASE
					WHEN
						InnerCLT0001A.C_CLM_SRCE = 'U'
						AND InnerCLT00014.C_LOSS_TYP = 'FIRE'
						AND (
								(
									(
										InnerCLT0001A.F_AUTO = 'Y'
										OR InnerCLT0001A.F_APD = 'Y'
									)
									AND
									(
										InnerCLT0001A.F_PROP = 'N'
										OR InnerCLT0001A.F_CSLTY = 'N'
									)
								)
								OR 
								(
									InnerCLT0001A.C_POL_TYP in ('PAPP','CAPP','PPMH') 
									AND InnerCLT00014.C_CVG_TYP in ('COMP','OTAU') 
								)
							)
					THEN
						1
					ELSE
						0
				END
			) AS FLAG_UF_AUTO,
			/*5*/SUM(
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
					ELSE
						0
					END
				) AS FLAG_VALID_LOSS_DESC,
			/*6*/SUM(
				CASE
					WHEN
						LEN(InnerCLT0001A.T_LOSS_DSC) >6
					THEN
						1
					ELSE 0
				END
			) AS FLAG_VALID_LOSS_DESC_LENGTH,
			/*7*/SUM(
				CASE
					WHEN
						InnerCLT00014.A_EST_LOSS > 0
					THEN
						1
					ELSE
						0
				END
			) AS FLAG_VALID_LOSS_ESTIMATE,
			/*8*/SUM(
				CASE
					WHEN
						InnerCLT00014.A_STTLMT > 0
					THEN
						1
					ELSE
						0
				END
			) AS FLAG_VALID_LOSS_SETTLEMENT
			/*
			,ROW_NUMBER() OVER(
				PARTITION BY
					InnerCLT0001A.I_ALLCLM
				ORDER BY
					InnerCLT0001A.Date_Insert DESC
			) uniqueInstanceValue
			--*/
		FROM
			ClaimSearch_Prod.dbo.CLT0001A AS InnerCLT0001A
			INNER JOIN ClaimSearch_Prod.dbo.CLT00014 AS InnerCLT00014
				ON InnerCLT0001A.I_ALLCLM = InnerCLT00014.I_ALLCLM
		WHERE
			InnerCLT0001A.date_insert >= CAST(
				REPLACE(
					CAST(
						@dateFilterParam
						AS VARCHAR(10)
					),
				'-','')
				AS INT
			)
			AND
			(
				(
					InnerCLT0001A.C_CLM_SRCE = 'P'
					AND InnerCLT0001A.C_LOSS_TYP = 'FIRE'
				)
				OR 
				(
					InnerCLT0001A.C_CLM_SRCE = 'U'
					AND InnerCLT00014.C_LOSS_TYP IN
					(
						'FIRE','LGHT','EXPL'
					)
				)
				
			)
			AND InnerCLT0001A.C_LOL_ST_ALPH IS NOT NULL
		GROUP BY
			InnerCLT0001A.I_ALLCLM,
			InnerCLT0001A.C_LOL_ST_ALPH
	) AS UniqueFireMarshalClaim
	LEFT OUTER JOIN (
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
			dbo.Dashboard_COM_State
	) AS UniqueInstanceStateStatus
		ON UniqueFireMarshalClaim.C_LOL_ST_ALPH = UniqueInstanceStateStatus.[state]
			/*AND UniqueFireMarshalClaim.uniqueInstanceValue = UniqueInstanceStateStatus.uniqueInstanceValue*/
WHERE
	--ISNULL(UniqueInstanceStateStatus.uniqueInstanceValue,1) = 1; /*ISNULL CHECK DUE TO L.O.J.*/
	UniqueInstanceStateStatus.uniqueInstanceValue = 1
	
--/*Performance Consideration:
--	Potentially, created a Filtered Unique Index on the TempTable for
--*/

--/*Set Logging Variables for Current Step_End_Success*/
--SELECT
--	@stepEndDateTime = GETDATE(),
--	@recordsAffected = ROWCOUNT_BIG(),
--	@isSuccessful = 1,
--	@stepExecutionNotes = NULL;

--/*Log Activity*/
--INSERT INTO dbo.FireMarshalDriverActivityLog
--(
--	productCode,
--	sourceDateTime,
--	executionDateTime,
--	stepId,
--	stepDescription,
--	stepStartDateTime,
--	stepEndDateTime,
--	recordsAffected,
--	isSuccessful,
--	stepExecutionNotes
--)
--SELECT
--	@productCode,
--	@sourceDateTime,
--	@executionDateTime,
--	@stepId,
--	@stepDescription,
--	@stepStartDateTime,
--	@stepEndDateTime,
--	@recordsAffected,
--	@isSuccessful,
--	@stepExecutionNotes;

--/*Set Logging Variables for Current Step_Start*/
--SELECT
--	@stepId = 100,
--	@stepDescription = 'MergeIntoFMDriverData,Update,Deactivate,Insert',
--	@stepStartDateTime = GETDATE();

--MERGE INTO dbo.FireMarshalDriver AS TARGET
--USING
--(
--	SELECT
--		#FMClaimData.isoClaimId,
--		/*
--		#FMClaimData.isLegacyFire,
--		#FMClaimData.isUFFire,
--		#FMClaimData.isUFLightExplosion,
--		#FMClaimData.isUFAuto,
--		--*/
--		#FMClaimData.adhocIncludesValidLossDescription,
--		#FMClaimData.adhocIncludesValidLossDescriptionLength,
--		#FMClaimData.adhocIncludesValidLossEstimate,
--		#FMClaimData.adhocIncludesValidLossSettlement,
--		#FMClaimData.adhocFMPerspectiveTypeId,
--		#FMClaimData.adhocFMStateStatusCode
--	FROM
--		#FMClaimData
--) AS SOURCE
--ON
--	TARGET.isoClaimId = SOURCE.isoClaimId
--WHEN MATCHED AND 
--	(
--		/*isoClaimId*/
--		TARGET.adhocIncludesValidLossDescription <> SOURCE.adhocIncludesValidLossDescription
--		OR TARGET.adhocIncludesValidLossDescriptionLength <> SOURCE.adhocIncludesValidLossDescriptionLength
--		OR TARGET.adhocIncludesValidLossEstimate <> SOURCE.adhocIncludesValidLossEstimate
--		OR TARGET.adhocIncludesValidLossSettlement <> SOURCE.adhocIncludesValidLossSettlement
--		OR TARGET.isActive <> 1
--		OR ISNULL(TARGET.adhocFMPerspectiveTypeId,255) <> ISNULL(SOURCE.adhocFMPerspectiveTypeId,255)
--		OR ISNULL(TARGET.adhocFMStateStatusCode,'~') <> ISNULL(SOURCE.adhocFMStateStatusCode,'~')
--		/*dateInserted*/
--	)
--THEN
--	UPDATE
--	SET
--		TARGET.adhocIncludesValidLossDescription = SOURCE.adhocIncludesValidLossDescription,
--		TARGET.adhocIncludesValidLossDescriptionLength = SOURCE.adhocIncludesValidLossDescriptionLength,
--		TARGET.adhocIncludesValidLossEstimate = SOURCE.adhocIncludesValidLossEstimate,
--		TARGET.adhocIncludesValidLossSettlement = SOURCE.adhocIncludesValidLossSettlement,
--		TARGET.isActive = 1,
--		TARGET.adhocFMPerspectiveTypeId = SOURCE.adhocFMPerspectiveTypeId,
--		TARGET.adhocFMStateStatusCode = SOURCE.adhocFMStateStatusCode,
--		TARGET.dateInserted = @dateInserted
--WHEN NOT MATCHED BY SOURCE
--THEN
--	UPDATE
--	SET
--		TARGET.isActive = 0
--WHEN NOT MATCHED BY TARGET
--THEN
--	INSERT
--	(
--		isoClaimId,
--		/*
--		isLegacyFire,
--		isUFFire,
--		isUFLightExplosion,
--		isUFAuto,
--		--*/
--		adhocIncludesValidLossDescription,
--		adhocIncludesValidLossDescriptionLength,
--		adhocIncludesValidLossEstimate,
--		adhocIncludesValidLossSettlement,
--		isActive,
--		adhocFMPerspectiveTypeId,
--		adhocFMStateStatusCode,
--		dateInserted
--	)
--	VALUES
--	(
--		SOURCE.isoClaimId,
--		/*
--		SOURCE.isLegacyFire,
--		SOURCE.isUFFire,
--		SOURCE.isUFLightExplosion,
--		SOURCE.isUFAuto,
--		--*/
--		SOURCE.adhocIncludesValidLossDescription,
--		SOURCE.adhocIncludesValidLossDescriptionLength,
--		SOURCE.adhocIncludesValidLossEstimate,
--		SOURCE.adhocIncludesValidLossSettlement,
--		1 /*isActive*/,
--		SOURCE.adhocFMPerspectiveTypeId,
--		SOURCE.adhocFMStateStatusCode,
--		@dateInserted
--	);

--SELECT
--	@stepEndDateTime = GETDATE(),
--	@recordsAffected = ROWCOUNT_BIG(),
--	@isSuccessful = 1,
--	@stepExecutionNotes = NULL;

--/*Log Activity*/
--INSERT INTO dbo.FireMarshalDriverActivityLog
--(
--	productCode,
--	sourceDateTime,
--	executionDateTime,
--	stepId,
--	stepDescription,
--	stepStartDateTime,
--	stepEndDateTime,
--	recordsAffected,
--	isSuccessful,
--	stepExecutionNotes
--)
--SELECT
--	@productCode,
--	@sourceDateTime,
--	@executionDateTime,
--	@stepId,
--	@stepDescription,
--	@stepStartDateTime,
--	@stepEndDateTime,
--	@recordsAffected,
--	@isSuccessful,
--	@stepExecutionNotes;

--IF (@internalTransactionCount = 1)
--BEGIN
--	COMMIT TRANSACTION;
--END

ROLLBACK TRANSACTION