SET NOEXEC OFF;

USE ClaimSearch_Prod;
--USE ClaimSearch_Dev;

/******MSGLog Snippet. Can be added to comment block at end of query after execute for recordkeeping.******/
DECLARE @tab CHAR(1) = CHAR(9);
DECLARE @newLine CHAR(1) = CHAR(13);
DECLARE @currentDBEnv VARCHAR(100) = CAST(@@SERVERNAME + '.' + DB_NAME() AS VARCHAR(100));
DECLARE @currentUser VARCHAR(100) = CAST(CURRENT_USER AS VARCHAR(100));
DECLARE @executeTimestamp VARCHAR(20) = CAST(GETDATE() AS VARCHAR(20));
Print '*****************************************' + @newLine
	+ '*' + @tab + 'Env: ' + 
	+ CASE
	WHEN
		LEN(@currentDBEnv) >=27
	THEN
		@currentDBEnv
	ELSE
		@currentDBEnv + @tab
	END
	+ @tab + '*' +@newLine
	+ '*' + @tab + 'User: ' + @currentUser + @tab + @tab + @tab + @tab + '*' +@newLine
	+ '*' + @tab + 'Time: ' + @executeTimestamp + @tab + @tab + @tab + '*' +@newLine
	+'*****************************************';
/**********************************************************************************************************/
BEGIN TRANSACTION
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
UPDATE DecisionNet.ProductHierarchy
SET
	ProductHierarchy.dataSource = 'manual insert: ' + CAST(ProductHierarchy.dateInserted AS VARCHAR(25))
FROM
	DecisionNet.ProductHierarchy
WHERE
	--ProductHierarchy.dataSource IS NULL
	ProductHierarchy.dateInserted <='20190101';
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
DECLARE @manualDateInsert DATE = GETDATE();
UPDATE DecisionNet.ProductHierarchy
SET
	ProductHierarchy.productHierarchyLvl1 = BusinesRequestedManualPHUpdates_20200122.productHierarchyLvl1,
	ProductHierarchy.productHierarchyLvl2 = BusinesRequestedManualPHUpdates_20200122.productHierarchyLvl2,
	ProductHierarchy.productHierarchyLvl3 = BusinesRequestedManualPHUpdates_20200122.productHierarchyLvl3,
	ProductHierarchy.dateInserted = @manualDateInsert,
	ProductHierarchy.dataSource = 'manual insert: ' + CAST(@manualDateInsert AS VARCHAR(25))
FROM
	DecisionNet.ProductHierarchy
	INNER JOIN (
		VALUES
			('VLR7', '2', 'LPR Picture Search by Batch VIN', 'Public Records', 'Vehicle Sightings', 'Vehicle Location Workbench'),
			('VLR8', '2', 'LPR VIN to Plate Search', 'Public Records', 'Vehicle Sightings', 'Vehicle Location Workbench')
	) BusinesRequestedManualPHUpdates_20200122 (productTransactionCode, groupId, productTransactionDescription, productHierarchyLvl1, productHierarchyLvl2, productHierarchyLvl3)
		ON ProductHierarchy.productTransactionCode = BusinesRequestedManualPHUpdates_20200122.productTransactionCode;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCSDW-290
Date: 2018-08-02
Author: Robert David Warner
Description: Mechanism for data-refresh of the Product Table.
			Originally, the table was being droppped,re-created, reinserted into.
			Current behavior now is Upsert, through Merge syntax.
			dateFilterParam defaults to upsertAgainst last 7 calendar days, but otherwise
			can be set passed into for FULL INSERT/UPSERT.
			
			Adding a mechanism for automated ProductHeirarchy initialization for newly added products.

			Performance: Execution was around 14 seconds, but appears to have jumped to 40. Need to closely monitor.
						Shifted to SELECT INTO vs. INSERT INTO for log-minimalization.
						INSERT LEFT JOIN since perfect set comparison not applicable.
***********************************************
WorkItem: ___________
Date: 2020-01-22
Author: Robert David Warner
Description: Updating mechanism for automated ProductHeirarchy initialization for newly added products;
				Improving Automated-DN-Product-Classifier (ADPC)logic,
				Turning on ADPC for sys-gen classifications.

			Performance: No change from above.
************************************************/
ALTER PROCEDURE DecisionNet.hsp_UpdateInsertProduct
	@dateFilterParam DATE = NULL
AS
BEGIN
	DECLARE @dateInserted DATE = GETDATE(),
			@sourcedate date;
	
	SELECT
		@dateFilterParam =  COALESCE(@dateFilterParam,MAX(sourcedate))
	FROM
		[ClaimSearch_Prod].[DecisionNet].[CS_DecisionNet_Dashboard_Process_Log] WITH(nolock)
	WHERE
		StepNumber IN (
			14, 15
		)
		AND RecordsAffected > 0;
	
	SELECT  @sourcedate = CONVERT(DATE,CONVERT(VARCHAR(10),MAX(date_insert)),112)
	FROM ClaimSearch.CS.CLT00201  WITH(NOLOCK)
	
	
	DECLARE @Log_StepDesc varchar(1000)
		,@Log_RecordsAffected bigint
		,@Log_StartDateTime datetime
		,@ProductCode varchar(2)
		,@Log_StepStatus varchar(100)
		,@Log_EndDateTime datetime 
		,@Log_TimeTaken varchar(10)
		,@Log_ActualProcessedDate date
		
	SET @ProductCode = 'CS';

	IF NOT EXISTS (
		SELECT 1
		FROM
			ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log WITH(NOLOCK)
		WHERE
			StepNumber =  5
			AND SourceDate = @sourcedate
		)
	BEGIN
		----------------------------------------------------------------------------------------------------------	
		SET @Log_StepDesc = 'Create #TempCLT207Data';
		SET @Log_StartDateTime = GETDATE();
	
		SELECT
			CAST(RankedDescriptions.productTransactionCode AS CHAR(4)) AS productTransactionCode, 
			CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.productDescription)),'')AS VARCHAR(75)) AS productDescription,
			CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.productCode)),'') AS CHAR(9)) AS productCode,
			CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.oldProductCode)),'') AS CHAR(9)) AS oldProductCode,
			CAST(RankedDescriptions.lineItemUnitCost AS DECIMAL(17,2)) AS lineItemUnitCost,
			CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.billableMatchCode)),'') AS CHAR(1)) AS billableMatchCode,
			CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.billableNonMatchCode)),'') AS CHAR(1)) AS billableNonMatchCode,
			CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.transactionTypeCode)),'') AS CHAR(1)) AS transactionTypeCode
			INTO #TempCLT207Data
		FROM
			(
				SELECT
					DescriptionCompareSet.productTransactionCode,
					DescriptionCompareSet.productDescription,
					MostRecentlyObservedCLTData.productCode,
					MostRecentlyObservedCLTData.oldProductCode,
					MostRecentlyObservedCLTData.lineItemUnitCost,
					MostRecentlyObservedCLTData.billableMatchCode,
					MostRecentlyObservedCLTData.billableNonMatchCode,
					MostRecentlyObservedCLTData.transactionTypeCode,
					DescriptionCompareSet.descriptionCompareSetVale,
					ROW_NUMBER() OVER(
						PARTITION BY
							DescriptionCompareSet.productTransactionCode
						ORDER BY
							CASE
								WHEN
									(DescriptionCompareSet.productDescription LIKE '%[0-9][0-9][0-9][0-9]%')
								THEN
									1
								ELSE
									0
							END,
							MostRecentlyObservedCLTData.Date_Insert DESC
					) AS productDescription_rowNumber
				FROM
				(
					SELECT
						INNERCLT00207.C_ISO_TRNS AS productTransactionCode,
						INNERCLT00207.T_ISO_TRNS AS productDescription,
						ROW_NUMBER() OVER(
							PARTITION BY
								INNERCLT00207.C_ISO_TRNS, INNERCLT00207.T_ISO_TRNS
							ORDER BY
								CASE
									WHEN
										(INNERCLT00207.T_ISO_TRNS LIKE '%[0-9][0-9][0-9][0-9]%')
									THEN
										1
									ELSE
										0
								END,
								INNERCLT00207.Date_Insert DESC
						) AS DescriptionCompareSetVale
					FROM
						[ClaimSearch_Prod].dbo.CLT00207 AS INNERCLT00207 WITH (NOLOCK)
					/*WHERE Do to an inconsistency between CLT207's currentstateprocess and the currentstateprocess of MPV,
							Information is lost if this filter is observed. 20181004
						CAST(CAST(INNERCLT00207.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam*/
				) AS DescriptionCompareSet
				CROSS APPLY
				(
					SELECT TOP (1)
						RecentINNERCLT00207.C_PS_PRD AS productCode,
						RecentINNERCLT00207.C_PS_PRD_OLD AS oldProductCode,
						RecentINNERCLT00207.A_ISO_TRNS_LIST AS lineItemUnitCost,
						RecentINNERCLT00207.C_TRAN_TYP AS transactionTypeCode,
						RecentINNERCLT00207.Date_Insert,
						RecentINNERCLT00207.F_BILL_MTCH AS billableMatchCode,
						RecentINNERCLT00207.F_BILL_NO_MTCH AS billableNonMatchCode
					FROM
						[ClaimSearch_Prod].dbo.CLT00207 AS RecentINNERCLT00207 WITH (NOLOCK)
					WHERE
						RecentINNERCLT00207.C_ISO_TRNS = DescriptionCompareSet.productTransactionCode
					ORDER BY
						RecentINNERCLT00207.Date_Insert DESC
				) MostRecentlyObservedCLTData
				WHERE
					DescriptionCompareSet.descriptionCompareSetVale = 1
			) AS RankedDescriptions
		WHERE
			RankedDescriptions.productDescription_rowNumber = 1;

		SELECT  @Log_RecordsAffected = @@ROWCOUNT;
		SELECT @Log_ActualProcessedDate = CONVERT(DATE,GETDATE());
		SET @Log_EndDateTime = GETDATE();
		SET @Log_TimeTaken = CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+CONVERT(VARCHAR(5),(DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%60));

		INSERT INTO ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log
		(
			ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus,
			RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc]
		)
		SELECT
			@ProductCode, @sourcedate, @Log_ActualProcessedDate, 1, @Log_StepDesc, 'Success',
			@Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL;

		-----------------------------------------------------------------------------	
		SET @Log_StepDesc = 'Create #TempMPVData';
		SET @Log_StartDateTime = GETDATE();

		SELECT
			CAST(RankedDescriptions.productCode AS CHAR(9)) AS productCode,
			CAST(RankedDescriptions.productTransactionCode AS CHAR(4)) AS productTransactionCode,
			CAST(LEFT(RankedDescriptions.productCode,4) AS CHAR(4)) AS productTransactionTypeCode,
			CAST(NULLIF(LTRIM(RTRIM(RankedDescriptions.productDescription)),'') AS VARCHAR(75)) AS productDescription,
			RankedDescriptions.lineItemUnitCost
			INTO #TempMPVData
		FROM
			(
				SELECT
					DescriptionCompareSet.productTransactionCode,
					MostRecentlyObservedMPVData.productCode,
					DescriptionCompareSet.productDescription,
					MostRecentlyObservedMPVData.dateLineItemExractRun,
					DescriptionCompareSet.descriptionCompareSetVale,
					MostRecentlyObservedMPVData.lineItemUnitCost,
					ROW_NUMBER() OVER(
						PARTITION BY
							DescriptionCompareSet.productTransactionCode
						ORDER BY
							CASE
								WHEN
									(DescriptionCompareSet.productDescription LIKE '%[0-9][0-9][0-9][0-9]%')
								THEN
									1
								ELSE
									0
							END,
							MostRecentlyObservedMPVData.dateLineItemExractRun DESC
					) AS productDescription_rowNumber
				FROM
					(
						SELECT
							RIGHT(MPV00202.I_PRD,4) AS productTransactionCode,
							MPV00202.T_PRD_DSC AS productDescription,
							ROW_NUMBER() OVER(
								PARTITION BY
									RIGHT(MPV00202.I_PRD,4), MPV00202.T_PRD_DSC
								ORDER BY
									CASE
										WHEN
											(MPV00202.T_PRD_DSC LIKE '%[0-9][0-9][0-9][0-9]%')
										THEN
											1
										ELSE
											0
									END,
									CAST(
										SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,1,10)
										+ ' '
										+ REPLACE((SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,12,8)),'.',':')
										+ (SUBSTRING(MPV00202.D_LN_ITM_EXRCT_RUN,20,8))
										AS DATETIME2(5)
									)
									DESC
							) AS descriptionCompareSetVale
						FROM
							[ClaimSearch_Prod].dbo.MPV00202 WITH (NOLOCK)
						/*WHERE Do to an inconsistency between CLT207's currentstateprocess and the currentstateprocess of MPV,
							Information is lost if this filter is observed. 20181004
						CAST(CAST(INNERCLT00207.Date_Insert AS CHAR(8)) AS DATE) >= @dateFilterParam*/
					) AS DescriptionCompareSet
					CROSS APPLY
					(
						SELECT TOP (1)
							RecentINNERMPV00202.I_PRD AS productCode,
							RecentINNERMPV00202.A_LN_ITM_UNIT AS lineItemUnitCost,
							CAST(
								SUBSTRING(RecentINNERMPV00202.D_LN_ITM_EXRCT_RUN,1,10)
								+ ' '
								+ REPLACE((SUBSTRING(RecentINNERMPV00202.D_LN_ITM_EXRCT_RUN,12,8)),'.',':')
								+ (SUBSTRING(RecentINNERMPV00202.D_LN_ITM_EXRCT_RUN,20,8))
								AS DATETIME2(5)
							)AS dateLineItemExractRun
						FROM
							[ClaimSearch_Prod].dbo.MPV00202 AS RecentINNERMPV00202 WITH (NOLOCK)
						WHERE
							RIGHT(RecentINNERMPV00202.I_PRD,4) = DescriptionCompareSet.productTransactionCode
						ORDER BY
							RecentINNERMPV00202.Date_Insert DESC
					) MostRecentlyObservedMPVData
				WHERE
					DescriptionCompareSet.descriptionCompareSetVale = 1
			) AS RankedDescriptions
		WHERE
			RankedDescriptions.productDescription_rowNumber = 1;

		SELECT  @Log_RecordsAffected = @@ROWCOUNT;
		SELECT @Log_ActualProcessedDate = CONVERT(DATE,GETDATE());
		SET @Log_EndDateTime = GETDATE();
		SET @Log_TimeTaken = CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+CONVERT(VARCHAR(5),(DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%60));

		INSERT INTO ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log
		(
			ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus,
			RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc]
		)
		SELECT
			@ProductCode, @sourcedate, @Log_ActualProcessedDate, 2, @Log_StepDesc, 'Success',
			@Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL;

		----------------------------------------------------------------------------------------------
		SET @Log_StepDesc = 'Create #ProductMergeSource';
		SET @Log_StartDateTime = GETDATE();
		DECLARE @additionalRowCount SMALLINT = 0;

		SELECT
			COALESCE(
				#TempCLT207Data.productTransactionCode,
				#TempMPVData.productTransactionCode
			) AS productTransactionCode,
			COALESCE(
					#TempCLT207Data.productDescription,
					#TempMPVData.productDescription
			) AS productTransactionDescription,
			ProductHierarchy.productTransactionCodeRemap AS productTransactionCodeRemap, /*can be NULL*/
			SugestedProductHeirarchy.suggestedProductHierarchyLvl1,
			SugestedProductHeirarchy.suggestedProductHierarchyLvl2,
			SugestedProductHeirarchy.suggestedProductHierarchyLvl3,
			@dateInserted AS dateInserted,
			'system-generated: ' + CAST(@dateInserted AS VARCHAR(25)) AS dataSource
			INTO #ProductHierarchyDataToUpdate
		FROM
			#TempMPVData
			FULL OUTER JOIN #TempCLT207Data
				ON #TempCLT207Data.productTransactionCode = #TempMPVData.productTransactionCode
			OUTER APPLY (
				SELECT
					CASE
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%People%'
						THEN
							1 /*People*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Assets%'
						THEN
							3 /*Assets*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Business%'
						THEN
							4 /*Business*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Healthcare%'
						THEN
							5 /*Healthcare*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Directory%Assistance%'
						THEN
							6 /*Directory Assistance*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Weather%Report%'
						THEN
							7 /*Weather Reports*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Criminal%Records%'
						THEN
							8 /*Criminal\Civil*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Police%Report%'
						THEN
							9 /*Police Reports*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Driver%History%'
						THEN
							10 /*Driver History*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Medical%Record%'
						THEN
							11 /*Medical Records*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%ClaimDirector%'
						THEN
							12 /*ClaimDirector*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Agency%'
						THEN
							15 /*Agency Fees*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%OFAC%'
						THEN
							16 /*OFAC*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Append%DS%'
						THEN
							17 /*Append-DS*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Vehicle%'
						THEN
							2 /*Vehicle*/
						WHEN
							COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								LIKE '%Fee%'
							AND COALESCE(#TempCLT207Data.productDescription,#TempMPVData.productDescription)
								NOT LIKE '%Property%'
						THEN
							14 /*Court Fees*/
						ELSE
							13 /*OTHER*/
					END AS suggestedProductGroupId
			) AS SugestedProductGroup
			OUTER APPLY (
				SELECT TOP 1
					InnerProduct.productHierarchyLvl1 AS suggestedProductHierarchyLvl1,
					InnerProduct.productHierarchyLvl2 AS suggestedProductHierarchyLvl2,
					InnerProduct.productHierarchyLvl3 AS suggestedProductHierarchyLvl3
				FROM
					DecisionNet.Product AS InnerProduct
				WHERE
					InnerProduct.productGroupId = SugestedProductGroup.suggestedProductGroupId
			) AS SugestedProductHeirarchy
			LEFT OUTER JOIN DecisionNet.ProductHierarchy
				ON COALESCE(#TempCLT207Data.productTransactionCode, #TempMPVData.productTransactionCode) = ProductHierarchy.productTransactionCode
		WHERE
			ISNULL(ProductHierarchy.dataSource,'system-generated: ') LIKE 'system-generated: %';

		SELECT @additionalRowCount = @@ROWCOUNT;
		
		UPDATE DecisionNet.ProductHierarchy
		SET
			ProductHierarchy.productTransactionDescription = SOURCE.productTransactionDescription,
			ProductHierarchy.productTransactionCodeRemap = SOURCE.productTransactionCodeRemap,
			ProductHierarchy.productHierarchyLvl1 = SOURCE.suggestedProductHierarchyLvl1,
			ProductHierarchy.productHierarchyLvl2 = SOURCE.suggestedProductHierarchyLvl2,
			ProductHierarchy.productHierarchyLvl3 = SOURCE.suggestedProductHierarchyLvl3,
			ProductHierarchy.dateInserted = SOURCE.dateInserted,
			ProductHierarchy.datasource = SOURCE.datasource
		FROM
			#ProductHierarchyDataToUpdate AS SOURCE
		WHERE
			ProductHierarchy.productTransactionCode = SOURCE.productTransactionCode
			AND
			(
				ISNULL(ProductHierarchy.productTransactionDescription,'~~~') <> ISNULL(SOURCE.productTransactionDescription,'~~~')
				OR ISNULL(ProductHierarchy.productTransactionCodeRemap,'~~~~') <> ISNULL(SOURCE.productTransactionCodeRemap,'~~~~')
				OR ProductHierarchy.productHierarchyLvl1 <> SOURCE.suggestedProductHierarchyLvl1
				OR ProductHierarchy.productHierarchyLvl2 <> SOURCE.suggestedProductHierarchyLvl2
				OR ProductHierarchy.productHierarchyLvl3 <> SOURCE.suggestedProductHierarchyLvl3
				/*OR ProductHierarchy.dateInserted <> SOURCE.dateInserted*//*Update on DateDiff would always be true*/
				/*OR ISNULL(ProductHierarchy.datasource,'~~~') <> ISNULL(SOURCE.datasource,'~~~')*//*Currently ONLY sys-generated sources are updated*/
			);
		
		SELECT @additionalRowCount = @additionalRowCount + @@ROWCOUNT;
		
		INSERT INTO DecisionNet.ProductHierarchy
		(
			productTransactionCode, productTransactionDescription, productTransactionCodeRemap, productHierarchyLvl1, productHierarchyLvl2, productHierarchyLvl3, dateInserted, dataSource
		)
		SELECT
			#ProductHierarchyDataToUpdate.productTransactionCode,
			#ProductHierarchyDataToUpdate.productTransactionDescription,
			#ProductHierarchyDataToUpdate.productTransactionCodeRemap, /*can be NULL*/
			#ProductHierarchyDataToUpdate.suggestedProductHierarchyLvl1,
			#ProductHierarchyDataToUpdate.suggestedProductHierarchyLvl2,
			#ProductHierarchyDataToUpdate.suggestedProductHierarchyLvl3,
			#ProductHierarchyDataToUpdate.dateInserted,
			#ProductHierarchyDataToUpdate.dataSource
		FROM
			#ProductHierarchyDataToUpdate
			LEFT OUTER JOIN DecisionNet.ProductHierarchy
				ON #ProductHierarchyDataToUpdate.productTransactionCode = ProductHierarchy.productTransactionCode
		WHERE
			ProductHierarchy.productTransactionCode IS NULL;
		
		SELECT @additionalRowCount = @additionalRowCount + @@ROWCOUNT;
			
		SELECT
			COALESCE(
				ProductHierarchy.productTransactionCode,
				#TempCLT207Data.productTransactionCode,
				#TempMPVData.productTransactionCode
			) AS productTransactionCode,
			COALESCE(
				NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchyLvl1)),''),
				NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchyLvl1)),'')
			) AS productHierarchyLvl1,
			COALESCE(
				NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchyLvl2)),''),
				NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchyLvl2)),'')
			) AS productHierarchyLvl2,
			COALESCE(
				NULLIF(LTRIM(RTRIM(ProductHierarchy.productHierarchyLvl3)),''),
				NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productHierarchyLvl3)),'')
			) AS productHierarchyLvl3,
			COALESCE(
					COALESCE(
						NULLIF(LTRIM(RTRIM(ProductHierarchy.productTransactionDescription)),''),
						NULLIF(LTRIM(RTRIM(HierarchyRemapLookup.productTransactionDescription)),'')
					),
					#TempCLT207Data.productDescription,
					#TempMPVData.productDescription
			) AS productTransactionDescription,
			#TempCLT207Data.billableMatchCode,
			#TempCLT207Data.billableNonMatchCode,
			COALESCE(#TempCLT207Data.productCode, #TempMPVData.productCode) AS recentlyObservedProductCode,
			#TempCLT207Data.oldProductCode AS recentlyObservedOldProductCode,
			/*The MPV tables are the source of truth on cost - DanR. 20180724
					To Consider: we could define additional cases for importing CLT207-lineItemUnitCost-Data,
					based on frequency of CLT207 update etc.*/
			COALESCE(#TempMPVData.lineItemUnitCost,#TempCLT207Data.lineItemUnitCost) AS recentlyObservedLineItemCost,
			/*For transactionTypeCode: Potentially Coalesce between CLT00207 and the productTransactionTypeCode of #TempMPVData*/
			#TempCLT207Data.transactionTypeCode,
			@dateInserted AS dateInserted
			INTO #ProductMergeSource
		FROM
			#TempMPVData
			FULL OUTER JOIN #TempCLT207Data
				ON #TempCLT207Data.productTransactionCode = #TempMPVData.productTransactionCode
			FULL OUTER JOIN DecisionNet.ProductHierarchy
				ON ProductHierarchy.productTransactionCode = #TempMPVData.productTransactionCode
					OR ProductHierarchy.productTransactionCode = #TempCLT207Data.productTransactionCode
			OUTER APPLY
			(
				SELECT
					RankedINNERProductHeirarchyRemap.productHierarchyLvl1,
					RankedINNERProductHeirarchyRemap.productHierarchyLvl2,
					RankedINNERProductHeirarchyRemap.productHierarchyLvl3,
					RankedINNERProductHeirarchyRemap.productTransactionDescription
				FROM
					(
						SELECT
							INNERProductHierarchyRemap.productHierarchyLvl1,
							INNERProductHierarchyRemap.productHierarchyLvl2,
							INNERProductHierarchyRemap.productHierarchyLvl3,
							INNERProductHierarchyRemap.productTransactionDescription,
							ROW_NUMBER() OVER(
								PARTITION BY INNERProductHierarchyRemap.productTransactionCodeRemap
									ORDER BY INNERProductHierarchyRemap.dateInserted DESC
							) AS remapByDate
						FROM
							DecisionNet.ProductHierarchy AS INNERProductHierarchyRemap
						WHERE
							INNERProductHierarchyRemap.productTransactionCodeRemap = COALESCE(#TempMPVData.productTransactionCode,#TempCLT207Data.productTransactionCode)
					) AS RankedINNERProductHeirarchyRemap
				WHERE
					RankedINNERProductHeirarchyRemap.remapByDate = 1
			) AS HierarchyRemapLookup;

		SELECT  @Log_RecordsAffected = @@ROWCOUNT + @additionalRowCount;
		SELECT @Log_ActualProcessedDate = CONVERT(DATE,GETDATE());
		SET @Log_EndDateTime = GETDATE();
		SET @Log_TimeTaken = CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+CONVERT(VARCHAR(5),(DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%60));

		INSERT INTO ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log
		(
			ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus,
			RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc]
		)
		SELECT
			@ProductCode, @sourcedate, @Log_ActualProcessedDate, 3, @Log_StepDesc, 'Success',
			@Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL;

		--------------------------------------------------------------------------------------
		SET @Log_StepDesc = 'Update DecisionNet.Product'
		SET @Log_StartDateTime = GETDATE()	
		
		UPDATE DecisionNet.Product
			SET
				Product.productGroupId = SOURCE.productGroupId,
				Product.productTransactionDescription = SOURCE.productTransactionDescription,
				Product.productHierarchyLvl1 = SOURCE.productHierarchyLvl1,
				Product.productHierarchyLvl2 = SOURCE.productHierarchyLvl2,
				Product.productHierarchyLvl3 = SOURCE.productHierarchyLvl3,
				Product.billableMatchCode = SOURCE.billableMatchCode,
				Product.billableNonMatchCode = SOURCE.billableNonMatchCode,
				Product.recentlyObservedProductCode = SOURCE.recentlyObservedProductCode,
				Product.recentlyObservedOldProductCode = SOURCE.recentlyObservedOldProductCode,
				Product.recentlyObservedLineItemCost = SOURCE.recentlyObservedLineItemCost,
				Product.transactionTypeCode = SOURCE.transactionTypeCode,
				Product.dateInserted = SOURCE.dateInserted
			FROM
				(
					SELECT
						#ProductMergeSource.productTransactionCode,
						CASE
							/*
								Makes you wish the ternary operator existed... added in SQL-SRVR 2012:
								IIF(condition, truthy, falsy)
							*/
							/*
								The following Match conditions are explicitly set based on existing business rules 20180806.
									Should the case be that ProductGroupId values are NOT being derived correctly, these
									comparisons would need to be updated.
									**20191017** New ProductHeirarchy automated-update mechanism added to this sproc should improve
									accuracy of ProductGroupId assignment.
							*/
							WHEN
								SUBSTRING(#ProductMergeSource.productHierarchyLvl2,1,6) = 'People'
							THEN
								1 /*People*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Vehicle Data'
								OR #ProductMergeSource.productHierarchyLvl2 = 'Vehicle Sightings'
							THEN
								2 /*Vehicle*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Assets'
							THEN
								3 /*Assets*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Business'
								OR #ProductMergeSource.productHierarchyLvl2 = 'Businesses'
							THEN
								4 /*Business*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Healthcare'
							THEN
								5 /*Healthcare*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Directory Assistance'
							THEN
								6 /*Directory Assistance*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Weather Report'
								OR #ProductMergeSource.productHierarchyLvl2 = 'Weather Reports'
							THEN
								7 /*Weather Reports*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Criminal Records'
							THEN
								8 /*Criminal\Civil*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Police Reports'
							THEN
								9 /*Police Reports*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Driver History'
							THEN
								10 /*Driver History*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Medical Records'
							THEN
								11 /*Medical Records*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'ClaimDirector'
							THEN
								12 /*ClaimDirector*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Court Fees'
							THEN
								14 /*Court Fees*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Agency Fees'
							THEN
								15 /*Agency Fees*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'OFAC'
							THEN
								16 /*OFAC*/
							WHEN
								#ProductMergeSource.productHierarchyLvl2 = 'Append-DS'
							THEN
								17 /*Append-DS*/
							ELSE
								13 /*OTHER*/
						END AS productGroupId,
						#ProductMergeSource.productTransactionDescription,
						#ProductMergeSource.productHierarchyLvl1,
						#ProductMergeSource.productHierarchyLvl2,
						#ProductMergeSource.productHierarchyLvl3,
						#ProductMergeSource.billableMatchCode,
						#ProductMergeSource.billableNonMatchCode,
						#ProductMergeSource.recentlyObservedProductCode,
						#ProductMergeSource.recentlyObservedOldProductCode,
						#ProductMergeSource.recentlyObservedLineItemCost,
						#ProductMergeSource.transactionTypeCode,
						#ProductMergeSource.dateInserted
					FROM
						#ProductMergeSource
				) AS SOURCE
			WHERE
				Product.productTransactionCode = SOURCE.productTransactionCode
				AND
				(
					Product.productGroupId <> SOURCE.productGroupId
					/*Values for Hierarchy_Lvl, description, and matchCode are guaranteed <> ''; so sentinal value can be used*/
					OR ISNULL(Product.productTransactionDescription,'') <> ISNULL(SOURCE.productTransactionDescription,'')
					OR ISNULL(Product.productHierarchyLvl1,'') <> ISNULL(SOURCE.productHierarchyLvl1,'')
					OR ISNULL(Product.productHierarchyLvl2,'') <> ISNULL(SOURCE.productHierarchyLvl2,'')
					OR ISNULL(Product.productHierarchyLvl3,'') <> ISNULL(SOURCE.productHierarchyLvl3,'')
					OR ISNULL(Product.billableMatchCode,'') <> ISNULL(SOURCE.billableMatchCode,'')
					OR ISNULL(Product.billableNonMatchCode,'') <> ISNULL(SOURCE.billableNonMatchCode,'')
					OR ISNULL(Product.transactionTypeCode,'') <> ISNULL(SOURCE.transactionTypeCode,'')
					OR ISNULL(Product.recentlyObservedProductCode,'') <> ISNULL(SOURCE.recentlyObservedProductCode,'')
					/* recentlyObservedProductCode or recentlyObservedOldProductCode not necessarily consistent and may result in
						unwanted frequency of updates if considered.
						OR ISNULL(Product.recentlyObservedOldProductCode,'') <> ISNULL(SOURCE.recentlyObservedOldProductCode,'')
					*/
					/*No obvious sentinal value available*/
					OR(
						Product.recentlyObservedLineItemCost IS NULL AND SOURCE.recentlyObservedLineItemCost IS NOT NULL
						OR Product.recentlyObservedLineItemCost IS NOT NULL AND SOURCE.recentlyObservedLineItemCost IS NULL
						OR Product.recentlyObservedLineItemCost <> SOURCE.recentlyObservedLineItemCost
					)
					/*Dont update if the only difference is the dateInserted
						OR TARGET.dateInserted <> SOURCE.dateInserted
					*/
				);

		SELECT  @Log_RecordsAffected = @@ROWCOUNT;
		SELECT @Log_ActualProcessedDate = CONVERT(DATE,GETDATE());
		SET @Log_EndDateTime = GETDATE();
		SET @Log_TimeTaken = CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+CONVERT(VARCHAR(5),(DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%60));

		INSERT INTO ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log
		(
			ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus,
			RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc]
		)
		SELECT
			@ProductCode, @sourcedate, @Log_ActualProcessedDate, 4, @Log_StepDesc, 'Success',
			@Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL;

		------------------------------------------------------------------------------------------------------------------------------
		SET @Log_StepDesc = 'insert into DecisionNet.Product';
		SET @Log_StartDateTime = GETDATE();

		/*Explicit LEFT JOIN style for INSERT non-existent records, since set comparison not applicable*/
		INSERT INTO DecisionNet.Product
		(
			productTransactionCode,
			productGroupId,
			productTransactionDescription,
			productHierarchyLvl1,
			productHierarchyLvl2,
			productHierarchyLvl3,
			billableMatchCode,
			billableNonMatchCode,
			recentlyObservedProductCode,
			recentlyObservedOldProductCode,
			recentlyObservedLineItemCost,
			transactionTypeCode,
			dateInserted
		)
		SELECT
			SOURCE.productTransactionCode,
			SOURCE.productGroupId,
			SOURCE.productTransactionDescription,
			SOURCE.productHierarchyLvl1,
			SOURCE.productHierarchyLvl2,
			SOURCE.productHierarchyLvl3,
			SOURCE.billableMatchCode,
			SOURCE.billableNonMatchCode,
			SOURCE.recentlyObservedProductCode,
			SOURCE.recentlyObservedOldProductCode,
			SOURCE.recentlyObservedLineItemCost,
			SOURCE.transactionTypeCode,
			SOURCE.dateInserted
		FROM
			(
				SELECT
					#ProductMergeSource.productTransactionCode,
					CASE
						/*
							Makes you wish the ternary operator existed... added in SQL-SRVR 2012:
							IIF(condition, truthy, falsy)
						*/
						/*
							The following Match conditions are explicitly set based on existing business rules 20180806.
								Should the case be that ProductGroupId values are NOT being derived correctly, these
								comparisons would need to be updated.
								**20191017** New ProductHeirarchy automated-update mechanism added to this sproc should improve
									accuracy of ProductGroupId assignment.
						*/
						WHEN
							SUBSTRING(#ProductMergeSource.productHierarchyLvl2,1,6) = 'People'
						THEN
							1 /*People*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Vehicle Data'
							OR #ProductMergeSource.productHierarchyLvl2 = 'Vehicle Sightings'
						THEN
							2 /*Vehicle*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Assets'
						THEN
							3 /*Assets*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Business'
							OR #ProductMergeSource.productHierarchyLvl2 = 'Businesses'
						THEN
							4 /*Business*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Healthcare'
						THEN
							5 /*Healthcare*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Directory Assistance'
						THEN
							6 /*Directory Assistance*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Weather Report'
							OR #ProductMergeSource.productHierarchyLvl2 = 'Weather Reports'
						THEN
							7 /*Weather Reports*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Criminal Records'
						THEN
							8 /*Criminal\Civil*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Police Reports'
						THEN
							9 /*Police Reports*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Driver History'
						THEN
							10 /*Driver History*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Medical Records'
						THEN
							11 /*Medical Records*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'ClaimDirector'
						THEN
							12 /*ClaimDirector*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Court Fees'
						THEN
							14 /*Court Fees*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Agency Fees'
						THEN
							15 /*Agency Fees*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'OFAC'
						THEN
							16 /*OFAC*/
						WHEN
							#ProductMergeSource.productHierarchyLvl2 = 'Append-DS'
						THEN
							17 /*Append-DS*/
						ELSE
							13 /*OTHER*/
					END AS productGroupId,
					#ProductMergeSource.productTransactionDescription,
					#ProductMergeSource.productHierarchyLvl1,
					#ProductMergeSource.productHierarchyLvl2,
					#ProductMergeSource.productHierarchyLvl3,
					#ProductMergeSource.billableMatchCode,
					#ProductMergeSource.billableNonMatchCode,
					#ProductMergeSource.recentlyObservedProductCode,
					#ProductMergeSource.recentlyObservedOldProductCode,
					#ProductMergeSource.recentlyObservedLineItemCost,
					#ProductMergeSource.transactionTypeCode,
					#ProductMergeSource.dateInserted
				FROM
					#ProductMergeSource
			) AS SOURCE
			LEFT OUTER JOIN DecisionNet.Product
				ON Product.productTransactionCode = SOURCE.productTransactionCode
		WHERE
			Product.productTransactionCode IS NULL;

		SELECT  @Log_RecordsAffected = @@ROWCOUNT;
		SELECT @Log_ActualProcessedDate = CONVERT(DATE,GETDATE());
		SET @Log_EndDateTime = GETDATE();
		SET @Log_TimeTaken = CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)/3600)+':'+CONVERT(VARCHAR(5),DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%3600/60)+':'+CONVERT(VARCHAR(5),(DATEDIFF(s, @Log_StartDateTime, @Log_EndDateTime)%60));

		INSERT INTO ClaimSearch_Prod.DecisionNet.CS_DecisionNet_Dashboard_Process_Log
		(
			ProductCode, SourceDate, ActualProcessedDate, StepNumber, StepDesc, StepStatus,
			RecordsAffected, StartDateTime, EndDateTime, [TimeTaken(HH:MI:SS)], [ErrorDesc]
		)
		SELECT
			@ProductCode, @sourcedate, @Log_ActualProcessedDate, 5, @Log_StepDesc, 'Success',
			@Log_RecordsAffected, @Log_StartDateTime, @Log_EndDateTime, @Log_TimeTaken, NULL;
	END;
END;
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO

--PRINT 'ROLLBACK ';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;
/*
******************************************	Env: JDESQLPRD3.ClaimSearch_Dev		**	User: VRSKJDEPRD\i24325				**	Time: Oct 17 2019  1:56PM			******************************************

(7320 row(s) affected)
COMMIT

*/
