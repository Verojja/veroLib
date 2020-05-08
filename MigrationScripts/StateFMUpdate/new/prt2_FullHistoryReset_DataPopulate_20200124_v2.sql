--USE CLAIMSEARCH_Dev;
USE CLAIMSEARCH_PROD;

BEGIN TRANSACTION;
/****************************************************************************************/
	/* VARIABLE Declaration; PLEASE CHANGE THIS VARIABLE TO THE DESIRED RESET DATE */
--DECLARE '20150101' DATE = '20150101';
--/****************************************************************************************/
--/*** Everything else is automated. Just double check that the script is set to COMMIT ***/
--/****************************************************************************************/
--DECLARE '20150101' DATE,
--		'20150105' Date; 
--SELECT 
--	'20150101' = CASE
--		WHEN
--			MONTH('20150101') < 10
--		THEN
--			CAST(CAST(YEAR('20150101') AS CHAR(4)) + '0' + CAST(MONTH('20150101') AS VARCHAR(2))+'01' AS DATE)
--		ELSE
--			CAST(CAST(YEAR('20150101') AS CHAR(4)) + CAST(MONTH('20150101') AS VARCHAR(2))+'01' AS DATE)
--	END,
--	'20150105' = CASE
--		WHEN
--			MONTH('20150101') < 10
--		THEN
--			CAST(CAST(YEAR('20150101') AS CHAR(4)) + '0' + CAST(MONTH('20150101') AS VARCHAR(2))+'05' AS DATE)
--		ELSE
--			CAST(CAST(YEAR('20150101') AS CHAR(4)) + CAST(MONTH('20150101') AS VARCHAR(2))+'05' AS DATE)
--	END;
--SELECT '20150101', '20150101', '20150105' Date;
/****************************************************************************************/
/****************************************************************************************/
EXEC dbo.hsp_UpdateInsertFireMarshalDriver
@dateFilterParam = '20150101', /*limited to last5 years*/
@dailyLoadOverride = 1

GO

	EXEC dbo.hsp_UpdateInsertAddress
	@dateFilterParam = '20150101',
	@dailyLoadOverride = 1

GO

	EXEC dbo.hsp_UpdateInsertAdjuster
	@dateFilterParam = '20150101',
	@dailyLoadOverride = 1

GO

	EXEC dbo.hsp_UpdateInsertPolicy
	@dateFilterParam = '20150101',
	@dailyLoadOverride = 1

GO

		EXEC dbo.hsp_UpdateInsertClaim
		@dateFilterParam = '20150101',
		@dailyLoadOverride = 1

		GO

		EXEC dbo.hsp_UpdateInsertInvolvedParty
		@dateFilterParam = '20150101',
		@dailyLoadOverride = 1

		GO

			EXEC dbo.hsp_UpdateInsertElementalClaim
			@dateFilterParam = '20150101',
			@dailyLoadOverride = 1

GO

				EXEC dbo.hsp_FireMarshalSendClaims
				@executionDateParam = '20150101' /*Note: This variable has a different name*/
				@mustMatchDB2FMProcess = 0

GO

				EXEC dbo.hsp_UpdateInsertFMPendingClaim
				@dateFilterParam = '20150101',
				@dailyLoadOverride = 1

GO

/*This will possibly result in a small spike in pending claims for the month of reset,
followed by a higher than average historicsend\generate for the month following the reset.*/
					EXEC dbo.hsp_FireMarshalSendClaims
					@executionDateParam = '20150105' /*Note: This variable has a different name*/
					@mustMatchDB2FMProcess = 1	/*This will persist as many values as possible for historic.*/

/*controller will also be unaffected since the @executionDateParam is less than the current projected gen date
	The I_ALLCLM numbers from existing system will be brought in, and existing observes through
		20171130	20181001	20190601
		20171201	20181004	20190701
		20180101	20181016	20190801
		20180401	20181101	20190901
		20180404	20181201	20191001
		20180525	20190103	20191101
		20180601	20190201	20191201
		20180705	20190301	20200101 --<----CurrentYear
		20180801	20190401
		20180901	20190501
*/

GO

				EXEC dbo.hsp_UpdateInsertFMHistoricClaim /*handle's "pending" claims*/
				@dateFilterParam = '20150101',
				@dailyLoadOverride = 1

GO

			EXEC dbo.hsp_UpdateInsertFireMarshalExtract
			@dateFilterParam = '20150101',
			@dailyLoadOverride = 1

GO

--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;

/*

*/