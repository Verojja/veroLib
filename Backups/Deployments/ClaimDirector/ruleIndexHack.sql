BEGIN TRANSACTION

UPDATE CDP.ReasonsExport
SET
	ReasonsExport.[Index] = StupidHackAlias.uniqueInstanceValue
FROM
	(
		SELECT
			InnerReasonsExport.[ISO File Number],
			InnerReasonsExport.RC_Description,
			ROW_NUMBER() OVER(
				PARTITION BY
					InnerReasonsExport.[ISO File Number]
				ORDER BY
					InnerReasonsExport.dateInserted
			) AS uniqueInstanceValue
		FROM
			CDP.ReasonsExport AS InnerReasonsExport
--		WHERE
--			InnerReasonsExport.[ISO File Number] IN
--			(
--'3A004893470',
--'1B004895863',
--'7M004508016',
--'0B004810720',
--'0E004595541',
--'7L004706324',
--'7B004744645',
--'5K004884675',
--'3B004710615',
--'6S004871553'
--			)
	) AS StupidHackAlias
WHERE
	ReasonsExport.[ISO File Number] = StupidHackAlias.[ISO File Number]
	AND ReasonsExport.RC_Description = StupidHackAlias.RC_Description
	
--SELECT * FROM CDP.ReasonsExport WHERE [Index] IS NOT NULL
--ORDER BY [ISO File Number],[Index]
COMMIT TRANSACTION


SELECT * FROM CDP.InvolvedPartyExtract