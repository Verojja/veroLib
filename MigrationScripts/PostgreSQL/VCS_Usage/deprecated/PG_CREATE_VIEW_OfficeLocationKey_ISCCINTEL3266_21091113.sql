/***********************************************
WorkItem: ISCCINTEL-3266
Date: 2018-11-13
Author: Julia Lawrence
Description: Object for exposing uniform office-location-format,
				designed by Buisiness (Zack Miller).
				Also exposes INCOMP regoff mapping.

			 Performance: Current plan is to deploy VIEW to ClaimSearch_Prod
				despite source-object existing in *another-DB-Environment.
				*Even if indexes are built on source-object, this view will
				not be able to take advantage of them.
************************************************/
CREATE VIEW public."V_OfficeLocationKey" AS
SELECT
	insotab.inscomp AS "companyCode",
	insotab."regoff" AS "officeCode",
	CAST(
		COALESCE(
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab."regoff")),'') != '' THEN CAST(insotab."regoff" AS CHAR(5))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab."offname")),'') != '' THEN ' - ' || CAST(insotab."offname" AS VARCHAR(30))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.mcity)),'') != '' THEN ' - ' || CAST(insotab.mcity AS VARCHAR(30))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.mst)),'') != '' THEN ', ' || CAST(insotab.mst AS VARCHAR(2))
				ELSE ''
			END
			||
			CASE
				WHEN COALESCE(LTRIM(RTRIM(insotab.mzip)),'') != '' THEN ' ' || CAST(insotab.mzip AS VARCHAR(9))
				ELSE ''
			END,
			''
		) AS VARCHAR(85)
	) AS "officeLocationForDisplay"
FROM
	natb.insotab;