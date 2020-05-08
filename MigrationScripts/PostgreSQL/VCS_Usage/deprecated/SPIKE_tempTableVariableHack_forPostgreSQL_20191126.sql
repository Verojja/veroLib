--BEGIN TRANSACTION;

CREATE TEMPORARY TABLE "ActivityLogVariables" (
	"productCode" VARCHAR(50), /*This value remains consistent for all steps, so it can be set now*/
	"sourceDateTime" TIMESTAMP, /*This value remains consistent for all steps, but it's value is set in the next section*/
	"executionDateTime" TIMESTAMP,
	"stepId" SMALLINT,
	"stepDescription" VARCHAR(1000),
	"stepStartDateTime" TIMESTAMP,
	"stepEndDateTime" TIMESTAMP,
	"recordsAffected" BIGINT,
	"isSuccessful" BIT,
	"stepExecutionNotes" VARCHAR(1000)
);

CREATE TEMPORARY TABLE "Test" (
	"productCode" VARCHAR(50), /*This value remains consistent for all steps, so it can be set now*/
	"sourceDateTime" TIMESTAMP, /*This value remains consistent for all steps, but it's value is set in the next section*/
	"executionDateTime" TIMESTAMP,
	"stepId" SMALLINT,
	"stepDescription" VARCHAR(1000),
	"stepStartDateTime" TIMESTAMP,
	"stepEndDateTime" TIMESTAMP,
	"recordsAffected" BIGINT,
	"isSuccessful" BIT,
	"stepExecutionNotes" VARCHAR(1000)
);

INSERT INTO "ActivityLogVariables"
(
	"productCode",
	"sourceDateTime",
	"executionDateTime",
	"stepId",
	"stepDescription",
	"stepStartDateTime",
	"stepEndDateTime",
	"recordsAffected",
	"isSuccessful",
	"stepExecutionNotes"
)
SELECT
	'PG_VCSUsage',
	COALESCE("LastActivityLogExecution"."executionDateTime",NOW()),
	NOW(),
	1,
	'CaptureIPUserAuditDataToImport',
	NOW(),
	NULL,
	NULL,
	NULL,
	NULL
FROM
	(
		VALUES
			(1)
	) AS "SingleRowHack" ("unUsedValue") 
	LEFT OUTER JOIN
	(
		SELECT
			"IPAuditActivityLog"."executionDateTime"
		FROM
			public."IPAuditActivityLog"
		ORDER BY
	 		"IPAuditActivityLog"."iPAuditActivityLogId" DESC
		LIMIT 1
	) AS "LastActivityLogExecution"
		ON 1=1;
/*
	some code here			 
*/	 	
--SELECT * FROM "ActivityLogVariables";

UPDATE "ActivityLogVariables"
SET
	"stepEndDateTime" = NOW(),
	"recordsAffected" = "AggregateInAnUpdateTest"."someValue",
	"isSuccessful" = CAST(1 AS BIT)
FROM
	(
		SELECT
			COUNT(*) AS "someValue"
		FROM
			"Test"
	) "AggregateInAnUpdateTest"
WHERE
	"ActivityLogVariables"."stepId" = 1;

			 
INSERT INTO "ActivityLogVariables"
(
	"productCode",
	"sourceDateTime",
	"executionDateTime",
	"stepId",
	"stepDescription",
	"stepStartDateTime",
	"stepEndDateTime",
	"recordsAffected",
	"isSuccessful",
	"stepExecutionNotes"
)
SELECT
	'PG_VCSUsage',
	COALESCE("LastActivityLogExecution"."executionDateTime",NOW()),
	NOW(),
	2,
	'UpdateExistingIPUserAuditRecords',
	NOW(),
	NULL,
	NULL,
	NULL,
	NULL
FROM
	(
		VALUES
			(1)
	) AS "SingleRowHack" ("unUsedValue") 
	LEFT OUTER JOIN
	(
		SELECT
			"IPAuditActivityLog"."executionDateTime"
		FROM
			public."IPAuditActivityLog"
		ORDER BY
	 		"IPAuditActivityLog"."iPAuditActivityLogId" DESC
		LIMIT 1
	) AS "LastActivityLogExecution"
		ON 1=1;
/*
	some code here			 
*/		 
/*
	some code here			 
*/	 	
--SELECT * FROM "ActivityLogVariables";

UPDATE "ActivityLogVariables"
SET
	"stepEndDateTime" = NOW(),
	"recordsAffected" = "AggregateInAnUpdateTest"."someValue",
	"isSuccessful" = CAST(1 AS BIT)
FROM
	(
		SELECT
			COUNT(*) AS "someValue"
		FROM
			"Test"
	) "AggregateInAnUpdateTest"
WHERE
	"ActivityLogVariables"."stepId" = 2;

			 
SELECT * FROM "ActivityLogVariables";
	 
/*

DROP TABLE "ActivityLogVariables";
DROP TABLE "Test";

*/