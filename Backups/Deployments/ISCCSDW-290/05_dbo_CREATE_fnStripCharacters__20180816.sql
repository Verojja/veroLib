SET NOEXEC OFF;
/*
	1 Time Script.
	This script will instantiate a new function; it is NOT designed
		to be made into a job/sproc or automated in any way.
		It will only be executed a single time to CREATE a new function.
	
	Execution of this script relies on zero data on tables. IE: there is NO required data refresh
	for existing production data.
	
	Note: At the time of script-submission, GRANT / DENY permission(s) statements were NOT included.
*/


BEGIN TRANSACTION
/*Remeber to switch to explicit COMMIT TRANSACTION (line 63) for the production deploy.
Message log output should be similar to the following:

	COMMIT TRANSACTION
*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: Unknown
Date: Unknown
Author: Dan Ravaglia
Description: Creation of function sanitizing string fields.
*************************//**********************
WorkItem: ISCCSDW-290
Date: 2018-08-16
Author: Robert David Warner
Description: Pushing the function to ProdEnvironment and adding comment header.
************************************************/
CREATE FUNCTION dbo.fn_StripCharacters
(
    @String NVARCHAR(MAX), 
    @MatchExpression VARCHAR(255)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    SET @MatchExpression =  '%['+@MatchExpression+']%'

    WHILE PatIndex(@MatchExpression, @String) > 0
        SET @String = Stuff(@String, PatIndex(@MatchExpression, @String), 1, '')

    RETURN @String

END
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
PRINT'ROLLBACK TRANSACTION';ROLLBACK TRANSACTION;
--PRINT'COMMIT TRANSACTION';COMMIT TRANSACTION;
/*

*/