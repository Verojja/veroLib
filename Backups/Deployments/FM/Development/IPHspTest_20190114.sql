

SET NOEXEC OFF;
USE ClaimSearch_Dev


	DECLARE @paramone DATETIME2(2) = CAST('2008-08-01' AS DATETIME2(0));
	DECLARE @paramTwo BIT = 1;

	EXEC dbo.hsp_UpdateInsertInvolvedParty
	--@paramone,@paramTwo;r


	SELECT * FROM dbo.InvolvedPartyActivityLog

	SELECT COUNT(*) FROM dbo.V_ActiveFMNonAliasedInvolvedParty
	SELECT COUNT(*) FROM dbo.V_ActiveFMAliasedInvolvedParty
	SELECT COUNT(*) FROM dbo.V_ActiveFMNonAliasedServiceProvider
	SELECT COUNT(*) FROM dbo.V_ActiveFMAliasedServiceProvider
	
	SELECT COUNT(*) FROM dbo.InvolvedParty


(1 row(s) affected)
Msg 4104, Level 16, State 1, Procedure hsp_UpdateInsertInvolvedParty, Line 309
The multi-part identifier "#FMNonAliasedInvolvedPartyData.involvedPartyId" could not be bound.
Msg 266, Level 16, State 2, Procedure hsp_UpdateInsertInvolvedParty, Line 309
Transaction count after EXECUTE indicates a mismatching number of BEGIN and COMMIT statements. Previous count = 0, current count = 1.

(2 row(s) affected)

(1 row(s) affected)