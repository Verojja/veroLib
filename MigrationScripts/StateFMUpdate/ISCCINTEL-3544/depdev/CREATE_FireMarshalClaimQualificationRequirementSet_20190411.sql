SET NOEXEC OFF;

USE ClaimSearch_Dev
--USE ClaimSearch_Prod

BEGIN TRANSACTION

GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/***********************************************
WorkItem: ISCCINTEL-2316
Date: 2018-04-11
Author: Daniel Ravaglia and Robert David Warner
Description: FM claim qualification requirement set(s).
				Each row either identifies an elemental condition which would
				qualify a particular fire claim as "bad" (IE: not to be sent
					until condition is no longer true)
				or permutations of each elemental condition combined.
						  
			Performance: No current notes.
************************************************/
CREATE TABLE dbo.FireMarshalClaimQualificationRequirementSet
(
	ruleId SMALLINT NOT NULL,
	ruleSetKey VARCHAR(200) NOT NULL,
	requirementSetDescription VARCHAR(900) NULL,
	violationDisplayMessage VARCHAR(255) NULL,
	/*only one NULL row should exist,
			for fmQualificationRequirmentSetId = 0
	*/
	CONSTRAINT PK_FireMarshalClaimQualReqSet_ruleId
		PRIMARY KEY CLUSTERED (ruleId),
	CONSTRAINT U_FireMarshalClaimQualReqSet_ruleSetKey
		UNIQUE (ruleSetKey)
);
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
/*
	In general, BAD FM Claims will not be included in the regular "FMHistoric DataSet to Insert", and thus will not be removed from the
		Pending object (outside of being flagged as Non-Actve.
	Some states may require additional fileExports for Bad FM Claims.
*/
INSERT INTO dbo.FireMarshalClaimQualificationRequirementSet
(
	ruleId,
	ruleSetKey,
	requirementSetDescription,
	violationDisplayMessage
)
VALUES
	(0, '(0)', 'All claims are considered GOOD; IE: As no claims are BAD,
		 no claims are inherently removed from FM-Send or displayed differently on the dashboard', NULL),
	
	(1, '(1)', 'Claims with Estimated Amount Missing are considered BAD', 'Estimated amount missing'),
	(2, '(2)', 'Claims with Settlement Amount Missing are considered BAD', 'Settlement amount missing'),
	(3, '(3)', 'Claims with Non-descript Loss Description field(s) are considered BAD', 'Loss description invalid'),

	(4, '(4)', 'CLOSED Claims are removed from FM-Send (as though BAD), dashboard functionality unaffected', NULL), /*Business decided that this rule will not affect verbage/error-phrasing*/
	(5, '(5)', 'For any rule(s) (other than rule4) grouped with THIS rule, the behavior of the associated rule(s) is not observed for CLOSED Claims', NULL), /*Business decided that this rule will not affect verbage/error-phrasing*/
	
	(6, '(1, 4)', '(!Est$ &NoSendClosedClaims)', 'Estimated amount missing'),
	(7, '(1, 5)', '(!Est$ &ClaimOpen)', 'Estimated amount missing'),
	(8, '(1, 4, 5)', '(!Est$ &ClaimOpen &NoSendClosedClaims)', 'Estimated amount missing'),

	(9, '(2, 4)', '(!Stl$ &NoSendClosedClaims)', 'Settlement amount missing'),
/*GA*/	(10, '(2, 5)', '(!Stl$ &ClaimOpen)', 'Settlement amount missing'),
	(11, '(2, 4, 5)', '(!Stl$ &ClaimOpen &NoSendClosedClaims)', 'Settlement amount missing'),
	
	(12, '(1, 2)', '(!Est$ | !Stl$)', 'Estimated and/or Settlement amount missing'),
	(13, '(1, 2, 4)', '((!Est$ | !Stl$) &NoSendClosedClaims)', 'Estimated and/or Settlement amount missing'),
	(14, '(1, 2, 5)', '((!Est$ | !Stl$) &ClaimOpen)', 'Estimated and/or Settlement amount missing'),
	(15, '(1, 2, 4, 5)', '((!Est$ | !Stl$) &ClaimOpen &NoSendClosedClaims', 'Estimated and/or Settlement amount missing'),

	(16, '((1, 5), 2)', '((!Est$ &ClaimOpen) | !Stl$)', 'Estimated and/or Settlement amount missing'),
	(17, '((1, 5), 2, 4)', '((!Est$ &ClaimOpen) | !Stl$ &NoSendClosedClaims)', 'Estimated and/or Settlement amount missing'),
	(18, '(1, (2, 5))', '((!Stl$ &ClaimOpen) | !Est$)', 'Estimated and/or Settlement amount missing'),
	(19, '(1, (2, 5), 4)', '((!Stl$ &ClaimOpen) | !Est$ &NoSendClosedClaims)', 'Estimated and/or Settlement amount missing'),


	(20, '(1, 2, 3)', '(!Est$ | !Stl$ | !LossDesc)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
/*KS*/		(21, '(1&2,3, 4)', '(!Est$ | !Stl$ | !LossDesc &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(22, '((1, 5), 2, 3)', '((!Est$ &ClaimOpen) | !Stl$ | !LossDesc)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(23, '((1, 5), 2, 3, 4)', '((!Est$ &ClaimOpen) | !Stl$ | !LossDesc &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(24, '(1, (2, 5), 3)', '(!Est$ | (!Stl$ &ClaimOpen) | !LossDesc)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(25, '(1, (2, 5), 3, 4)', '(!Est$ | (!Stl$ &ClaimOpen) | !LossDesc &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(26, '(1, 2, (3, 5))', '(!Est$ | !Stl$ | (!LossDesc &ClaimOpen))', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(27, '(1, 2, (3, 5), 4)', '(!Est$ | !Stl$ | (!LossDesc &ClaimOpen) &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(28, '((1, 2, 5) 3)', '((!Est$ | !Stl$ &ClaimOpen) | !LossDesc)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(29, '((1, 2, 5) 3, 4)', '((!Est$ | !Stl$ &ClaimOpen) | !LossDesc &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(30, '((1, 3, 5), 2', '((!Est$ | !LossDesc &ClaimOpen) | !Stl$)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(31, '((1, 3, 5), 2, 4)', '((!Est$ | !LossDesc &ClaimOpen) | !Stl$ &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(32, '(1, (2, 3, 5)', '(!Est$ | (!Stl$ | !LossDesc &ClaimOpen))', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(33, '(1, (2, 3, 5), 4', '(!Est$ | (!Stl$ | !LossDesc &ClaimOpen) &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(34, '(1, 2, 3, 5)', '(!Est$ | !Stl$ | !LossDesc &ClaimOpen)', 'Loss description invalid and Estimated and/or Settlement amount missing'),
	(35, '(1, 2, 3, 4, 5)', '(!Est$ | !Stl$ | !LossDesc &ClaimOpen &NoSendClosedClaims)', 'Loss description invalid and Estimated and/or Settlement amount missing');
	


/*GA*/
/*KS*/
GO
IF(@@TRANCOUNT <1 OR @@ERROR <> 0)
BEGIN
	ROLLBACK TRANSACTION;
	SET NOEXEC ON;
END
GO
--SELECT * FROM dbo.FireMarshalClaimQualificationRequirementSet
--PRINT 'ROLLBACK';ROLLBACK TRANSACTION;
PRINT 'COMMIT';COMMIT TRANSACTION;
/*

(36 row(s) affected)
COMMIT

*/