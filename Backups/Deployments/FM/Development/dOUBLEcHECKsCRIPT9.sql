USE ClaimSearch_Prod
--SELECT COUNT(*) AS countFireMarshalDriver FROM dbo.FireMarshalDriver
--SELECT COUNT(*) AS countPolicy FROM dbo.Policy
--SELECT COUNT(*) AS countAddress FROM dbo.Address
--SELECT COUNT(*) AS countAdjuster FROM dbo.Adjuster
--SELECT COUNT(*) AS countClaim FROM dbo.Claim
--SELECT COUNT(*) AS countInvolvedParty FROM dbo.InvolvedParty
--SELECT COUNT(*) AS countInvolvedPartyAddressMap FROM dbo.InvolvedPartyAddressMap
--SELECT COUNT(*) AS countElementalClaim FROM dbo.ElementalClaim
--SELECT COUNT(*) AS countFireMarshalPendingClaim FROM dbo.FireMarshalPendingClaim
--SELECT COUNT(*) AS countFireMarshalClaimSendHistory FROM dbo.FireMarshalClaimSendHistory

SELECT COUNT(*) AS countFireMarshalPendingClaim FROM dbo.FireMarshalPendingClaim
SELECT COUNT(*) AS countFireMarshalClaimSendHistory FROM dbo.FireMarshalClaimSendHistory
SELECT COUNT(*) AS countFireMarshalExtract FROM dbo.FireMarshalExtract


SELECT 
'Passive' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalClaimSendHistory
WHERE FireMarshalClaimSendHistory.reportingStatus = 'Passive'
UNION ALL
SELECT 
'Sent' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalClaimSendHistory
WHERE FireMarshalClaimSendHistory.reportingStatus = 'Sent'

SELECT 
'Pending' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.reportingStatus = 'Pending'
UNION ALL
SELECT 
'Exception' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalPendingClaim
WHERE FireMarshalPendingClaim.reportingStatus = 'Exception'


SELECT 
'Pending' as countName,
COUNT(*) AS countValue FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Pending'
UNION ALL
SELECT 
'Passive' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Passive'
UNION ALL
SELECT 
'Exception' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Exception'
UNION ALL
SELECT 
'Sent' as countName,
COUNT(*) AS countValue
FROM dbo.FireMarshalExtract
WHERE FireMarshalExtract.reportingStatus = 'Sent'

