claimId
originalClaimNumber
claimSourceSystem
claimEntryMethod
endedForReasonCode
lossDescription
lossDescriptionExtended
catastropheId
isClaimSearchProperty
isClaimSearchAuto
isClaimSearchCasualty
isClaimSearchAPD
dateOfLoss
insuranceCompanyReceivedDate
systemDateReceived
isActive
dateInserted
isoClaimId

Claim.claimId,
Claim.originalClaimNumber,
Claim.claimSourceSystem,
Claim.claimEntryMethod,
Claim.endedForReasonCode,
Claim.lossDescription,
Claim.lossDescriptionExtended,
Claim.catastropheId,
Claim.isClaimSearchProperty,
Claim.isClaimSearchAuto,
Claim.isClaimSearchCasualty,
Claim.isClaimSearchAPD,
Claim.dateOfLoss,
Claim.insuranceCompanyReceivedDate,
Claim.systemDateReceived,
Claim.isActive,
Claim.dateInserted,
Claim.isoClaimId

SOURCE.claimId,
SOURCE.originalClaimNumber,
SOURCE.claimSourceSystem,
SOURCE.claimEntryMethod,
SOURCE.endedForReasonCode,
SOURCE.lossDescription,
SOURCE.lossDescriptionExtended,
SOURCE.catastropheId,
SOURCE.isClaimSearchProperty,
SOURCE.isClaimSearchAuto,
SOURCE.isClaimSearchCasualty,
SOURCE.isClaimSearchAPD,
SOURCE.dateOfLoss,
SOURCE.insuranceCompanyReceivedDate,
SOURCE.systemDateReceived,
SOURCE.isActive,
SOURCE.dateInserted,
SOURCE.isoClaimId

Claim.claimId
Claim.originalClaimNumber
Claim.claimSourceSystem
Claim.claimEntryMethod
Claim.endedForReasonCode
Claim.lossDescription
Claim.lossDescriptionExtended
Claim.catastropheId
Claim.isClaimSearchProperty
Claim.isClaimSearchAuto
Claim.isClaimSearchCasualty
Claim.isClaimSearchAPD
Claim.dateOfLoss
Claim.insuranceCompanyReceivedDate
Claim.systemDateReceived
Claim.isActive
Claim.dateInserted
Claim.isoClaimId




Claim.originalClaimNumber <> SOURCE.originalClaimNumber
OR Claim.claimSourceSystem <> SOURCE.claimSourceSystem
OR Claim.claimEntryMethod <> SOURCE.claimEntryMethod
OR Claim.endedForReasonCode <> SOURCE.endedForReasonCode
OR Claim.lossDescription <> SOURCE.lossDescription
OR Claim.lossDescriptionExtended <> SOURCE.lossDescriptionExtended
OR Claim.catastropheId <> SOURCE.catastropheId
OR Claim.isClaimSearchProperty <> SOURCE.isClaimSearchProperty
OR Claim.isClaimSearchAuto <> SOURCE.isClaimSearchAuto
OR Claim.isClaimSearchCasualty <> SOURCE.isClaimSearchCasualty
OR Claim.isClaimSearchAPD <> SOURCE.isClaimSearchAPD
OR Claim.dateOfLoss <> SOURCE.dateOfLoss
OR Claim.insuranceCompanyReceivedDate <> SOURCE.insuranceCompanyReceivedDate
OR Claim.systemDateReceived <> SOURCE.systemDateReceived
OR Claim.isActive <> SOURCE.isActive
OR Claim.dateInserted <> SOURCE.dateInserted
OR Claim.isoClaimId <> SOURCE.isoClaimId

