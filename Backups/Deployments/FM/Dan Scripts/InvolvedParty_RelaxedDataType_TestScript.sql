SELECT TOP (10)
	InvolvedParty.taxIdentificationNumberObfuscated,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.taxIdentificationNumberObfuscated) > 15

SELECT TOP (10)
	InvolvedParty.taxIdentificationNumberLastFour,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.taxIdentificationNumberLastFour) > 4
	
SELECT TOP (10)
	InvolvedParty.socialSecurityNumberObfuscated,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.socialSecurityNumberObfuscated) > 30
	
SELECT TOP (10)
	InvolvedParty.socialSecurityNumberLastFour,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.socialSecurityNumberLastFour) > 4
	
SELECT TOP (10)
	InvolvedParty.hICNObfuscated,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.hICNObfuscated) > 36
	
SELECT TOP (10)
	InvolvedParty.driversLicenseNumberObfuscated,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.driversLicenseNumberObfuscated) > 16

SELECT TOP (10)
	InvolvedParty.driversLicenseNumberLast3,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.driversLicenseNumberLast3) > 3

SELECT TOP (10)
	InvolvedParty.driversLicenseClass,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.driversLicenseClass) > 3

SELECT TOP (10)
	InvolvedParty.driversLicenseState,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.driversLicenseState) > 2

SELECT TOP (10)
	InvolvedParty.genderCode,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.genderCode) > 2

SELECT TOP (10)
	InvolvedParty.passportID,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.passportID) > 9

SELECT TOP (10)
	InvolvedParty.professionalMedicalLicense,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.professionalMedicalLicense) > 20

SELECT TOP (10)
	InvolvedParty.fullName,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.fullName) > 70


SELECT TOP (10)
	InvolvedParty.suffix,
	*
FROM dbo.InvolvedParty WITH (NOLOCK)
WHERE LEN(InvolvedParty.suffix) > 50
