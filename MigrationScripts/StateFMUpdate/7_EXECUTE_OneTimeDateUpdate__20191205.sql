/*This will update the claims in our system with lattitude, longitude, county, and other address datapoints that they don't currently have.
	 This should take anywhere from 20-40 minutes, depending on server traffic at the time you execute.*/

USE ClaimSearch_Prod;
EXEC dbo.hsp_FireMarshalSendClaims
	@executionDateParam = '20150101';