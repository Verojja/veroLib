  --------------------------------
  -- PROGRAM: FM Claim Dimension
  -- DEVELOPER: DANIEL RAVAGLIA
  -- DATE: 12/24/2018
  ------------------------------

---------------------------------------------------------------------------
-- Extract scrubs and geocodes
--  needed table: [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Scrubs_Extract] 
---------------------------------------------------------------------------

-- Keys for CLT1
DROP TABLE [ClaimSearch_Dev].[dbo].[FM_Proc_CLT1_Extract]
SELECT [ALLCLMROWID], A.I_ALLCLM 
INTO [ClaimSearch_Dev].[dbo].[FM_Proc_CLT1_Extract]
FROM [ClaimSearch_Prod].[dbo].[CLT00001]A
INNER JOIN [ClaimSearch_Dev].[dbo].[FM_ExtractFile]B
ON A.I_ALLCLM = B.I_ALLCLM

-- Keys for scrubs, geocodes
DROP TABLE [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Keys_Extract]
SELECT A.ALLCLMROWID, AddressKey 
INTO [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Keys_Extract]
FROM [ClaimSearch].[dbo].[CS_Lookup_Melissa_Address_Mapping_to_CLT00001]A
INNER JOIN [ClaimSearch_Dev].[dbo].[FM_Proc_CLT1_Extract]B
ON A.ALLCLMROWID = B.ALLCLMROWID

-- Extract Scrubs, Geocodes
drop table [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Scrubs_Extract]   
SELECT DISTINCT
	   A.[AddressKey]  
	  ,B.ALLCLMROWID
	  ,C.I_ALLCLM
	  ,TABLENAME    
	  ,[ADR_LN1_V]
	  ,[ADR_LN2_V]
	  ,[CITY_V]
	  ,[State]
	  ,[ZIP_V]
	  ,[MD_Address]
	  ,[MD_Address2]
	  ,[MD_Suite]
	  ,[MD_City]
	  ,[MD_State]
	  ,[MD_Zip]
	  ,[MD_Plus4]
	  ,[MD_Results]      
	  ,[MD_Latitude]
	  ,[MD_Longitude]      
	  ,[MD_Country]      
	  ,[MD_AddressTypeDesc]      
	  ,[MD_GeoCounty]
	  ,[MD_GeoCountyFIPS]
	  ,[MD_TimeZone]
	  ,[MD_TZCode]
	  ,[MD_AddrName]
	  ,[MDAddrSuiteName]
	  ,[MD_AddrSuiteRange]
	  ,[MD_AddrPMBName]
	  ,[MD_AddrPMBRange]
	  ,[MD_AddrExtraInformation]
	  ,[MD_AddrCountyName]
	  ,[MD_AddrCountyFIPS]
	  ,[MD_Placecode]
	  ,[MD_PlaceName]   
  INTO [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Scrubs_Extract]   
  FROM [ClaimSearch].[dbo].[CS_Lookup_Unique_Addresses_Melissa_Output]A
  
  INNER JOIN [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Keys_Extract]B
  ON A.AddressKey = B.AddressKey
  
  INNER JOIN [ClaimSearch_Dev].[dbo].[FM_Proc_CLT1_Extract]C
  ON B.ALLCLMROWID  = C.ALLCLMROWID  
CREATE UNIQUE INDEX I_ALLCLM_UIDX ON [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Scrubs_Extract] (I_ALLCLM)   
  
--------------------------------
--- main claim dimension
------------------------------
drop table [ClaimSearch_Dev].[dbo].[DIM_FM_Claim]
SELECT 
         CAST(main.I_ALLCLM AS CHAR(11)) AS I_ALLCLM
		--,CAST(main.FM_Perspective AS CHAR(25)) AS FM_Perspective
		--,CAST(main.FLAG_Amount_Qualifier_Provided AS CHAR(1)) AS FLAG_Amount_Qualifier_Provided
		--,main.Included_in_FM_Report  --// WILL REMOVE AFTER TESTING
		--,main.State_Status
		--,main.Date_Insert
		,CAST(clt1.I_CUST AS CHAR(4)) AS I_CUST
		,CAST(clt1.I_REGOFF AS CHAR(5)) AS I_REGOFF
		,CAST(clt1.N_POL AS VARCHAR(30)) AS N_POL
		,CAST(clt1.N_CLM AS VARCHAR(30)) AS N_CLM
		,CAST(clt1.D_OCUR AS DATE) AS D_OCUR
		,CAST(clt1.H_OCUR AS CHAR(4)) AS H_OCUR
		,CAST(clt1.F_AM_PM AS CHAR(1)) AS F_AM_PM
		,CAST(clt1.C_POL_TYP AS CHAR(4)) AS C_POL_TYP
		,CAST(clt1.T_LOSS_DSC AS VARCHAR(50)) AS T_LOSS_DSC
		,CAST(clt1.T_LOL_STR1 AS VARCHAR(50)) AS T_LOL_STR1
		,CAST(clt1.M_LOL_CITY AS VARCHAR(25)) AS M_LOL_CITY
		,CAST(clt1.C_LOL_ST_ALPH AS CHAR(2)) AS C_LOL_ST_ALPH
		,CAST(clt1.C_LOL_ZIP AS CHAR(5)) AS C_LOL_ZIP
		,CAST(clt1.M_SIU_CO AS VARCHAR(70)) AS M_SIU_CO
		,CAST(clt1.C_BUS_TYP AS char(2)) AS C_BUS_TYP
		,CAST(clt1.D_RCV AS CHAR(26)) AS D_RCV
		,CAST(clt1.C_LOSS_TYP AS CHAR(4)) AS C_LOSS_TYP	
		,CAST(clt1.M_FUL_NM_SIU AS VARCHAR(70)) AS M_FUL_NM_SIU
		,CAST(clt1.N_AREA_WK_SIU AS SMALLINT) AS N_AREA_WK_SIU
		,CAST(clt1.N_TEL_WK_SIU AS INT) AS N_TEL_WK_SIU
		,CAST(clt1.N_AREA_CELL_SIU AS SMALLINT) AS N_AREA_CELL_SIU
		,CAST(clt1.N_TEL_CELL_SIU AS INT) AS N_TEL_CELL_SIU
		,CAST(scrub.MD_Address AS VARCHAR(50)) AS Scrub_Address
		,CAST(scrub.MD_Address2 AS VARCHAR(50)) AS Scrub_Address2
		,CAST(scrub.MD_City AS VARCHAR(35)) AS Scrub_City
		,CAST(scrub.MD_State AS CHAR(15)) AS Scrub_State
		,CAST(scrub.MD_Zip AS CHAR(5)) AS Scrub_Zip
		,CAST(scrub.MD_Plus4 AS CHAR(4)) AS Scrub_Zip4
		,CAST(scrub.MD_GeoCounty AS VARCHAR(25)) AS Scrub_GeoCounty 
		,CAST(scrub.MD_GeoCountyFIPS AS CHAR(5)) AS Scrub_GeoCountyFIPS
		,CAST(scrub.MD_Latitude AS CHAR(12)) AS Geo_Latitude
		,CAST(scrub.MD_Longitude AS CHAR(12)) AS Geo_Longitude
		,CASE WHEN Scrub.MD_Results LIKE '%GS01%' THEN CAST('Street' AS varchar(15))
			WHEN Scrub.MD_Results LIKE '%GS02%' THEN CAST('Neighborhood' AS varchar(15))
			WHEN Scrub.MD_Results LIKE '%GS03%' THEN CAST('Community Level' AS varchar(15))
			WHEN Scrub.MD_Results LIKE '%GS04%' THEN CAST('State' AS varchar(15))
			WHEN Scrub.MD_Results LIKE '%GS05%' THEN CAST('Rooftop' AS varchar(15))
			WHEN Scrub.MD_Results LIKE '%GS06%' THEN CAST('INTerpolated Rooftop' AS varchar(15))
		    WHEN Scrub.MD_Results LIKE '%GS10%' THEN CAST('Wire Center Lat/Long' AS varchar(15))            
            WHEN Scrub.MD_Results LIKE '%GE02%' THEN CAST('Coordinates Not DB' AS varchar(15))
            WHEN Scrub.MD_Results LIKE '%GE50%' THEN CAST('Invalid Lat/Long' AS varchar(15))
            WHEN Scrub.MD_Results LIKE '%GE01%' THEN CAST('Invalid Postal Code' AS varchar(15))
			else CAST('Not Geocoded' AS varchar(15)) 
	   END AS GEOCODE_LEVEL
  into [ClaimSearch_Dev].[dbo].[DIM_FM_Claim]
  FROM [ClaimSearch_Dev].[dbo].[FM_ExtractFile]main
  
  
  inner join [ClaimSearch_Prod].[dbo].[CLT00001]clt1
  on main.i_allclm = clt1.I_ALLCLM
  AND main.Date_Insert = CLT1.DATE_INSERT

  left join [ClaimSearch_Dev].[dbo].[FM_Proc_Address_Scrubs_Extract]scrub
  on scrub.I_ALLCLM = main.i_allclm
  
  create unique index i_allclm_idx on [ClaimSearch_Dev].[dbo].[DIM_FM_Claim] (I_ALLCLM)