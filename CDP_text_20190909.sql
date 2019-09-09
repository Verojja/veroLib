
		/*ClaimReferenceExtract.userAddressLine1,*/
		/*ClaimReferenceExtract.userCity,*/
		/*ClaimReferenceExtract.userState,*/
		/*ClaimReferenceExtract.userZipCode,*/
		/*ClaimReferenceExtract.concatendatedProductHierarchyLvl AS Product_Location,*/
		/*ClaimReferenceExtract.productTransactionDescription,*/
		/*The representing the following mutually-exclusive/binary values as INT to stay consistent with strucutres
			that were developed against. Would NOT recommend following this example in the future.
		*/
								Robert was brought into the Claim Director Platinum project in early August 2019 to assist with data automation.  Ideally, he should have been part of the process months earlier. This created an environment where he had to complete several months’ worth of work in just one month. He put in hundreds of extra hours to deliver a working data model and process that met the needs of the Anti-Fraud Solutions business unit.
								Robert has been invaluable to the Anti-Fraud Solutions business unit to support the delivery of their industry predictive model solution to enhance the Claim Director product.  They have routinely commented in private and public about how he goes above and beyond to fully understand the business’ needs and the quality of his work is second to none.  He takes pride in coming up with creative and innovative solutions to meet the needs of our business partners.  The following quote from Tom Love and Jim Hulett says it all. “Robert is a collaborative partner to this work that we are lucky to have. Thank you for everything Robert we look forward to continuing the collaboration with you and the Anti-Fraud Solutions team.” 

Responsible for data modeling
and data architecture for datawarehouse
re-architect datawarehouse

achictected datamodel solutions (including automated metadata log capture 

consistently pioneers solutoins to uncommon problems thought impossible by the business

"
onsistently exceeds expectations:

There were many challenges to building out the DN model.  It's the most complex data model we have to date, and it's poorly understood. The source data is also derived from a system that was inefficiently designed and Robert is downstream of that.  In absence of clear knowledge transfer (on the expenditure side) Robert was able to build a properly constrained system that has exceeded all my expectations.  In short, he accomplished what we were told could not be done.

	
Consistently exceeds expectations:

Robert's ideas for future coding standards have exceeded expectations.  I look forward to seeing this in affect against all of our applications.

 Consistently Exceeds Expectations.

Robert has exceeded expectations around data modelling for ClaimSearch data.  He has taken great initiative to go beyond the technical workings of the data modelling software and has taken a deep dive into the insurance domain at a policy level.  This understanding will serve us well as we go beyond merely repackaging the date directly from the source table and instead add value for more strategic use cases for our data models.

s:	
Exceptional Performance:

Robert has created the premire roadmap with respect to SQL coding practices, naming convention and data modeling design all while keeping an eye on business practice and implementation.  His breadth of knowledge in this area is unparelled and I look forward to see it will be leveraged in the futre. 


:	
Exceptional Performance

Robert has become the expert in this domain point simple.  He has the combined business/technical/data understanding that has been desperately needed and his overall knowledge rivals the developers who have worked for years in this area.  This product also combined the billing systems we currently have in place which are for more products than just DN so this knowledge will come in useful later on.

Roberts management of this project also needs to be mentioned.  He has taken the role of basically consistently moving this along and that is the role of a project manager. 

Robert has also taken the initiative to work with the BI team to improve the performance of corresponding dashboard.

	
Robert's current job duties have hindered his ability to meet this goal.  This is not reflective of his ability to meet the task as much as it reflects the amount of time he has available.

Robert has a few areas to improve on. 

Patience - while Robert is truly an expert level engineer, he "at times" needs to be more patient with those who are not at his level.

Time Management - there are significant hurdles at Verisk with respect to providing access to all the tools that are required in order to be effective as a data engineer (e.g. lack of permissions and poor resource allocations).  Robert will need to better balance the need to do what it right against what is required in a timely manner.

skill development - Robert should continue to develop his skillset as we move away from SQL server and more towords Snowflake and other big data tools.




"


		/**********************************************/
		COALESCE(ClaimReferenceExtract.lineItemCost, ClaimReferenceExtract.recentlyObservedLineItemCost) AS A_LN_ITM_EXTN_TR,
		ClaimReferenceExtract.unitTax AS A_LN_ITM_TAX_TR,
		ClaimReferenceExtract.invoiceDate,
		ClaimReferenceExtract.invoiceNumber,
		ClaimReferenceExtract.productCode,
		/*Columns Deprecated
			ClaimReferenceExtract.vendorTransactionDescription AS [T_VEND_TRNS],
			ClaimReferenceExtract.vendorAccountType AS [ACT_TYP],
		*/
		ClaimReferenceExtract.vendorId,
		/*ClaimReferenceExtract.islocationSearchUsed AS [LOCATION SEARCH ENTRY],*/
		/*ClaimReferenceExtract.isPersonalSearchUsed AS [PERSONAL SEARCH ENTRY],*/
		/*ClaimReferenceExtract.isVehicleSearchUsed AS [VEHICLE SEARCH ENTRY]*/
		dateInserted
	FROM
		DecisionNet.ClaimReferenceExtract  WITH(NOLOCK)
		LEFT OUTER JOIN dbo.V_MM_Hierarchy AS CompanyHeirarchy WITH(NOLOCK)
			ON CompanyHeirarchy.lvl0 = ClaimReferenceExtract.companySoldToCode
	WHERE
		/*Refactor to DATEFROMPARTS in SQLSERVER 2012,
			use of BETWEEN preservs potential indexes on the DATE column*/
		ClaimReferenceExtract.transactionDate >= CAST(CAST((YEAR(GETDATE())-4) AS CHAR(4)) +'0101' AS DATE)
);