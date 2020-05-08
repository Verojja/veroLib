/*
Dev note:

Financial Market Performance Gain which removes the need for entity resolution:
	1.) Put N rows (basd on N sources) for a single entity into a single object.
	2.) When exposing an entity, partition the rows and order by source-priority based on business product,
	3.) Filter to first/single-row-returned.


*/