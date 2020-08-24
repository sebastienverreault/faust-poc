CREATE KEYSPACE weblogs 
	WITH replication = { 
		'class': 'SimpleStrategy', 
		'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS weblogs.logs ( 
	ip_address text,
	user_agent text,
	request text,
	byte_ranges list<text>,
	PRIMARY KEY (ip_address, user_agent, request)
);

