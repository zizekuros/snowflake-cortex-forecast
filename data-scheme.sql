create or replace TABLE MYDATABASE.PUBLIC.EVENTS (
	TIME TIMESTAMP_NTZ(9) NOT NULL,
	EVENT_TYPE VARCHAR(50) NOT NULL
);