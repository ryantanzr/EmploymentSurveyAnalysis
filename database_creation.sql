CREATE DATABASE "graduate_employment_survey"
	WITH
	OWNER = postgres
	ENCODING = 'UTF8'
	TABLESPACE = pg_default
	CONNECTION LIMIT = -1;

-- Creating an ETL User
CREATE USER etl WITH PASSWORD 'learning247';
-- Grant connection
GRANT CONNECT ON DATABASE "graduate_employment_survey" TO etl;
-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl;