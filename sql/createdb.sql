CREATE DATABASE "twitterusers_IN"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_India.1252'
    LC_CTYPE = 'English_India.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "twitterusers_IN"
    IS 'This DB has a collection of twitter user details from India.';