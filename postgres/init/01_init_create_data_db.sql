-- Create the ETL user
CREATE ROLE data_user LOGIN PASSWORD 'root';

-- Create the data_db database owned by data_user
CREATE DATABASE data_db OWNER data_user;

-- Grant all privileges on the database to data_user
\connect data_db
GRANT ALL PRIVILEGES ON DATABASE data_db TO data_user;

