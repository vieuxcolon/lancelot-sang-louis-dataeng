-- Create druid role
CREATE ROLE druid LOGIN PASSWORD 'druid';

-- Create druid database owned by druid
CREATE DATABASE druid OWNER druid;

-- Grant all privileges on the druid database
\connect druid
GRANT ALL PRIVILEGES ON DATABASE druid TO druid;

