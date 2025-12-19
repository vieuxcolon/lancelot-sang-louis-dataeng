-- create postgres superuser if missing
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres') THEN
    CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'postgres';
  END IF;
END $$;

-- create druid role
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'druid') THEN
    CREATE ROLE druid LOGIN PASSWORD 'druid';
  END IF;
END $$;

-- create druid database
CREATE DATABASE druid OWNER druid;
