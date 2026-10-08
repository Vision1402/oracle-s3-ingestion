-- =====================================================================
-- PROJECT 3 - PHASE 0 - Script 00: create the NORTHWIND schema owner
-- Run as SYSTEM, connected to the pluggable database FREEPDB1.
-- In Oracle, a "user" and a "schema" are the same thing: tables belong to a user.
-- =====================================================================
SET ECHO ON

CREATE USER northwind IDENTIFIED BY "Northwind123"
  DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users;

-- Only the privileges the legacy app needs (least privilege again, like Project 1)
GRANT CREATE SESSION,     -- log in
      CREATE TABLE,
      CREATE VIEW,
      CREATE SEQUENCE,    -- auto-numbering
      CREATE TRIGGER,
      CREATE PROCEDURE,   -- PL/SQL business logic
      CREATE JOB          -- DBMS_SCHEDULER nightly batch
  TO northwind;

-- Lets NORTHWIND read Oracle's data dictionary during the assessment phase
GRANT SELECT_CATALOG_ROLE TO northwind;

EXIT
