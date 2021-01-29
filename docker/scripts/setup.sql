USE master;
GO
CREATE DATABASE swift_tds_database;
GO
CREATE LOGIN swift_tds_user 
    WITH PASSWORD = 'SwiftTDS!';
GO
CREATE USER swift_tds_user FOR LOGIN swift_tds_user;
GO
USE swift_tds_database
GO
CREATE USER swift_tds_user FOR LOGIN swift_tds_user;
GO
GRANT SELECT TO swift_tds_user;
GO