-- 01_create_fraud_schema.sql

-- Create schema for fraud detection
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fraud')
BEGIN
    EXEC('CREATE SCHEMA fraud');
END
