----revised full sql code
-- 01_create_fraud_schema.sql

-- Create schema for fraud detection
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fraud')
BEGIN
    EXEC('CREATE SCHEMA fraud');
END

-- 02_create_fraud_tables.sql

-- Create tables in fraud schema

CREATE TABLE fraud.customers (
    customer_id UNIQUEIDENTIFIER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    join_date DATE NOT NULL,
    risk_score INT DEFAULT 50
);

CREATE TABLE fraud.accounts (
    account_id UNIQUEIDENTIFIER PRIMARY KEY,
    customer_id UNIQUEIDENTIFIER,
    account_type VARCHAR(20) NOT NULL,
    balance DECIMAL(12, 2) DEFAULT 0.00,
    FOREIGN KEY (customer_id) REFERENCES fraud.customers(customer_id)
);

CREATE TABLE fraud.devices (
    device_id UNIQUEIDENTIFIER PRIMARY KEY,
    device_type VARCHAR(50),
    ip_address VARCHAR(45)
);

CREATE TABLE fraud.customer_devices (
    customer_id UNIQUEIDENTIFIER,
    device_id UNIQUEIDENTIFIER,
    PRIMARY KEY (customer_id, device_id),
    FOREIGN KEY (customer_id) REFERENCES fraud.customers(customer_id),
    FOREIGN KEY (device_id) REFERENCES fraud.devices(device_id)
);

CREATE TABLE fraud.transactions (
    transaction_id UNIQUEIDENTIFIER PRIMARY KEY,
    account_id UNIQUEIDENTIFIER,
    customer_id UNIQUEIDENTIFIER,
    device_id UNIQUEIDENTIFIER,
    amount DECIMAL(12, 2) NOT NULL,
    merchant VARCHAR(100),
    location VARCHAR(100),
    timestamp DATETIME NOT NULL,
    is_flagged BIT DEFAULT 0,
    reason VARCHAR(255),
    FOREIGN KEY (account_id) REFERENCES fraud.accounts(account_id),
    FOREIGN KEY (customer_id) REFERENCES fraud.customers(customer_id),
    FOREIGN KEY (device_id) REFERENCES fraud.devices(device_id),
    FOREIGN KEY (customer_id, device_id) REFERENCES fraud.customer_devices(customer_id, device_id)
);

CREATE TABLE fraud.fraud_alerts (
    alert_id UNIQUEIDENTIFIER PRIMARY KEY,
    transaction_id UNIQUEIDENTIFIER,
    severity VARCHAR(20),
    description VARCHAR(255),
    created_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (transaction_id) REFERENCES fraud.transactions(transaction_id)
);

-----03_insert_fraud_dummy_data
USE BEMM459_ARAUT;
GO

-- STEP 1: Insert 25 customers
DECLARE @i INT = 1;

WHILE @i <= 25
BEGIN
    INSERT INTO fraud.customers (customer_id, name, email, join_date, risk_score)
    VALUES (
        NEWID(),
        CONCAT('Customer_', @i),
        CONCAT('customer', @i, '@example.com'),
        DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 100), GETDATE()),
        CAST(RAND() * 50 + 50 AS INT)
    );
    SET @i += 1;
END;

-- STEP 2: Create 25 accounts (1 per customer)
DECLARE customer_cursor CURSOR FOR
SELECT customer_id FROM fraud.customers ORDER BY NEWID();

DECLARE @cust_id UNIQUEIDENTIFIER;

OPEN customer_cursor;
FETCH NEXT FROM customer_cursor INTO @cust_id;

WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO fraud.accounts (account_id, customer_id, account_type, balance)
    VALUES (
        NEWID(),
        @cust_id,
        CASE WHEN RAND() < 0.5 THEN 'Savings' ELSE 'Checking' END,
        ROUND(RAND() * 9000 + 1000, 2)
    );
    FETCH NEXT FROM customer_cursor INTO @cust_id;
END;

CLOSE customer_cursor;
DEALLOCATE customer_cursor;

-- STEP 3: Insert 50 devices and assign them to random customers
DECLARE @j INT = 1;
DECLARE @device_id UNIQUEIDENTIFIER;

WHILE @j <= 50
BEGIN
    SET @device_id = NEWID();

    INSERT INTO fraud.devices (device_id, device_type, ip_address)
    VALUES (
        @device_id,
        CASE WHEN RAND() < 0.5 THEN 'Mobile' ELSE 'Laptop' END,
        CONCAT(CAST(RAND() * 255 AS INT), '.', CAST(RAND() * 255 AS INT), '.', CAST(RAND() * 255 AS INT), '.', CAST(RAND() * 255 AS INT))
    );

    -- Assign this device to a random customer
    INSERT INTO fraud.customer_devices (customer_id, device_id)
    SELECT TOP 1 customer_id, @device_id
    FROM fraud.customers
    ORDER BY NEWID();

    SET @j += 1;
END;

-- STEP 4: Insert 30 transactions using valid (customer_id, device_id) pairs
DECLARE @k INT = 1;
DECLARE @account_id UNIQUEIDENTIFIER;
DECLARE @transaction_id UNIQUEIDENTIFIER;
DECLARE @amount DECIMAL(10,2);
DECLARE @merchant VARCHAR(100);
DECLARE @location VARCHAR(100);
DECLARE @timestamp DATETIME;
DECLARE @flag BIT;
DECLARE @reason VARCHAR(255);
DECLARE @customer_id UNIQUEIDENTIFIER;
DECLARE @device_id_txn UNIQUEIDENTIFIER;

WHILE @k <= 30
BEGIN
    -- Pick a random account
    SELECT TOP 1 
        @account_id = account_id,
        @customer_id = customer_id
    FROM fraud.accounts
    ORDER BY NEWID();

    -- Get a valid device for this customer
    SELECT TOP 1 @device_id_txn = device_id
    FROM fraud.customer_devices
    WHERE customer_id = @customer_id
    ORDER BY NEWID();

    SET @transaction_id = NEWID();
    SET @amount = ROUND(RAND() * 6000 + 100, 2);
    SET @merchant = (SELECT TOP 1 val FROM (VALUES ('Amazon'), ('eBay'), ('Walmart'), ('Target')) AS v(val) ORDER BY NEWID());
    SET @location = (SELECT TOP 1 val FROM (VALUES ('NY'), ('CA'), ('TX'), ('FL')) AS l(val) ORDER BY NEWID());
    SET @timestamp = DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 30), GETDATE());
    SET @flag = CASE WHEN @amount > 4000 THEN 1 ELSE 0 END;
    SET @reason = CASE WHEN @flag = 1 THEN 'High amount' ELSE NULL END;

    INSERT INTO fraud.transactions (transaction_id, account_id, customer_id, device_id, amount, merchant, location, timestamp, is_flagged, reason)
    VALUES (@transaction_id, @account_id, @customer_id, @device_id_txn, @amount, @merchant, @location, @timestamp, @flag, @reason);

    SET @k += 1;
END;

-- 04_flag_high_amount_procedure.sql

USE BEMM459_ARAUT;
GO

-- Drop existing procedure
IF OBJECT_ID('fraud.usp_FlagHighAmountTransactions', 'P') IS NOT NULL
    DROP PROCEDURE fraud.usp_FlagHighAmountTransactions;
GO

-- Create updated procedure with tiered severity
CREATE PROCEDURE fraud.usp_FlagHighAmountTransactions
AS
BEGIN
    SET NOCOUNT ON;

    -- Insert alerts for unflagged high-amount transactions
    INSERT INTO fraud.fraud_alerts (alert_id, transaction_id, severity, description, created_at)
    SELECT 
        NEWID(),
        t.transaction_id,
        CASE
            WHEN t.amount > 8000 THEN 'Critical'
            WHEN t.amount > 6000 THEN 'High'
            ELSE 'Moderate'
        END AS severity,
        CONCAT('Transaction of $', t.amount, ' flagged for review'),
        GETDATE()
    FROM fraud.transactions t
    LEFT JOIN fraud.fraud_alerts fa ON t.transaction_id = fa.transaction_id
    WHERE t.amount > 4000 AND fa.transaction_id IS NULL;

    -- Mark those transactions as flagged
    UPDATE t
    SET 
        t.is_flagged = 1,
        t.reason = 'Flagged due to high transaction amount'
    FROM fraud.transactions t
    LEFT JOIN fraud.fraud_alerts fa ON t.transaction_id = fa.transaction_id
    WHERE t.amount > 4000 AND fa.transaction_id IS NULL;
END;
GO
-- 05_create_fraud_views_and_execute_fla.sql

USE BEMM459_ARAUT;
GO

-- Drop and recreate flagged transaction summary view
IF OBJECT_ID('fraud.vi_flagged_transactions_summary', 'V') IS NOT NULL
    DROP VIEW fraud.vi_flagged_transactions_summary;
GO

CREATE VIEW fraud.vi_flagged_transactions_summary AS
SELECT
    t.transaction_id,
    c.customer_id,
    c.name AS customer_name,
    c.email,
    c.risk_score,
    a.account_id,
    a.account_type,
    t.device_id,
    t.amount,
    t.merchant,
    t.location,
    t.timestamp,
    t.reason,
    fa.severity,
    fa.created_at AS alert_created_at
FROM fraud.transactions t
JOIN fraud.accounts a ON t.account_id = a.account_id
JOIN fraud.customers c ON t.customer_id = c.customer_id
LEFT JOIN fraud.fraud_alerts fa ON t.transaction_id = fa.transaction_id
WHERE t.is_flagged = 1;
GO

-- Weekly alert summary
IF OBJECT_ID('fraud.vi_alerts_per_week', 'V') IS NOT NULL
    DROP VIEW fraud.vi_alerts_per_week;
GO

CREATE VIEW fraud.vi_alerts_per_week AS
SELECT
    DATEPART(YEAR, created_at) AS year,
    DATEPART(WEEK, created_at) AS week,
    COUNT(*) AS total_alerts
FROM fraud.fraud_alerts
GROUP BY DATEPART(YEAR, created_at), DATEPART(WEEK, created_at);
GO

-- Customer risk level buckets
IF OBJECT_ID('fraud.vi_risk_score_buckets', 'V') IS NOT NULL
    DROP VIEW fraud.vi_risk_score_buckets;
GO

CREATE VIEW fraud.vi_risk_score_buckets AS
SELECT
    CASE
        WHEN risk_score < 50 THEN 'Low Risk'
        WHEN risk_score BETWEEN 50 AND 75 THEN 'Medium Risk'
        ELSE 'High Risk'
    END AS risk_level,
    COUNT(*) AS customer_count
FROM fraud.customers
GROUP BY
    CASE
        WHEN risk_score < 50 THEN 'Low Risk'
        WHEN risk_score BETWEEN 50 AND 75 THEN 'Medium Risk'
        ELSE 'High Risk'
    END;
GO

-- Flagged merchant-location heatmap
IF OBJECT_ID('fraud.vi_suspicious_activity_heatmap', 'V') IS NOT NULL
    DROP VIEW fraud.vi_suspicious_activity_heatmap;
GO

CREATE VIEW fraud.vi_suspicious_activity_heatmap AS
SELECT
    merchant,
    location,
    COUNT(*) AS flagged_count
FROM fraud.transactions
WHERE is_flagged = 1
GROUP BY merchant, location;
GO

-- Run the updated fraud detection procedure
EXEC fraud.usp_FlagHighAmountTransactions;
GO
-- 06_test_alert_inserts.sql

USE BEMM459_ARAUT;
GO

-- STEP 0: Update customer names and emails
PRINT 'Updating customer names and emails...';

IF OBJECT_ID('tempdb..#CustomerNames') IS NOT NULL DROP TABLE #CustomerNames;
CREATE TABLE #CustomerNames (
    row_num INT PRIMARY KEY,
    name NVARCHAR(100),
    email NVARCHAR(100)
);

-- Insert sample names
INSERT INTO #CustomerNames (row_num, name, email)
VALUES 
    (1, 'John Wade', 'john.wade@example.uk'),
    (2, 'Alice Grant', 'alice.grant@example.uk'),
    (3, 'Robert Smith', 'robert.smith@example.uk'),
    (4, 'Emma Green', 'emma.green@example.uk'),
    (5, 'Michael Brown', 'michael.brown@example.uk'),
    (6, 'Sophia Clark', 'sophia.clark@example.uk'),
    (7, 'James Hall', 'james.hall@example.uk'),
    (8, 'Emily Lewis', 'emily.lewis@example.uk'),
    (9, 'David Young', 'david.young@example.uk'),
    (10, 'Olivia King', 'olivia.king@example.uk');

;WITH OrderedCustomers AS (
    SELECT customer_id, ROW_NUMBER() OVER (ORDER BY NEWID()) AS row_num
    FROM fraud.customers
)
UPDATE c
SET 
    c.name = cn.name,
    c.email = cn.email
FROM fraud.customers c
JOIN OrderedCustomers oc ON c.customer_id = oc.customer_id
JOIN #CustomerNames cn ON oc.row_num = cn.row_num;

-- STEP 1: Insert test transactions for each severity level
PRINT 'Inserting test transactions with different severity levels...';

DECLARE @account_id UNIQUEIDENTIFIER;
DECLARE @customer_id UNIQUEIDENTIFIER;
DECLARE @device_id UNIQUEIDENTIFIER;
DECLARE @amount DECIMAL(12,2);
DECLARE @i INT = 1;

WHILE @i <= 3
BEGIN
    SET @amount = 
        CASE @i
            WHEN 1 THEN 5000.00   -- Moderate
            WHEN 2 THEN 7000.00   -- High
            WHEN 3 THEN 9000.00   -- Critical
        END;

    SELECT TOP 1 
        @account_id = a.account_id,
        @customer_id = a.customer_id
    FROM fraud.accounts a
    ORDER BY NEWID();

    SELECT TOP 1 @device_id = cd.device_id
    FROM fraud.customer_devices cd
    WHERE cd.customer_id = @customer_id
    ORDER BY NEWID();

    INSERT INTO fraud.transactions (
        transaction_id, account_id, customer_id, device_id,
        amount, merchant, location, timestamp, is_flagged, reason
    )
    VALUES (
        NEWID(), @account_id, @customer_id, @device_id,
        @amount, 'SeverityTestMerchant', 'SeverityCity', GETDATE(), 1, 'Inserted for severity test'
    );

    SET @i += 1;
END;

-- STEP 2: Trigger fraud detection
PRINT 'Running fraud detection procedure...';
EXEC fraud.usp_FlagHighAmountTransactions;

-- STEP 3: Show generated alerts
SELECT 
    fa.alert_id,
    fa.transaction_id,
    fa.severity,
    fa.description,
    fa.created_at,
    c.customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    t.amount
FROM fraud.fraud_alerts fa
JOIN fraud.transactions t ON fa.transaction_id = t.transaction_id
JOIN fraud.customers c ON t.customer_id = c.customer_id
ORDER BY fa.created_at DESC;



 