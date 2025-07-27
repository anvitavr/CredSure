-- 06_test_alert_inserts.sql

USE BEMM459_ARAUT;
GO

-- STEP 0: Update customer names and emails with realistic values
PRINT 'Updating customer names and emails...';

-- Drop temp table if it exists
IF OBJECT_ID('tempdb..#CustomerNames') IS NOT NULL DROP TABLE #CustomerNames;

-- Create temp table with 25 names and emails
CREATE TABLE #CustomerNames (
    row_num INT PRIMARY KEY,
    name NVARCHAR(100),
    email NVARCHAR(100)
);

-- Insert 25 names
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
    (10, 'Olivia King', 'olivia.king@example.uk'),
    (11, 'Daniel Wright', 'daniel.wright@example.uk'),
    (12, 'Lily Scott', 'lily.scott@example.uk'),
    (13, 'Matthew Hill', 'matthew.hill@example.uk'),
    (14, 'Grace Adams', 'grace.adams@example.uk'),
    (15, 'Andrew Baker', 'andrew.baker@example.uk'),
    (16, 'Chloe Turner', 'chloe.turner@example.uk'),
    (17, 'Nathan Walker', 'nathan.walker@example.uk'),
    (18, 'Ella Martin', 'ella.martin@example.uk'),
    (19, 'Luke Allen', 'luke.allen@example.uk'),
    (20, 'Isla Parker', 'isla.parker@example.uk'),
    (21, 'Harry Evans', 'harry.evans@example.uk'),
    (22, 'Mia Cooper', 'mia.cooper@example.uk'),
    (23, 'Ryan Ward', 'ryan.ward@example.uk'),
    (24, 'Lucy James', 'lucy.james@example.uk'),
    (25, 'Ben Hughes', 'ben.hughes@example.uk');

-- Randomly assign these names to existing customers
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

DECLARE @i INT = 1;
DECLARE @amount DECIMAL(12,2);
DECLARE @account_id UNIQUEIDENTIFIER;
DECLARE @customer_id UNIQUEIDENTIFIER;
DECLARE @device_id UNIQUEIDENTIFIER;

WHILE @i <= 3
BEGIN
    SET @amount = 
        CASE @i
            WHEN 1 THEN 5000.00  -- Moderate
            WHEN 2 THEN 7000.00  -- High
            WHEN 3 THEN 9000.00  -- Critical
        END;

    -- Get random account and customer
    SELECT TOP 1 
        @account_id = a.account_id,
        @customer_id = a.customer_id
    FROM fraud.accounts a
    ORDER BY NEWID();

    -- Get a device linked to that customer
    SELECT TOP 1 @device_id = cd.device_id
    FROM fraud.customer_devices cd
    WHERE cd.customer_id = @customer_id
    ORDER BY NEWID();

    -- Insert transaction
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

-- STEP 2: Execute fraud detection stored procedure
PRINT 'Running fraud detection procedure...';
EXEC fraud.usp_FlagHighAmountTransactions;

-- STEP 3: Check alerts with customer details
PRINT 'Displaying generated fraud alerts with customer details...';

SELECT 
    fa.alert_id,
    fa.transaction_id,
    fa.severity,
    fa.description,
    fa.created_at,
    c.customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    t.amount,
    t.timestamp
FROM fraud.fraud_alerts fa
JOIN fraud.transactions t ON fa.transaction_id = t.transaction_id
JOIN fraud.customers c ON t.customer_id = c.customer_id
ORDER BY fa.created_at DESC;
