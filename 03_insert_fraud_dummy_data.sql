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
