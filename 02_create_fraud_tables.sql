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
