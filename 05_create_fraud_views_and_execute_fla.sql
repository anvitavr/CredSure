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
