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
