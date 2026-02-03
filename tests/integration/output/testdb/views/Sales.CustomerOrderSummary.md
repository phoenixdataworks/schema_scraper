# Sales.CustomerOrderSummary

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| CustomerID | int | NO |  |
| CustomerName | nvarchar(101) | NO |  |
| Email | nvarchar(255) | NO |  |
| TotalOrders | int | YES |  |
| LifetimeValue | decimal(38,2) | NO |  |
| LastOrderDate | datetime2 | YES |  |

## Base Tables

- Sales.Customers
- Sales.Orders

## Definition

```sql

CREATE VIEW Sales.CustomerOrderSummary AS
SELECT
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.Email,
    COUNT(o.OrderID) AS TotalOrders,
    ISNULL(SUM(o.TotalAmount), 0) AS LifetimeValue,
    MAX(o.OrderDate) AS LastOrderDate
FROM Sales.Customers c
LEFT JOIN Sales.Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName, c.Email;

```
