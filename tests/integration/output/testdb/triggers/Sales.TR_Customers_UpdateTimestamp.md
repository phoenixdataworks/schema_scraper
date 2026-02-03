# Sales.TR_Customers_UpdateTimestamp

**Table:** [Sales.Customers](../tables/Sales.Customers.md)

**Type:** AFTER

**Events:** UPDATE

## Definition

```sql

CREATE TRIGGER Sales.TR_Customers_UpdateTimestamp ON Sales.Customers AFTER UPDATE AS
BEGIN SET NOCOUNT ON; UPDATE Sales.Customers SET UpdatedAt = GETUTCDATE() FROM Sales.Customers c INNER JOIN inserted i ON c.CustomerID = i.CustomerID; END;

```
