# Inventory.GetProductsByCategory

**Type:** Inline Table Valued
**Language:** T-SQL

## Parameters

| Name | Type | Default |
|------|------|---------|
| @Category | nvarchar(100) |  |

## Return Columns

| Column | Type | Nullable |
|--------|------|----------|
| ProductID | int | NO |
| SKU | nvarchar(50) | NO |
| Name | nvarchar(200) | NO |
| UnitPrice | decimal(10,2) | NO |
| QuantityInStock | int | NO |

## Definition

```sql

CREATE FUNCTION Inventory.GetProductsByCategory(@Category NVARCHAR(100)) RETURNS TABLE AS
RETURN (SELECT ProductID, SKU, Name, UnitPrice, QuantityInStock FROM Inventory.Products WHERE Category = @Category AND IsDiscontinued = 0);

```
