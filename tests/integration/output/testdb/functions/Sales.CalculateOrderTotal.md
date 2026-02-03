# Sales.CalculateOrderTotal

**Type:** Scalar
**Language:** T-SQL

## Parameters

| Name | Type | Default |
|------|------|---------|
| @OrderID | int |  |

## Returns

`decimal(12,2)`

## Definition

```sql

CREATE FUNCTION Sales.CalculateOrderTotal(@OrderID INT) RETURNS DECIMAL(12,2) AS
BEGIN
    DECLARE @Total DECIMAL(12,2);
    SELECT @Total = ISNULL(SUM(Quantity * UnitPrice * (1 - DiscountPercent/100)), 0) FROM Sales.OrderItems WHERE OrderID = @OrderID;
    RETURN @Total;
END;

```
