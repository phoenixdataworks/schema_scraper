# Inventory.UpdateStock

**Language:** T-SQL

## Parameters

| Name | Type | Direction | Default |
|------|------|-----------|---------|
| @ProductID | int | INPUT |  |
| @QuantityChange | int | INPUT |  |

## Definition

```sql

CREATE PROCEDURE Inventory.UpdateStock @ProductID INT, @QuantityChange INT AS
BEGIN SET NOCOUNT ON; UPDATE Inventory.Products SET QuantityInStock = QuantityInStock + @QuantityChange WHERE ProductID = @ProductID; END;

```
