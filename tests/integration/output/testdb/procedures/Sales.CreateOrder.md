# Sales.CreateOrder

**Language:** T-SQL

## Parameters

| Name | Type | Direction | Default |
|------|------|-----------|---------|
| @CustomerID | int | INPUT |  |
| @Notes | nvarchar(max) | INPUT |  |
| @OrderID | int | OUTPUT |  |

## Definition

```sql

CREATE PROCEDURE Sales.CreateOrder @CustomerID INT, @Notes NVARCHAR(MAX) = NULL, @OrderID INT OUTPUT AS
BEGIN SET NOCOUNT ON; INSERT INTO Sales.Orders (CustomerID, Notes) VALUES (@CustomerID, @Notes); SET @OrderID = SCOPE_IDENTITY(); END;

```
