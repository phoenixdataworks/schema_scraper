-- MS SQL Server Test Schema for Schema Scraper
-- Contains 1-2 objects of each supported type

USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'TestDB')
BEGIN
    ALTER DATABASE TestDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE TestDB;
END
GO

CREATE DATABASE TestDB;
GO

USE TestDB;
GO

-- =============================================================================
-- SCHEMAS
-- =============================================================================
CREATE SCHEMA Sales;
GO

CREATE SCHEMA Inventory;
GO

-- =============================================================================
-- SEQUENCES
-- =============================================================================
CREATE SEQUENCE Sales.OrderSeq
    AS INT
    START WITH 1000
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9999999
    CACHE 10;
GO

CREATE SEQUENCE Inventory.ProductSeq
    AS INT
    START WITH 1
    INCREMENT BY 1;
GO

-- =============================================================================
-- USER-DEFINED TYPES
-- =============================================================================
CREATE TYPE Sales.OrderItemList AS TABLE (
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    DiscountPercent DECIMAL(5,2) DEFAULT 0
);
GO

CREATE TYPE Sales.EmailAddress FROM NVARCHAR(255) NOT NULL;
GO

-- =============================================================================
-- TABLES
-- =============================================================================
CREATE TABLE Sales.Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    Email NVARCHAR(255) NOT NULL UNIQUE,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Phone NVARCHAR(20),
    Street NVARCHAR(200),
    City NVARCHAR(100),
    State NVARCHAR(50),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(50) DEFAULT 'USA',
    CreatedAt DATETIME2 DEFAULT GETUTCDATE(),
    UpdatedAt DATETIME2 DEFAULT GETUTCDATE(),
    IsActive BIT DEFAULT 1
);

CREATE NONCLUSTERED INDEX IX_Customers_Name ON Sales.Customers (LastName, FirstName);
CREATE NONCLUSTERED INDEX IX_Customers_Email ON Sales.Customers (Email);
GO

EXEC sp_addextendedproperty 'MS_Description', 'Customer master table containing contact and shipping information', 'SCHEMA', 'Sales', 'TABLE', 'Customers';
GO

CREATE TABLE Inventory.Products (
    ProductID INT PRIMARY KEY DEFAULT NEXT VALUE FOR Inventory.ProductSeq,
    SKU NVARCHAR(50) NOT NULL UNIQUE,
    Name NVARCHAR(200) NOT NULL,
    Description NVARCHAR(MAX),
    UnitPrice DECIMAL(10,2) NOT NULL CONSTRAINT CHK_Products_UnitPrice CHECK (UnitPrice > 0),
    QuantityInStock INT NOT NULL DEFAULT 0 CONSTRAINT CHK_Products_Quantity CHECK (QuantityInStock >= 0),
    ReorderLevel INT DEFAULT 10,
    Category NVARCHAR(100),
    IsDiscontinued BIT DEFAULT 0,
    CreatedAt DATETIME2 DEFAULT GETUTCDATE()
);

CREATE NONCLUSTERED INDEX IX_Products_Category ON Inventory.Products (Category);
CREATE NONCLUSTERED INDEX IX_Products_SKU ON Inventory.Products (SKU);
GO

CREATE TABLE Sales.Orders (
    OrderID INT PRIMARY KEY DEFAULT NEXT VALUE FOR Sales.OrderSeq,
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Sales.Customers(CustomerID),
    OrderDate DATETIME2 DEFAULT GETUTCDATE(),
    Status NVARCHAR(20) DEFAULT 'pending' CONSTRAINT CHK_Orders_Status CHECK (Status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    TotalAmount DECIMAL(12,2) NOT NULL DEFAULT 0 CONSTRAINT CHK_Orders_TotalAmount CHECK (TotalAmount >= 0),
    Notes NVARCHAR(MAX)
);

CREATE NONCLUSTERED INDEX IX_Orders_Customer ON Sales.Orders (CustomerID);
CREATE NONCLUSTERED INDEX IX_Orders_Date ON Sales.Orders (OrderDate DESC);
CREATE NONCLUSTERED INDEX IX_Orders_Status ON Sales.Orders (Status);
GO

CREATE TABLE Sales.OrderItems (
    OrderItemID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL FOREIGN KEY REFERENCES Sales.Orders(OrderID) ON DELETE CASCADE,
    ProductID INT NOT NULL FOREIGN KEY REFERENCES Inventory.Products(ProductID),
    Quantity INT NOT NULL CONSTRAINT CHK_OrderItems_Quantity CHECK (Quantity > 0),
    UnitPrice DECIMAL(10,2) NOT NULL CONSTRAINT CHK_OrderItems_UnitPrice CHECK (UnitPrice > 0),
    DiscountPercent DECIMAL(5,2) DEFAULT 0 CONSTRAINT CHK_OrderItems_Discount CHECK (DiscountPercent >= 0 AND DiscountPercent <= 100),
    CONSTRAINT UQ_OrderItems_OrderProduct UNIQUE (OrderID, ProductID)
);
GO

-- =============================================================================
-- VIEWS
-- =============================================================================
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
GO

CREATE VIEW Inventory.LowStockProducts AS
SELECT
    ProductID,
    SKU,
    Name,
    QuantityInStock,
    ReorderLevel,
    ReorderLevel - QuantityInStock AS UnitsToOrder
FROM Inventory.Products
WHERE QuantityInStock < ReorderLevel
  AND IsDiscontinued = 0;
GO

-- =============================================================================
-- STORED PROCEDURES
-- =============================================================================
CREATE PROCEDURE Sales.CreateOrder
    @CustomerID INT,
    @Notes NVARCHAR(MAX) = NULL,
    @OrderID INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO Sales.Orders (CustomerID, Notes)
    VALUES (@CustomerID, @Notes);

    SET @OrderID = SCOPE_IDENTITY();
END;
GO

CREATE PROCEDURE Inventory.UpdateStock
    @ProductID INT,
    @QuantityChange INT
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM Inventory.Products WHERE ProductID = @ProductID)
    BEGIN
        RAISERROR('Product not found', 16, 1);
        RETURN;
    END

    UPDATE Inventory.Products
    SET QuantityInStock = QuantityInStock + @QuantityChange
    WHERE ProductID = @ProductID;
END;
GO

-- =============================================================================
-- FUNCTIONS
-- =============================================================================
CREATE FUNCTION Sales.CalculateOrderTotal(@OrderID INT)
RETURNS DECIMAL(12,2)
AS
BEGIN
    DECLARE @Total DECIMAL(12,2);

    SELECT @Total = ISNULL(SUM(Quantity * UnitPrice * (1 - DiscountPercent/100)), 0)
    FROM Sales.OrderItems
    WHERE OrderID = @OrderID;

    RETURN @Total;
END;
GO

CREATE FUNCTION Inventory.GetProductsByCategory(@Category NVARCHAR(100))
RETURNS TABLE
AS
RETURN
(
    SELECT ProductID, SKU, Name, UnitPrice, QuantityInStock
    FROM Inventory.Products
    WHERE Category = @Category
      AND IsDiscontinued = 0
);
GO

-- =============================================================================
-- TRIGGERS
-- =============================================================================
CREATE TRIGGER Sales.TR_Customers_UpdateTimestamp
ON Sales.Customers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Sales.Customers
    SET UpdatedAt = GETUTCDATE()
    FROM Sales.Customers c
    INNER JOIN inserted i ON c.CustomerID = i.CustomerID;
END;
GO

CREATE TRIGGER Sales.TR_OrderItems_UpdateTotal
ON Sales.OrderItems
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @AffectedOrders TABLE (OrderID INT);

    INSERT INTO @AffectedOrders (OrderID)
    SELECT DISTINCT OrderID FROM inserted
    UNION
    SELECT DISTINCT OrderID FROM deleted;

    UPDATE Sales.Orders
    SET TotalAmount = Sales.CalculateOrderTotal(o.OrderID)
    FROM Sales.Orders o
    INNER JOIN @AffectedOrders a ON o.OrderID = a.OrderID;
END;
GO

-- =============================================================================
-- SYNONYMS
-- =============================================================================
CREATE SYNONYM dbo.Prods FOR Inventory.Products;
CREATE SYNONYM dbo.Custs FOR Sales.Customers;
GO

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================
INSERT INTO Sales.Customers (Email, FirstName, LastName, Phone, City, State) VALUES
    ('john.doe@example.com', 'John', 'Doe', '555-0101', 'New York', 'NY'),
    ('jane.smith@example.com', 'Jane', 'Smith', '555-0102', 'Los Angeles', 'CA');

INSERT INTO Inventory.Products (SKU, Name, Description, UnitPrice, QuantityInStock, Category) VALUES
    ('WIDGET-001', 'Standard Widget', 'A basic widget for everyday use', 19.99, 100, 'Widgets'),
    ('WIDGET-002', 'Premium Widget', 'A high-quality premium widget', 49.99, 50, 'Widgets'),
    ('GADGET-001', 'Mini Gadget', 'Compact and portable gadget', 29.99, 75, 'Gadgets');
GO

PRINT 'TestDB initialization complete';
GO
