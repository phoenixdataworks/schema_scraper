-- SQLite Test Schema for Schema Scraper
-- Contains tables, views, triggers, and indexes

-- =============================================================================
-- TABLES
-- =============================================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    phone TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT DEFAULT 'USA',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    is_active INTEGER DEFAULT 1 CHECK (is_active IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_customers_name ON customers (last_name, first_name);
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers (email);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    unit_price REAL NOT NULL CHECK (unit_price > 0),
    quantity_in_stock INTEGER NOT NULL DEFAULT 0 CHECK (quantity_in_stock >= 0),
    reorder_level INTEGER DEFAULT 10,
    category TEXT,
    is_discontinued INTEGER DEFAULT 0 CHECK (is_discontinued IN (0, 1)),
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);
CREATE INDEX IF NOT EXISTS idx_products_sku ON products (sku);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date TEXT DEFAULT (datetime('now')),
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    total_amount REAL NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders (order_date DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price REAL NOT NULL CHECK (unit_price > 0),
    discount_percent REAL DEFAULT 0 CHECK (discount_percent >= 0 AND discount_percent <= 100),
    UNIQUE (order_id, product_id)
);

-- =============================================================================
-- VIEWS
-- =============================================================================
CREATE VIEW IF NOT EXISTS customer_order_summary AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_value,
    MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

CREATE VIEW IF NOT EXISTS low_stock_products AS
SELECT
    product_id,
    sku,
    name,
    quantity_in_stock,
    reorder_level,
    reorder_level - quantity_in_stock AS units_to_order
FROM products
WHERE quantity_in_stock < reorder_level
  AND is_discontinued = 0;

-- =============================================================================
-- TRIGGERS
-- =============================================================================
CREATE TRIGGER IF NOT EXISTS trg_customers_update_timestamp
AFTER UPDATE ON customers
FOR EACH ROW
BEGIN
    UPDATE customers SET updated_at = datetime('now') WHERE customer_id = NEW.customer_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_order_items_insert_update_total
AFTER INSERT ON order_items
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = (
        SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
        FROM order_items
        WHERE order_id = NEW.order_id
    )
    WHERE order_id = NEW.order_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_order_items_update_update_total
AFTER UPDATE ON order_items
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = (
        SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
        FROM order_items
        WHERE order_id = NEW.order_id
    )
    WHERE order_id = NEW.order_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_order_items_delete_update_total
AFTER DELETE ON order_items
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = (
        SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
        FROM order_items
        WHERE order_id = OLD.order_id
    )
    WHERE order_id = OLD.order_id;
END;

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================
INSERT OR IGNORE INTO customers (email, first_name, last_name, phone, city, state) VALUES
    ('john.doe@example.com', 'John', 'Doe', '555-0101', 'New York', 'NY'),
    ('jane.smith@example.com', 'Jane', 'Smith', '555-0102', 'Los Angeles', 'CA');

INSERT OR IGNORE INTO products (sku, name, description, unit_price, quantity_in_stock, category) VALUES
    ('WIDGET-001', 'Standard Widget', 'A basic widget for everyday use', 19.99, 100, 'Widgets'),
    ('WIDGET-002', 'Premium Widget', 'A high-quality premium widget', 49.99, 50, 'Widgets'),
    ('GADGET-001', 'Mini Gadget', 'Compact and portable gadget', 29.99, 75, 'Gadgets');
