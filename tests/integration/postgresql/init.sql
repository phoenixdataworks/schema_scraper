-- PostgreSQL Test Schema for Schema Scraper
-- Contains 1-2 objects of each supported type

-- =============================================================================
-- SCHEMAS
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS inventory;

-- =============================================================================
-- SEQUENCES
-- =============================================================================
CREATE SEQUENCE sales.order_seq
    START WITH 1000
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9999999
    CACHE 10;

CREATE SEQUENCE inventory.product_seq
    START WITH 1
    INCREMENT BY 1
    NO CYCLE;

-- =============================================================================
-- USER-DEFINED TYPES
-- =============================================================================
-- Enum type
CREATE TYPE sales.order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');

-- Composite type
CREATE TYPE sales.address_type AS (
    street VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50)
);

-- Domain type
CREATE DOMAIN sales.email_address AS VARCHAR(255)
    CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

-- =============================================================================
-- TABLES
-- =============================================================================
CREATE TABLE sales.customers (
    customer_id SERIAL PRIMARY KEY,
    email sales.email_address NOT NULL UNIQUE,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    phone VARCHAR(20),
    shipping_address sales.address_type,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_customers_name ON sales.customers (last_name, first_name);
CREATE INDEX idx_customers_email ON sales.customers (email);

COMMENT ON TABLE sales.customers IS 'Customer master table containing contact and shipping information';
COMMENT ON COLUMN sales.customers.customer_id IS 'Unique customer identifier';
COMMENT ON COLUMN sales.customers.email IS 'Customer email address (unique)';

CREATE TABLE sales.orders (
    order_id INTEGER PRIMARY KEY DEFAULT nextval('sales.order_seq'),
    customer_id INTEGER NOT NULL REFERENCES sales.customers(customer_id) ON DELETE RESTRICT,
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status sales.order_status DEFAULT 'pending',
    total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    notes TEXT,
    CONSTRAINT chk_order_date CHECK (order_date <= CURRENT_TIMESTAMP + INTERVAL '1 day')
);

CREATE INDEX idx_orders_customer ON sales.orders (customer_id);
CREATE INDEX idx_orders_date ON sales.orders (order_date DESC);
CREATE INDEX idx_orders_status ON sales.orders (status) WHERE status NOT IN ('delivered', 'cancelled');

COMMENT ON TABLE sales.orders IS 'Customer orders with status tracking';

CREATE TABLE inventory.products (
    product_id INTEGER PRIMARY KEY DEFAULT nextval('inventory.product_seq'),
    sku VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    quantity_in_stock INTEGER NOT NULL DEFAULT 0 CHECK (quantity_in_stock >= 0),
    reorder_level INTEGER DEFAULT 10,
    category VARCHAR(100),
    is_discontinued BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_products_category ON inventory.products (category);
CREATE INDEX idx_products_sku ON inventory.products (sku);

CREATE TABLE sales.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES sales.orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES inventory.products(product_id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    discount_percent DECIMAL(5,2) DEFAULT 0 CHECK (discount_percent >= 0 AND discount_percent <= 100),
    UNIQUE (order_id, product_id)
);

-- =============================================================================
-- VIEWS
-- =============================================================================
CREATE VIEW sales.customer_order_summary AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_value,
    MAX(o.order_date) AS last_order_date
FROM sales.customers c
LEFT JOIN sales.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

COMMENT ON VIEW sales.customer_order_summary IS 'Aggregated customer statistics including order counts and lifetime value';

CREATE VIEW inventory.low_stock_products AS
SELECT
    product_id,
    sku,
    name,
    quantity_in_stock,
    reorder_level,
    reorder_level - quantity_in_stock AS units_to_order
FROM inventory.products
WHERE quantity_in_stock < reorder_level
  AND is_discontinued = FALSE;

-- Materialized view
CREATE MATERIALIZED VIEW sales.monthly_sales_summary AS
SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    COUNT(*) AS order_count,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value
FROM sales.orders o
WHERE o.status NOT IN ('cancelled')
GROUP BY DATE_TRUNC('month', o.order_date)
WITH DATA;

CREATE UNIQUE INDEX idx_monthly_sales_month ON sales.monthly_sales_summary (month);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================
-- Scalar function
CREATE OR REPLACE FUNCTION sales.calculate_order_total(p_order_id INTEGER)
RETURNS DECIMAL(12,2)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total DECIMAL(12,2);
BEGIN
    SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
    INTO v_total
    FROM sales.order_items
    WHERE order_id = p_order_id;

    RETURN v_total;
END;
$$;

COMMENT ON FUNCTION sales.calculate_order_total IS 'Calculates the total amount for an order including discounts';

-- Table-valued function
CREATE OR REPLACE FUNCTION inventory.get_products_by_category(p_category VARCHAR)
RETURNS TABLE (
    product_id INTEGER,
    sku VARCHAR(50),
    name VARCHAR(200),
    unit_price DECIMAL(10,2),
    quantity_in_stock INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT p.product_id, p.sku, p.name, p.unit_price, p.quantity_in_stock
    FROM inventory.products p
    WHERE p.category = p_category
      AND p.is_discontinued = FALSE
    ORDER BY p.name;
END;
$$;

-- =============================================================================
-- STORED PROCEDURES
-- =============================================================================
CREATE OR REPLACE PROCEDURE sales.create_order(
    IN p_customer_id INTEGER,
    IN p_notes TEXT DEFAULT NULL,
    INOUT p_order_id INTEGER DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO sales.orders (customer_id, notes, total_amount)
    VALUES (p_customer_id, p_notes, 0)
    RETURNING order_id INTO p_order_id;
END;
$$;

CREATE OR REPLACE PROCEDURE inventory.update_stock(
    IN p_product_id INTEGER,
    IN p_quantity_change INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE inventory.products
    SET quantity_in_stock = quantity_in_stock + p_quantity_change
    WHERE product_id = p_product_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Product % not found', p_product_id;
    END IF;
END;
$$;

-- =============================================================================
-- TRIGGERS
-- =============================================================================
-- Trigger function for updating timestamps
CREATE OR REPLACE FUNCTION sales.update_timestamp()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_customers_update_timestamp
    BEFORE UPDATE ON sales.customers
    FOR EACH ROW
    EXECUTE FUNCTION sales.update_timestamp();

-- Trigger function for order total recalculation
CREATE OR REPLACE FUNCTION sales.recalculate_order_total()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE sales.orders
    SET total_amount = sales.calculate_order_total(
        CASE WHEN TG_OP = 'DELETE' THEN OLD.order_id ELSE NEW.order_id END
    )
    WHERE order_id = CASE WHEN TG_OP = 'DELETE' THEN OLD.order_id ELSE NEW.order_id END;

    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_order_items_update_total
    AFTER INSERT OR UPDATE OR DELETE ON sales.order_items
    FOR EACH ROW
    EXECUTE FUNCTION sales.recalculate_order_total();

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================
INSERT INTO sales.customers (email, first_name, last_name, phone) VALUES
    ('john.doe@example.com', 'John', 'Doe', '555-0101'),
    ('jane.smith@example.com', 'Jane', 'Smith', '555-0102');

INSERT INTO inventory.products (sku, name, description, unit_price, quantity_in_stock, category) VALUES
    ('WIDGET-001', 'Standard Widget', 'A basic widget for everyday use', 19.99, 100, 'Widgets'),
    ('WIDGET-002', 'Premium Widget', 'A high-quality premium widget', 49.99, 50, 'Widgets'),
    ('GADGET-001', 'Mini Gadget', 'Compact and portable gadget', 29.99, 75, 'Gadgets');

-- Refresh the materialized view
REFRESH MATERIALIZED VIEW sales.monthly_sales_summary;
