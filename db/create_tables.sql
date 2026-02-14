
SET search_path TO pharma, public;
-- dim_date
CREATE TABLE IF NOT EXISTS pharma.dim_date (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL,
    day INT,
    month INT,
    quarter INT,
    year INT,
    day_of_week INT,
    month_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- dim_sales_rep
CREATE TABLE IF NOT EXISTS pharma.dim_sales_rep (
    rep_key INT PRIMARY KEY,
    rep_name VARCHAR(100),
    region VARCHAR(50),
    team VARCHAR(50),
    hire_date DATE,
    experience_years INT,
    performance_tier VARCHAR(20)
);

-- dim_doctor
CREATE TABLE IF NOT EXISTS pharma.dim_doctor (
    doctor_key INT PRIMARY KEY,
    doctor_name VARCHAR(100),
    specialty VARCHAR(100),
    hospital VARCHAR(100),
    city VARCHAR(100),
    prescription_volume VARCHAR(20)
);

-- dim_product
CREATE TABLE IF NOT EXISTS pharma.dim_product (
    product_key INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(100),
    unit_price DECIMAL(10, 2),
    launch_date DATE,
    patent_status VARCHAR(50)
);

-- dim_territory
CREATE TABLE IF NOT EXISTS pharma.dim_territory (
    territory_key INT PRIMARY KEY,
    territory_name VARCHAR(100),
    region VARCHAR(50),
    state VARCHAR(5),
    population INT,
    market_potential VARCHAR(20)
);

-- fact_sales
CREATE TABLE IF NOT EXISTS pharma.fact_sales (
    sale_id INT PRIMARY KEY,
    date_key INT REFERENCES pharma.dim_date(date_key),
    rep_key INT REFERENCES pharma.dim_sales_rep(rep_key),
    doctor_key INT REFERENCES pharma.dim_doctor(doctor_key),
    product_key INT REFERENCES pharma.dim_product(product_key),
    territory_key INT REFERENCES pharma.dim_territory(territory_key),
    quantity_sold INT,
    revenue DECIMAL(12, 2),
    discount_percent DECIMAL(5, 2),
    marketing_spend DECIMAL(10, 2)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_date ON pharma.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_rep ON pharma.fact_sales(rep_key);
CREATE INDEX IF NOT EXISTS idx_fact_territory ON pharma.fact_sales(territory_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON pharma.fact_sales(product_key);