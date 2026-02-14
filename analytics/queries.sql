SET search_path TO pharma, public;

-- ============================================
-- Query 1: Sales Rep Performance Analysis
-- ============================================

CREATE OR REPLACE VIEW pharma.vw_rep_performance AS
WITH rep_metrics AS (
    SELECT 
        sr.rep_key,
        sr.rep_name,
        sr.region,
        sr.team,
        sr.performance_tier,
        sr.experience_years,
        COUNT(DISTINCT fs.sale_id) as total_transactions,
        COUNT(DISTINCT fs.doctor_key) as unique_doctors_covered,
        SUM(fs.revenue) as total_revenue,
        AVG(fs.revenue) as avg_transaction_value,
        SUM(fs.quantity_sold) as total_units_sold,
        SUM(fs.marketing_spend) as total_marketing_spend,
        SUM(fs.revenue) / NULLIF(SUM(fs.marketing_spend), 0) as marketing_roi
    FROM pharma.fact_sales fs
    JOIN pharma.dim_sales_rep sr 
        ON fs.rep_key = sr.rep_key
    GROUP BY sr.rep_key, sr.rep_name, sr.region, 
             sr.team, sr.performance_tier, sr.experience_years
),
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_revenue) AS p75,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_revenue) AS p25
    FROM rep_metrics
)
SELECT 
    rm.*,
    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY marketing_roi DESC) as roi_rank,
    CASE 
        WHEN rm.total_revenue >= p.p75 THEN 'Top Performer'
        WHEN rm.total_revenue >= p.p25 THEN 'Average Performer'
        ELSE 'Underperformer'
    END as performance_category
FROM rep_metrics rm
CROSS JOIN percentiles p;


-- ============================================
-- Query 2: Territory Analysis
-- ============================================

CREATE OR REPLACE VIEW pharma.vw_territory_analysis AS
WITH territory_sales AS (
    SELECT 
        t.territory_key,
        t.territory_name,
        t.region,
        t.state,
        t.population,
        t.market_potential,
        COUNT(DISTINCT fs.doctor_key) as doctors_covered,
        SUM(fs.revenue) as total_revenue,
        SUM(fs.quantity_sold) as total_units,
        AVG(fs.revenue) as avg_sale_value,
        COUNT(DISTINCT d.year || '-' || d.quarter) as quarters_active
    FROM pharma.fact_sales fs
    JOIN pharma.dim_territory t 
        ON fs.territory_key = t.territory_key
    JOIN pharma.dim_date d 
        ON fs.date_key = d.date_key
    GROUP BY t.territory_key, t.territory_name, t.region, 
             t.state, t.population, t.market_potential
)
SELECT 
    *,
    total_revenue / NULLIF(population, 0) * 1000 as revenue_per_1k_population,
    CASE 
        WHEN market_potential = 'High' 
             AND total_revenue < AVG(total_revenue) OVER () 
             THEN 'Underutilized'
        WHEN market_potential = 'Low' 
             AND total_revenue > AVG(total_revenue) OVER () 
             THEN 'Overperforming'
        ELSE 'Expected'
    END as market_alignment
FROM territory_sales;


-- ============================================
-- Query 3: Doctor Prescription Patterns
-- ============================================

CREATE OR REPLACE VIEW pharma.vw_doctor_insights AS
SELECT 
    d.doctor_key,
    d.doctor_name,
    d.specialty,
    d.prescription_volume,
    COUNT(DISTINCT fs.sale_id) as prescription_count,
    COUNT(DISTINCT fs.product_key) as unique_products_prescribed,
    SUM(fs.revenue) as total_prescription_value,
    AVG(fs.quantity_sold) as avg_quantity_per_prescription,
    COUNT(DISTINCT fs.rep_key) as reps_interacted_with,
    MAX(dd.date) as last_prescription_date,
    CURRENT_DATE - MAX(dd.date) as days_since_last_prescription
FROM pharma.dim_doctor d
LEFT JOIN pharma.fact_sales fs 
    ON d.doctor_key = fs.doctor_key
LEFT JOIN pharma.dim_date dd 
    ON fs.date_key = dd.date_key
GROUP BY d.doctor_key, d.doctor_name, 
         d.specialty, d.prescription_volume;


-- ============================================
-- Query 4: Product Performance by Category
-- ============================================

CREATE OR REPLACE VIEW pharma.vw_product_performance AS
WITH product_performance AS (
    SELECT 
        p.category,
        p.product_name,
        p.unit_price,
        p.patent_status,
        SUM(fs.revenue) as total_revenue,
        SUM(fs.quantity_sold) as total_units_sold,
        AVG(fs.discount_percent) as avg_discount,
        COUNT(DISTINCT fs.doctor_key) as unique_prescribers,
        SUM(fs.revenue) / 
            SUM(SUM(fs.revenue)) OVER (PARTITION BY p.category) 
            as category_revenue_share
    FROM pharma.fact_sales fs
    JOIN pharma.dim_product p 
        ON fs.product_key = p.product_key
    GROUP BY p.category, p.product_name, 
             p.unit_price, p.patent_status
)
SELECT 
    *,
    RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) 
        as rank_in_category
FROM product_performance;


-- ============================================
-- Query 5: Time-Series Trend Analysis
-- ============================================

CREATE OR REPLACE VIEW pharma.vw_quarterly_trends AS
WITH quarterly_sales AS (
    SELECT 
        d.year,
        d.quarter,
        t.region,
        SUM(fs.revenue) as quarterly_revenue,
        SUM(fs.quantity_sold) as quarterly_units,
        COUNT(DISTINCT fs.rep_key) as active_reps,
        SUM(fs.marketing_spend) as quarterly_marketing
    FROM pharma.fact_sales fs
    JOIN pharma.dim_date d 
        ON fs.date_key = d.date_key
    JOIN pharma.dim_territory t 
        ON fs.territory_key = t.territory_key
    GROUP BY d.year, d.quarter, t.region
)
SELECT 
    *,
    LAG(quarterly_revenue) 
        OVER (PARTITION BY region ORDER BY year, quarter) 
        as prev_quarter_revenue,
    (quarterly_revenue - 
        LAG(quarterly_revenue) 
        OVER (PARTITION BY region ORDER BY year, quarter))
    / NULLIF(
        LAG(quarterly_revenue) 
        OVER (PARTITION BY region ORDER BY year, quarter), 0
      ) * 100 
        as qoq_growth_rate,
    quarterly_revenue / NULLIF(quarterly_marketing, 0) 
        as marketing_efficiency
FROM quarterly_sales;
