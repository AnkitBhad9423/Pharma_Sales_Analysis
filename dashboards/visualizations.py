import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import sqlalchemy

class PharmaDashboard:
    def __init__(self, db_config):
        self.db_config = db_config
        
    def get_data(self, query):
        """Execute SQL query and return DataFrame"""
        conn = psycopg2.connect(**self.db_config)
        engine = sqlalchemy.create_engine(
        f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/{self.db_config['database']}"
    )

        df = pd.read_sql(query, engine)
        conn.close()
        return df
    
    def create_rep_effectiveness_dashboard(self):
        """Dashboard 1: Sales Rep Effectiveness"""
        query = """
        SELECT 
            rep_name,
            region,
            total_revenue,
            marketing_roi,
            unique_doctors_covered,
            performance_category
        FROM pharma.vw_rep_performance
        ORDER BY total_revenue DESC
        LIMIT 20
        """
        
        df = self.get_data(query)
        
        # Create subplot
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Top Reps by Revenue', 'Marketing ROI', 
                          'Doctor Coverage', 'Performance Distribution'),
            specs=[[{'type': 'bar'}, {'type': 'scatter'}],
                   [{'type': 'bar'}, {'type': 'pie'}]]
        )
        
        # Revenue bar chart
        fig.add_trace(
            go.Bar(x=df['rep_name'][:10], y=df['total_revenue'][:10], name='Revenue'),
            row=1, col=1
        )
        
        # ROI scatter
        fig.add_trace(
            go.Scatter(x=df['total_revenue'], y=df['marketing_roi'], 
                      mode='markers', name='ROI', text=df['rep_name']),
            row=1, col=2
        )
        
        # Doctor coverage
        fig.add_trace(
            go.Bar(x=df['rep_name'][:10], y=df['unique_doctors_covered'][:10], 
                  name='Doctors'),
            row=2, col=1
        )
        
        # Performance distribution
        perf_dist = df['performance_category'].value_counts()
        fig.add_trace(
            go.Pie(labels=perf_dist.index, values=perf_dist.values, name='Performance'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, title_text="Sales Rep Effectiveness Dashboard")
        fig.show()
        
        return fig
    
    def create_territory_heatmap(self):
        """Dashboard 2: Territory Performance Heatmap"""
        query = """
        SELECT 
            territory_name,
            region,
            total_revenue,
            market_potential,
            revenue_per_1k_population
        FROM (
            SELECT 
                t.territory_name,
                t.region,
                t.market_potential,
                t.population,
                SUM(fs.revenue) as total_revenue,
                SUM(fs.revenue) / NULLIF(t.population, 0) * 1000 as revenue_per_1k_population
            FROM pharma.fact_sales fs
            JOIN pharma.dim_territory t ON fs.territory_key = t.territory_key
            GROUP BY t.territory_name, t.region, t.market_potential, t.population
        ) sub
        """
        
        df = self.get_data(query)
        
        # Create heatmap
        pivot = df.pivot_table(values='total_revenue', 
                               index='territory_name', 
                               columns='region', 
                               aggfunc='sum').fillna(0)
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            colorscale='Viridis'
        ))
        
        fig.update_layout(
            title='Territory Revenue Heatmap by Region',
            xaxis_title='Region',
            yaxis_title='Territory',
            height=600
        )
        
        fig.show()
        return fig
    
    def create_quarterly_trends(self):
        """Dashboard 3: Quarterly Sales Trends"""
        query = """
        WITH quarterly_sales AS (
            SELECT 
                d.year,
                d.quarter,
                t.region,
                SUM(fs.revenue) as quarterly_revenue,
                SUM(fs.marketing_spend) as quarterly_marketing
            FROM pharma.fact_sales fs
            JOIN pharma.dim_date d ON fs.date_key = d.date_key
            JOIN pharma.dim_territory t ON fs.territory_key = t.territory_key
            GROUP BY d.year, d.quarter, t.region
        )
        SELECT 
            year || '-Q' || quarter as period,
            region,
            quarterly_revenue,
            quarterly_marketing,
            quarterly_revenue / NULLIF(quarterly_marketing, 0) as roi
        FROM quarterly_sales
        ORDER BY year, quarter
        """
        
        df = self.get_data(query)
        
        fig = go.Figure()
        
        for region in df['region'].unique():
            region_data = df[df['region'] == region]
            fig.add_trace(go.Scatter(
                x=region_data['period'],
                y=region_data['quarterly_revenue'],
                name=region,
                mode='lines+markers'
            ))
        
        fig.update_layout(
            title='Quarterly Revenue Trends by Region',
            xaxis_title='Quarter',
            yaxis_title='Revenue ($)',
            height=500
        )
        
        fig.show()
        return fig

# Create dashboards
db_config = {
    'host': 'localhost',
    'database': 'pharma_analytics',
    'user': 'postgres',
    'password': 'admin'
}

dashboard = PharmaDashboard(db_config)
dashboard.create_rep_effectiveness_dashboard()
dashboard.create_territory_heatmap()
dashboard.create_quarterly_trends()


class PharmaInsightsGenerator:
    def __init__(self, db_config):
        self.db_config = db_config
    
    def generate_insights_report(self):
        """Generate comprehensive business insights"""
        
        conn = psycopg2.connect(**self.db_config)
        
        insights = []
        
        # Insight 1: Underperforming high-potential territories
        query1 = """
        WITH territory_performance AS (
            SELECT 
                t.territory_name,
                t.region,
                t.market_potential,
                SUM(fs.revenue) as total_revenue,
                AVG(SUM(fs.revenue)) OVER (PARTITION BY t.market_potential) as avg_revenue_by_potential
            FROM pharma.fact_sales fs
            JOIN pharma.dim_territory t ON fs.territory_key = t.territory_key
            GROUP BY t.territory_name, t.region, t.market_potential
        )
        SELECT territory_name, region, total_revenue, avg_revenue_by_potential
        FROM territory_performance
        WHERE market_potential = 'High' AND total_revenue < avg_revenue_by_potential * 0.7
        """
        
        underperforming = pd.read_sql(query1, conn)
        
        if len(underperforming) > 0:
            insights.append({
                'category': 'Territory Optimization',
                'finding': f'{len(underperforming)} high-potential territories are underperforming',
                'recommendation': f'Reallocate top-performing reps to: {", ".join(underperforming["territory_name"].head(3).tolist())}',
                'expected_impact': 'Potential 15-25% revenue increase in these territories'
            })
        
        # Insight 2: Marketing spend efficiency
        query2 = """
        SELECT 
            region,
            SUM(revenue) as total_revenue,
            SUM(marketing_spend) as total_marketing,
            SUM(revenue) / NULLIF(SUM(marketing_spend), 0) as roi
        FROM pharma.fact_sales fs
        JOIN pharma.dim_territory t ON fs.territory_key = t.territory_key
        GROUP BY region
        ORDER BY roi DESC
        """
        
        marketing_efficiency = pd.read_sql(query2, conn)
        
        best_roi_region = marketing_efficiency.iloc[0]['region']
        worst_roi_region = marketing_efficiency.iloc[-1]['region']
        
        insights.append({
            'category': 'Marketing ROI',
            'finding': f'{best_roi_region} has highest ROI, {worst_roi_region} has lowest',
            'recommendation': f'Reduce marketing spend in {worst_roi_region} by 20%, reinvest in {best_roi_region}',
            'expected_impact': 'Projected 10% improvement in overall marketing efficiency'
        })
        
        # Insight 3: Top product opportunities
        query3 = """
        SELECT 
            p.product_name,
            p.category,
            COUNT(DISTINCT fs.doctor_key) as prescriber_count,
            SUM(fs.revenue) as total_revenue
        FROM pharma.fact_sales fs
        JOIN pharma.dim_product p ON fs.product_key = p.product_key
        GROUP BY p.product_name, p.category
        HAVING COUNT(DISTINCT fs.doctor_key) < 50
        ORDER BY total_revenue DESC
        LIMIT 5
        """
        
        growth_products = pd.read_sql(query3, conn)
        
        insights.append({
            'category': 'Product Growth',
            'finding': f'Top revenue products have low prescriber penetration',
            'recommendation': f'Expand prescriber base for: {", ".join(growth_products["product_name"].tolist())}',
            'expected_impact': 'Double prescriber count could add $2-5M in revenue'
        })
        
        conn.close()
        
        # Print report
        print("=" * 80)
        print("PHARMACEUTICAL SALES ANALYTICS - INSIGHTS REPORT")
        print("=" * 80)
        
        for i, insight in enumerate(insights, 1):
            print(f"\n📊 INSIGHT #{i}: {insight['category']}")
            print(f"   Finding: {insight['finding']}")
            print(f"   ✅ Recommendation: {insight['recommendation']}")
            print(f"   💰 Expected Impact: {insight['expected_impact']}")
        
        print("\n" + "=" * 80)
        
        return insights

# Generate insights
generator = PharmaInsightsGenerator(db_config)
insights = generator.generate_insights_report()