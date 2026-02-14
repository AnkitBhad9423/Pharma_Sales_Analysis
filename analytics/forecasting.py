import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import psycopg2
import matplotlib.pyplot as plt
# import seaborn as sns

class PharmaDemandForecasting:
    def __init__(self, db_config):
        self.db_config = db_config
        self.model = None
        
    def extract_data_for_modeling(self):
        """Extract aggregated data for forecasting"""
        query = """
        SELECT 
            d.year,
            d.quarter,
            d.month,
            t.region,
            t.market_potential,
            p.category,
            sr.performance_tier,
            COUNT(DISTINCT fs.sale_id) as transaction_count,
            SUM(fs.revenue) as total_revenue,
            SUM(fs.quantity_sold) as total_quantity,
            AVG(fs.discount_percent) as avg_discount,
            SUM(fs.marketing_spend) as total_marketing,
            COUNT(DISTINCT fs.doctor_key) as unique_doctors
        FROM pharma.fact_sales fs
        JOIN pharma.dim_date d ON fs.date_key = d.date_key
        JOIN pharma.dim_territory t ON fs.territory_key = t.territory_key
        JOIN pharma.dim_product p ON fs.product_key = p.product_key
        JOIN pharma.dim_sales_rep sr ON fs.rep_key = sr.rep_key
        GROUP BY d.year, d.quarter, d.month, t.region, t.market_potential, p.category, sr.performance_tier
        """
        
        conn = psycopg2.connect(**self.db_config)
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    
    def prepare_features(self, df):
        """Engineer features for modeling"""
        # Encode categorical variables
        df_encoded = pd.get_dummies(df, columns=['region', 'market_potential', 'category', 'performance_tier'])
        
        # Create lag features
        df_encoded = df_encoded.sort_values(['year', 'quarter'])
        df_encoded['revenue_lag1'] = df_encoded.groupby(['region'])['total_revenue'].shift(1)
        df_encoded['revenue_lag2'] = df_encoded.groupby(['region'])['total_revenue'].shift(2)
        
        # Drop rows with NaN (from lag features)
        df_encoded = df_encoded.dropna()
        
        return df_encoded
    
    def train_model(self, df):
        """Train Random Forest forecasting model"""
        # Prepare features
        df_model = self.prepare_features(df)
        
        # Define target and features
        target = 'total_revenue'
        exclude_cols = ['total_revenue', 'total_quantity', 'transaction_count']
        features = [col for col in df_model.columns if col not in exclude_cols]
        
        X = df_model[features]
        y = df_model[target]
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train model
        self.model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
        self.model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test)
        
        metrics = {
            'MAE': mean_absolute_error(y_test, y_pred),
            'RMSE': np.sqrt(mean_squared_error(y_test, y_pred)),
            'R2': r2_score(y_test, y_pred)
        }
        
        print("📊 Model Performance Metrics:")
        for metric, value in metrics.items():
            print(f"  {metric}: {value:,.2f}")
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': features,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print("\n🔝 Top 10 Important Features:")
        print(feature_importance.head(10))
        
        return self.model, metrics, feature_importance
    
    def forecast_next_quarter(self, current_data):
        """Generate forecast for next quarter"""
        # This is a simplified example
        # In practice, you'd prepare the data properly with next quarter's features
        prediction = self.model.predict(current_data)
        return prediction

# Run forecasting
db_config = {
    'host': 'localhost',
    'database': 'pharma_analytics',
    'user': 'postgres',
    'password': 'admin'
}

forecaster = PharmaDemandForecasting(db_config)
df = forecaster.extract_data_for_modeling()
model, metrics, importance = forecaster.train_model(df)