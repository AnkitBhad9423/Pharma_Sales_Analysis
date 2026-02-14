import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PharmaETL:
    def __init__(self, db_config):
        """Initialize database connection"""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def extract_csv(self, file_path):
        """Extract data from CSV file"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✅ Extracted {len(df)} records from {file_path}")
            return df
        except Exception as e:
            logger.error(f"❌ Extraction failed for {file_path}: {e}")
            raise
    
    def load_dimension(self, df, table_name):
        """Load data into dimension table"""
        try:
            # Convert DataFrame to list of tuples
            records = [tuple(x) for x in df.to_numpy()]
            
            # Create INSERT query with column names
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            query = f"INSERT INTO pharma.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            
            # Execute batch insert
            execute_batch(self.cursor, query, records, page_size=1000)
            self.conn.commit()
            
            logger.info(f"✅ Loaded {len(records)} records into {table_name}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Load failed for {table_name}: {e}")
            raise
    
    def incremental_load_facts(self, df, table_name='fact_sales'):
        """Incrementally load fact table (only new records)"""
        try:
            # Get max sale_id from database
            self.cursor.execute(f"SELECT COALESCE(MAX(sale_id), 0) FROM pharma.{table_name}")
            max_id = self.cursor.fetchone()[0]
            
            # Filter only new records
            new_records = df[df['sale_id'] > max_id]
            
            if len(new_records) == 0:
                logger.info("No new records to load")
                return
            
            # Load new records
            records = [tuple(x) for x in new_records.to_numpy()]
            columns = ', '.join(new_records.columns)
            placeholders = ', '.join(['%s'] * len(new_records.columns))
            query = f"INSERT INTO pharma.{table_name} ({columns}) VALUES ({placeholders})"
            
            execute_batch(self.cursor, query, records, page_size=1000)
            self.conn.commit()
            
            logger.info(f"✅ Incrementally loaded {len(records)} new records into {table_name}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Incremental load failed: {e}")
            raise
    
    def run_full_etl(self):
        """Execute complete ETL pipeline"""
        try:
            self.connect()
            
            # Load dimensions
            logger.info("Loading dimension tables...")
            dim_date = self.extract_csv('data/dim_date.csv')
            self.load_dimension(dim_date, 'dim_date')
            
            dim_sales_rep = self.extract_csv('data/dim_sales_rep.csv')
            self.load_dimension(dim_sales_rep, 'dim_sales_rep')
            
            dim_doctor = self.extract_csv('data/dim_doctor.csv')
            self.load_dimension(dim_doctor, 'dim_doctor')
            
            dim_product = self.extract_csv('data/dim_product.csv')
            self.load_dimension(dim_product, 'dim_product')
            
            dim_territory = self.extract_csv('data/dim_territory.csv')
            self.load_dimension(dim_territory, 'dim_territory')
            
            # Load facts (incremental)
            logger.info("Loading fact table...")
            fact_sales = self.extract_csv('data/fact_sales.csv')
            self.incremental_load_facts(fact_sales)
            
            logger.info("🎉 ETL pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.close()

# Database configuration
db_config = {
    'host': 'localhost',
    'database': 'pharma_analytics',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

# Run ETL
if __name__ == "__main__":
    etl = PharmaETL(db_config)
    etl.run_full_etl()