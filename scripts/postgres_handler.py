"""
PostgreSQL Handler
Handles operations with PostgreSQL database
"""

import os
import psycopg2
import pandas as pd
from typing import Optional, List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresHandler:
    """Handle PostgreSQL operations"""
    
    def __init__(self):
        """Initialize PostgreSQL connection"""
        self.host = os.getenv('POSTGRES_HOST', 'postgres')
        self.port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = os.getenv('POSTGRES_DATABASE', 'airflow')
        self.user = os.getenv('POSTGRES_USER', 'airflow')
        self.password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        self.conn = None
        logger.info("PostgresHandler initialized")
    
    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to PostgreSQL: {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            raise
    
    def disconnect(self):
        """Close PostgreSQL connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")
    
    def execute_query(self, query: str) -> List[tuple]:
        """
        Execute a SQL query
        
        Args:
            query: SQL query string
        
        Returns:
            Query results as list of tuples
        """
        if not self.conn:
            self.connect()
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            logger.info(f"Query executed successfully. Returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def execute_query_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute query and return results as DataFrame
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame with query results
        """
        if not self.conn:
            self.connect()
        
        try:
            df = pd.read_sql_query(query, self.conn)
            logger.info(f"Query executed successfully. Returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def execute_update(self, query: str) -> int:
        """
        Execute an UPDATE/INSERT/DELETE query
        
        Args:
            query: SQL query string
        
        Returns:
            Number of affected rows
        """
        if not self.conn:
            self.connect()
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            affected_rows = cursor.rowcount
            self.conn.commit()
            cursor.close()
            logger.info(f"Update executed successfully. {affected_rows} rows affected")
            return affected_rows
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error executing update: {str(e)}")
            raise
    
    def create_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        if_not_exists: bool = True
    ) -> bool:
        """
        Create a table in PostgreSQL
        
        Args:
            table_name: Name of the table
            columns: Dictionary mapping column names to data types
            if_not_exists: Whether to use IF NOT EXISTS
        
        Returns:
            True if successful
        """
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        column_defs = [f"{name} {dtype}" for name, dtype in columns.items()]
        create_query = f"CREATE TABLE {if_not_exists_clause} {table_name} ({', '.join(column_defs)})"
        
        try:
            self.execute_update(create_query)
            logger.info(f"Created table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise
    
    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append'
    ) -> bool:
        """
        Insert DataFrame into PostgreSQL table
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            if_exists: What to do if table exists ('append', 'replace', 'fail')
        
        Returns:
            True if successful
        """
        if not self.conn:
            self.connect()
        
        try:
            df.to_sql(
                table_name,
                self.conn,
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            logger.info(f"Inserted {len(df)} rows into {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error inserting DataFrame: {str(e)}")
            raise
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

