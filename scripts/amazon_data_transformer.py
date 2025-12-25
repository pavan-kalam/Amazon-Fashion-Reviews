"""
Amazon Product Data Transformer
Cleans and transforms Amazon product data from JSONL files
"""

import pandas as pd
import numpy as np
import re
import json
from datetime import datetime
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AmazonDataTransformer:
    """Transform and clean Amazon product data"""
    
    def __init__(self):
        """Initialize transformer"""
        logger.info("AmazonDataTransformer initialized")
    
    def clean_text(self, text: str) -> str:
        """
        Clean text data by removing special characters and normalizing
        
        Args:
            text: Raw text string
        
        Returns:
            Cleaned text string
        """
        if pd.isna(text) or text is None:
            return ""
        
        # Convert to string if not already
        text = str(text)
        
        # Remove URLs
        text = re.sub(r'http\S+|www.\S+', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def extract_list_fields(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Extract list fields into separate columns or string
        
        Args:
            df: DataFrame
            column: Column name containing lists
        
        Returns:
            DataFrame with extracted fields
        """
        df = df.copy()
        
        if column in df.columns:
            # Convert lists to strings
            df[f'{column}_count'] = df[column].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
            df[f'{column}_text'] = df[column].apply(
                lambda x: ' | '.join(str(item) for item in x) if isinstance(x, list) and x else ''
            )
        
        return df
    
    def extract_image_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract image information from images column
        
        Args:
            df: DataFrame with images column
        
        Returns:
            DataFrame with extracted image fields
        """
        df = df.copy()
        
        if 'images' in df.columns:
            # Count images
            df['image_count'] = df['images'].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
            
            # Extract main image URL
            df['main_image_url'] = df['images'].apply(
                lambda x: x[0]['large'] if isinstance(x, list) and len(x) > 0 and 'large' in x[0] else None
            )
            
            # Extract all image URLs
            df['all_image_urls'] = df['images'].apply(
                lambda x: ' | '.join([img.get('large', '') for img in x if isinstance(img, dict) and 'large' in img]) 
                if isinstance(x, list) else ''
            )
        
        return df
    
    def parse_price(self, price_str: Optional[str]) -> Optional[float]:
        """
        Parse price string to float
        
        Args:
            price_str: Price string (e.g., "$19.99" or "$19.99 - $29.99")
        
        Returns:
            Float price or None
        """
        if pd.isna(price_str) or price_str is None:
            return None
        
        # Extract first price from string
        price_match = re.search(r'\$?(\d+\.?\d*)', str(price_str))
        if price_match:
            try:
                return float(price_match.group(1))
            except ValueError:
                return None
        return None
    
    def calculate_rating_score(self, row: pd.Series) -> float:
        """
        Calculate weighted rating score
        
        Args:
            row: DataFrame row with average_rating and rating_number
        
        Returns:
            Weighted score
        """
        rating = row.get('average_rating', 0) or 0
        num_ratings = row.get('rating_number', 0) or 0
        
        # Weighted score: rating * log(1 + num_ratings)
        import math
        if num_ratings > 0:
            score = rating * math.log(1 + num_ratings)
            return round(score, 2)
        return 0.0
    
    def transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Amazon products DataFrame
        
        Args:
            df: Raw products DataFrame
        
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(df)} products")
        
        df = df.copy()
        
        # Clean text fields
        if 'title' in df.columns:
            df['title_cleaned'] = df['title'].apply(self.clean_text)
        
        if 'description' in df.columns:
            df = self.extract_list_fields(df, 'description')
            df['description_text'] = df['description_text'].apply(self.clean_text)
        
        if 'features' in df.columns:
            df = self.extract_list_fields(df, 'features')
            df['features_text'] = df['features_text'].apply(self.clean_text)
        
        # Extract image information
        df = self.extract_image_info(df)
        
        # Parse price
        if 'price' in df.columns:
            df['price_parsed'] = df['price'].apply(self.parse_price)
            df['has_price'] = df['price_parsed'].notna()
        
        # Calculate derived metrics
        df['rating_score'] = df.apply(self.calculate_rating_score, axis=1)
        
        # Handle missing values
        if 'average_rating' in df.columns:
            df['average_rating'] = pd.to_numeric(df['average_rating'], errors='coerce')
            df['average_rating'] = df['average_rating'].fillna(0)
        
        if 'rating_number' in df.columns:
            df['rating_number'] = pd.to_numeric(df['rating_number'], errors='coerce')
            df['rating_number'] = df['rating_number'].fillna(0)
        
        # Add extraction timestamp
        df['extraction_timestamp'] = datetime.now().isoformat()
        df['extraction_datetime'] = pd.to_datetime(df['extraction_timestamp'])
        
        # Calculate text lengths
        if 'title' in df.columns:
            df['title_length'] = df['title'].str.len()
        
        if 'description_text' in df.columns:
            df['description_length'] = df['description_text'].str.len()
        
        logger.info(f"Transformation complete. {len(df)} products processed")
        logger.info(f"New columns: {[col for col in df.columns if col not in ['title', 'description', 'features', 'images', 'price']]}")
        
        return df
    
    def aggregate_by_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate product metrics by category
        
        Args:
            df: Transformed products DataFrame
        
        Returns:
            Aggregated DataFrame
        """
        if 'main_category' not in df.columns:
            logger.warning("main_category column not found")
            return pd.DataFrame()
        
        agg_df = df.groupby('main_category').agg({
            'title': 'count',
            'average_rating': ['mean', 'count'],
            'rating_number': ['mean', 'sum'],
            'price_parsed': ['mean', 'min', 'max'],
            'image_count': 'mean',
            'rating_score': 'mean'
        }).reset_index()
        
        agg_df.columns = [
            'category', 'product_count', 'avg_rating', 'rated_products',
            'avg_ratings_per_product', 'total_ratings', 'avg_price',
            'min_price', 'max_price', 'avg_images', 'avg_rating_score'
        ]
        
        return agg_df
    
    def aggregate_by_price_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate products by price range
        
        Args:
            df: Transformed products DataFrame
        
        Returns:
            Aggregated DataFrame
        """
        if 'price_parsed' not in df.columns:
            logger.warning("price_parsed column not found")
            return pd.DataFrame()
        
        # Create price ranges
        df_with_price = df[df['price_parsed'].notna()].copy()
        df_with_price['price_range'] = pd.cut(
            df_with_price['price_parsed'],
            bins=[0, 10, 25, 50, 100, 200, float('inf')],
            labels=['$0-10', '$10-25', '$25-50', '$50-100', '$100-200', '$200+']
        )
        
        agg_df = df_with_price.groupby('price_range').agg({
            'title': 'count',
            'average_rating': 'mean',
            'rating_number': 'mean'
        }).reset_index()
        
        agg_df.columns = ['price_range', 'product_count', 'avg_rating', 'avg_ratings']
        
        return agg_df


if __name__ == "__main__":
    # Example usage
    import os
    from jsonl_processor import JSONLProcessor
    
    processor = JSONLProcessor()
    transformer = AmazonDataTransformer()
    
    # Process sample data
    filepath = '../meta_Amazon_Fashion.jsonl'
    if os.path.exists(filepath):
        df = processor.jsonl_to_dataframe(filepath, limit=100)
        transformed_df = transformer.transform_products(df)
        print(transformed_df.head())
        print(f"\nShape: {transformed_df.shape}")
        print(f"Columns: {list(transformed_df.columns)}")

