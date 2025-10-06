"""
Store predictions back to BigQuery
"""
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from typing import List, Dict

class BigQueryStore:
    def __init__(self):
        self.project_id = os.getenv("PROJECT_ID")
        self.warehouse_id = os.getenv("WAREHOUSE")
        credentials_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../gcp-credentials.json")
        )
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        
    def store_personalized_recommendations(
        self, 
        recommendations: List[Dict],
        model_id: int = 1
    ) -> None:
        """
        Store personalized recommendations to BigQuery
        
        Args:
            recommendations: List of dicts with keys: user_id, recommended_variant_id, 
                           recommendation_score, recommendation_type
            model_id: ID of the model that generated recommendations
        """
        if not recommendations:
            print("No personalized recommendations to store")
            return
        
        # Prepare DataFrame
        df = pd.DataFrame(recommendations)
        df['model_id'] = model_id
        df['created_at'] = datetime.utcnow()
        
        # Generate unique IDs
        df['recommendation_id'] = range(len(df))
        
        # Reorder columns to match BigQuery schema
        df = df[[
            'recommendation_id', 'user_id', 'recommended_variant_id', 
            'model_id', 'recommendation_score', 'recommendation_type', 'created_at'
        ]]
        
        # Upload to BigQuery
        table_id = f"{self.project_id}.{self.warehouse_id}.FactPersonalizedRecommendation"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("recommendation_id", "INT64"),
                bigquery.SchemaField("user_id", "INT64"),
                bigquery.SchemaField("recommended_variant_id", "INT64"),
                bigquery.SchemaField("model_id", "INT64"),
                bigquery.SchemaField("recommendation_score", "FLOAT64"),
                bigquery.SchemaField("recommendation_type", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ]
        )
        
        print(f"Uploading {len(df)} personalized recommendations to BigQuery...")
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✓ Successfully stored {len(df)} personalized recommendations")
    
    def store_product_recommendations(
        self, 
        recommendations: List[Dict],
        model_id: int = 1
    ) -> None:
        """
        Store product-to-product recommendations to BigQuery
        
        Args:
            recommendations: List of dicts with keys: source_shop_product_id, 
                           recommended_shop_product_id, recommendation_score, recommendation_type
            model_id: ID of the model that generated recommendations
        """
        if not recommendations:
            print("No product recommendations to store")
            return
        
        # Prepare DataFrame
        df = pd.DataFrame(recommendations)
        df['model_id'] = model_id
        df['created_at'] = datetime.utcnow()
        
        # Generate unique IDs
        df['recommendation_id'] = range(len(df))
        
        # Reorder columns
        df = df[[
            'recommendation_id', 'source_shop_product_id', 'recommended_shop_product_id',
            'model_id', 'recommendation_score', 'recommendation_type', 'created_at'
        ]]
        
        # Upload to BigQuery
        table_id = f"{self.project_id}.{self.warehouse_id}.FactProductRecommendation"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("recommendation_id", "INT64"),
                bigquery.SchemaField("source_shop_product_id", "INT64"),
                bigquery.SchemaField("recommended_shop_product_id", "INT64"),
                bigquery.SchemaField("model_id", "INT64"),
                bigquery.SchemaField("recommendation_score", "FLOAT64"),
                bigquery.SchemaField("recommendation_type", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ]
        )
        
        print(f"Uploading {len(df)} product recommendations to BigQuery...")
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✓ Successfully stored {len(df)} product recommendations")