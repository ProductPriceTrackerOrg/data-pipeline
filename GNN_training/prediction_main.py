"""
Main script for daily predictions (Daily job)
"""
import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from big_query.load_data import load_data_from_bigquery, load_data_from_supabase, transform_data
from big_query.data_store import BigQueryStore
from predictions.predictor import ModelPredictor
from predictions.recommendation_engine import RecommendationEngine

def main():
    """Main prediction pipeline"""
    
    print("="*60)
    print("      GNN PREDICTION PIPELINE - DAILY JOB")
    print("="*60)
    
    # Load environment variables
    load_dotenv()
    
    # Step 1: Load trained model
    print("\nStep 1: Loading trained model...")
    predictor = ModelPredictor(models_dir="GNN_training/models", model_name="personalized_gnn")
    model, mappings = predictor.load_model(use_latest=True)
    
    # Step 2: Load fresh daily data
    print("\nStep 2: Loading fresh data from BigQuery and Supabase...")
    bigquery_df = load_data_from_bigquery()
    profiles_df, activity_df = load_data_from_supabase()
    
    # Step 3: Transform data
    print("\nStep 3: Transforming data...")
    final_dataframes = transform_data(bigquery_df, profiles_df, activity_df)
    
    print(f"\nDaily data loaded:")
    print(f"  Products: {len(final_dataframes['products'])}")
    print(f"  Users: {len(final_dataframes['users'])}")
    print(f"  User-Product interactions: {len(final_dataframes['user_product_edges'])}")
    
    # Step 4: Prepare daily data
    print("\nStep 4: Preparing daily data for inference...")
    daily_data = predictor.prepare_daily_data(final_dataframes)
    
    # Step 5: Generate embeddings
    print("\nStep 5: Generating embeddings...")
    embeddings = predictor.generate_embeddings(daily_data)
    
    # Step 6: Initialize recommendation engine
    print("\nStep 6: Generating recommendations...")
    rec_engine = RecommendationEngine(
        embeddings=embeddings,
        mappings=mappings,
        product_df=final_dataframes['products'],
        user_df=final_dataframes['users']
    )
    
    # Step 7: Generate all recommendations
    print("\nStep 7a: Generating personalized recommendations for all users...")
    personalized_recommendations = rec_engine.get_personalized_recommendations_for_all_users(top_k=10)
    
    print("\nStep 7b: Generating similar products for all products...")
    product_recommendations = rec_engine.get_similar_products_for_all_products(top_k=5)
    
    # Step 8: Store recommendations in BigQuery
    print("\nStep 8: Storing recommendations in BigQuery...")
    bq_store = BigQueryStore()
    
    bq_store.store_personalized_recommendations(
        recommendations=personalized_recommendations,
        model_id=1
    )
    
    bq_store.store_product_recommendations(
        recommendations=product_recommendations,
        model_id=1
    )
    
    print("\n" + "="*60)
    print("      PREDICTION PIPELINE COMPLETED SUCCESSFULLY")
    print("="*60)
    print(f"\nGenerated:")
    print(f"  Personalized recommendations: {len(personalized_recommendations)}")
    print(f"  Product recommendations: {len(product_recommendations)}")
    print("\nRecommendations stored in BigQuery!")
    
    # Optional: Show sample recommendations
    print("\n" + "="*60)
    print("      SAMPLE RECOMMENDATIONS")
    print("="*60)
    
    # Sample user recommendation
    sample_user_id = list(mappings['user_mapping'].keys())[0]
    print(f"\nSample personalized recommendations for user {sample_user_id}:")
    sample_user_recs = rec_engine.get_recommendations_for_user(sample_user_id, top_k=3)
    if 'error' not in sample_user_recs:
        for rec in sample_user_recs['recommendations']:
            print(f"  {rec['rank']}. {rec['product_title']} (Score: {rec['similarity_score']:.4f})")
    
    # Sample product recommendation
    sample_product_id = list(mappings['product_mapping'].keys())[0]
    print(f"\nSample similar products for product {sample_product_id}:")
    sample_prod_recs = rec_engine.get_similar_products(sample_product_id, top_k=3)
    if 'error' not in sample_prod_recs:
        print(f"Query: {sample_prod_recs['query_product_title']}")
        for rec in sample_prod_recs['similar_products']:
            print(f"  {rec['rank']}. {rec['product_title']} (Score: {rec['similarity_score']:.4f})")

if __name__ == "__main__":
    main()