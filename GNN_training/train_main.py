"""
Main script for training GNN model (Weekly job)
"""
import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from big_query.load_data import load_data_from_bigquery, load_data_from_supabase, transform_data
from scripts.data_preparation import DataPreparator
from scripts.model_architecture import PersonalizedHeteroGNN
from scripts.utils import set_random_seeds
from training.trainer import GNNTrainer
from training.model_saver import ModelSaver

def main():
    """Main training pipeline"""
    
    print("="*60)
    print("      GNN TRAINING PIPELINE - WEEKLY JOB")
    print("="*60)
    
    # Load environment variables
    load_dotenv()
    
    # Set random seeds
    set_random_seeds(42)
    
    # Step 1: Load data from BigQuery and Supabase
    print("\nStep 1: Loading data from BigQuery and Supabase...")
    bigquery_df = load_data_from_bigquery()
    profiles_df, activity_df = load_data_from_supabase()
    
    # Step 2: Transform data
    print("\nStep 2: Transforming data...")
    final_dataframes = transform_data(bigquery_df, profiles_df, activity_df)
    
    # Get dataframes separately
    product_df = final_dataframes['products']
    brand_df = final_dataframes['brands']
    category_df = final_dataframes['categories']
    shop_df = final_dataframes['shops']
    users_df = final_dataframes['users']
    product_brand_edges_df = final_dataframes['product_brand_edges']
    product_category_edges_df = final_dataframes['product_category_edges']
    product_shop_edges_df = final_dataframes['product_shop_edges']
    user_product_edges_df = final_dataframes['user_product_edges']
    
    print(f"\nData loaded:")
    print(f"  Products: {len(product_df)}")
    print(f"  Users: {len(users_df)}")
    print(f"  Brands: {len(brand_df)}")
    print(f"  Categories: {len(category_df)}")
    print(f"  Shops: {len(shop_df)}")
    print(f"  User-Product interactions: {len(user_product_edges_df)}")
    
    # Step 3: Prepare data for GNN
    print("\nStep 3: Preparing data for GNN...")
    data_preparator = DataPreparator()
    
    # Create mappings
    product_mapping, user_mapping, brand_mapping, category_mapping, shop_mapping = \
        data_preparator.create_node_mappings(product_df, users_df, brand_df, category_df, shop_df)
    
    # Generate embeddings
    product_embeddings = data_preparator.generate_product_embeddings(product_df)
    
    # Create edge indices
    pb_edges, pc_edges, ps_edges, up_edges = data_preparator.create_edge_indices(
        product_brand_edges_df, product_category_edges_df, product_shop_edges_df, user_product_edges_df,
        product_mapping, user_mapping, brand_mapping, category_mapping, shop_mapping
    )
    
    # Build graph
    data = data_preparator.build_hetero_graph(
        product_embeddings, pb_edges, pc_edges, ps_edges, up_edges,
        len(user_mapping), len(brand_mapping), len(category_mapping), len(shop_mapping)
    )
    
    # Step 4: Initialize model
    print("\nStep 4: Initializing GNN model...")
    model = PersonalizedHeteroGNN(
        num_users=len(user_mapping),
        num_brands=len(brand_mapping),
        num_categories=len(category_mapping),
        num_shops=len(shop_mapping),
        hidden_channels=64,
        out_channels=32
    )
    
    print(f"Model initialized with {sum(p.numel() for p in model.parameters()):,} parameters")
    
    # Step 5: Train model
    print("\nStep 5: Training model...")
    trainer = GNNTrainer(
        model=model,
        data=data,
        product_mapping=product_mapping,
        user_mapping=user_mapping,
        brand_mapping=brand_mapping,
        category_mapping=category_mapping,
        learning_rate=0.002
    )
    
    training_results = trainer.train(
        num_epochs=120,
        user_product_weight=0.4,
        brand_weight=0.3,
        category_weight=0.3
    )
    
    print(f"\nTraining completed:")
    print(f"  Final train loss: {training_results['final_train_loss']:.4f}")
    print(f"  Final val loss: {training_results['final_val_loss']:.4f}")
    
    # Step 6: Save model
    print("\nStep 6: Saving model...")
    model_saver = ModelSaver(models_dir="GNN_training/models")
    
    mappings = {
        'product_mapping': product_mapping,
        'user_mapping': user_mapping,
        'brand_mapping': brand_mapping,
        'category_mapping': category_mapping,
        'shop_mapping': shop_mapping
    }
    
    model_path = model_saver.save_model(
        model=model,
        mappings=mappings,
        training_results=training_results,
        model_name="personalized_gnn"
    )
    
    print("\n" + "="*60)
    print("      TRAINING PIPELINE COMPLETED SUCCESSFULLY")
    print("="*60)
    print(f"\nModel saved to: {model_path}")
    print("Ready for daily predictions!")

if __name__ == "__main__":
    main()