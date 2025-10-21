"""
Model loading and embedding generation for predictions
"""
import torch
import os
from pathlib import Path
from scripts.model_architecture import PersonalizedHeteroGNN
from scripts.data_preparation import DataPreparator

BASE_DIR = Path(__file__).resolve().parents[1]


class ModelPredictor:
    def __init__(self, models_dir: str = None, model_name: str = "personalized_gnn"):
        self.models_dir = str(BASE_DIR / "models") if models_dir is None else models_dir
        self.model_name = model_name
        self.model = None
        self.mappings = None
        self.data_preparator = DataPreparator()
        
    def load_model(self, use_latest: bool = True):
        """Load trained model and mappings"""
        
        if use_latest:
            model_path = os.path.join(self.models_dir, f"{self.model_name}_latest_model.pth")
            data_path = os.path.join(self.models_dir, f"{self.model_name}_latest_data.pth")
        else:
            raise NotImplementedError("Specify model path for non-latest models")
        
        if not os.path.exists(model_path) or not os.path.exists(data_path):
            raise FileNotFoundError(f"Model files not found in {self.models_dir}")
        
        print(f"Loading model from {model_path}")
        
        # Load data with weights_only=False to handle numpy arrays in the saved data
        saved_data = torch.load(data_path, map_location='cpu', weights_only=False)
        
        self.mappings = {
            'product_mapping': saved_data['product_mapping'],
            'user_mapping': saved_data['user_mapping'],
            'brand_mapping': saved_data['brand_mapping'],
            'category_mapping': saved_data['category_mapping'],
            'shop_mapping': saved_data['shop_mapping']
        }
        
        model_config = saved_data['model_config']
        
        # Recreate model
        self.model = PersonalizedHeteroGNN(
            num_users=model_config['num_users'],
            num_brands=model_config['num_brands'],
            num_categories=model_config['num_categories'],
            num_shops=model_config['num_shops'],
            hidden_channels=model_config['hidden_channels'],
            out_channels=model_config['out_channels']
        )
        
        # Load weights with weights_only=False to handle numpy arrays in the saved model
        self.model.load_state_dict(torch.load(model_path, map_location='cpu', weights_only=False))
        self.model.eval()
        
        print("Model loaded successfully")
        print(f"  Users: {len(self.mappings['user_mapping'])}")
        print(f"  Products: {len(self.mappings['product_mapping'])}")
        print(f"  Brands: {len(self.mappings['brand_mapping'])}")
        print(f"  Categories: {len(self.mappings['category_mapping'])}")
        print(f"  Shops: {len(self.mappings['shop_mapping'])}")
        
        return self.model, self.mappings
    
    def prepare_daily_data(self, dataframes: dict):
        """Prepare daily data for inference"""
        
        product_df = dataframes['products']
        user_df = dataframes['users']
        brand_df = dataframes['brands']
        category_df = dataframes['categories']
        shop_df = dataframes['shops']
        
        # Update mappings for new items
        self._update_mappings_for_new_data(product_df, user_df, brand_df, category_df, shop_df)
        
        # Generate embeddings
        product_embeddings = self.data_preparator.generate_product_embeddings(product_df)
        
        # Create edges
        pb_edges, pc_edges, ps_edges, up_edges = self.data_preparator.create_edge_indices(
            dataframes['product_brand_edges'],
            dataframes['product_category_edges'],
            dataframes['product_shop_edges'],
            dataframes['user_product_edges'],
            self.mappings['product_mapping'],
            self.mappings['user_mapping'],
            self.mappings['brand_mapping'],
            self.mappings['category_mapping'],
            self.mappings['shop_mapping']
        )
        
        # Build graph
        data = self.data_preparator.build_hetero_graph(
            product_embeddings,
            pb_edges, pc_edges, ps_edges, up_edges,
            len(self.mappings['user_mapping']),
            len(self.mappings['brand_mapping']),
            len(self.mappings['category_mapping']),
            len(self.mappings['shop_mapping'])
        )
        
        return data
    
    def _update_mappings_for_new_data(self, product_df, user_df, brand_df, category_df, shop_df):
        """Update mappings to handle new items"""
        
        new_products = 0
        new_users = 0
        
        # Update product mapping
        max_prod_idx = max(self.mappings['product_mapping'].values()) if self.mappings['product_mapping'] else -1
        for prod_id in product_df['shop_product_id']:
            if prod_id not in self.mappings['product_mapping']:
                max_prod_idx += 1
                self.mappings['product_mapping'][prod_id] = max_prod_idx
                new_products += 1
        
        # Update user mapping
        max_user_idx = max(self.mappings['user_mapping'].values()) if self.mappings['user_mapping'] else -1
        for user_id in user_df['user_id']:
            if user_id not in self.mappings['user_mapping']:
                max_user_idx += 1
                self.mappings['user_mapping'][user_id] = max_user_idx
                new_users += 1
        
        # Update brand mapping
        max_brand_idx = max(self.mappings['brand_mapping'].values()) if self.mappings['brand_mapping'] else -1
        for brand in brand_df['brand_native_name']:
            if brand not in self.mappings['brand_mapping']:
                max_brand_idx += 1
                self.mappings['brand_mapping'][brand] = max_brand_idx
        
        # Update category mapping
        max_cat_idx = max(self.mappings['category_mapping'].values()) if self.mappings['category_mapping'] else -1
        for cat_id in category_df['category_id']:
            if cat_id not in self.mappings['category_mapping']:
                max_cat_idx += 1
                self.mappings['category_mapping'][cat_id] = max_cat_idx
        
        # Update shop mapping
        max_shop_idx = max(self.mappings['shop_mapping'].values()) if self.mappings['shop_mapping'] else -1
        for shop_id in shop_df['shop_id']:
            if shop_id not in self.mappings['shop_mapping']:
                max_shop_idx += 1
                self.mappings['shop_mapping'][shop_id] = max_shop_idx
        
        if new_products > 0 or new_users > 0:
            print(f"Mappings updated: {new_products} new products, {new_users} new users")
    
    def generate_embeddings(self, data):
        """Generate fresh embeddings using loaded model"""
        
        print("Generating embeddings...")
        
        self.model.eval()
        with torch.no_grad():
            embeddings = self.model(data.x_dict, data.edge_index_dict)
        
        print("Embeddings generated")
        return embeddings