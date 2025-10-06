"""
Data preparation utilities for GNN training and inference
"""
import torch
import pandas as pd
from sentence_transformers import SentenceTransformer
from torch_geometric.data import HeteroData
from typing import Dict, Tuple

class DataPreparator:
    def __init__(self):
        self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def create_node_mappings(
        self, 
        product_df: pd.DataFrame,
        user_df: pd.DataFrame,
        brand_df: pd.DataFrame,
        category_df: pd.DataFrame,
        shop_df: pd.DataFrame
    ) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
        """Create mappings from original IDs to sequential indices"""
        
        product_mapping = {
            pid: idx for idx, pid in enumerate(product_df['shop_product_id'].unique())
        }
        user_mapping = {
            uid: idx for idx, uid in enumerate(user_df['user_id'].unique())
        }
        brand_mapping = {
            brand: idx for idx, brand in enumerate(brand_df['brand_native_name'].unique())
        }
        category_mapping = {
            cid: idx for idx, cid in enumerate(category_df['category_id'].unique())
        }
        shop_mapping = {
            sid: idx for idx, sid in enumerate(shop_df['shop_id'].unique())
        }
        
        return product_mapping, user_mapping, brand_mapping, category_mapping, shop_mapping
    
    def generate_product_embeddings(self, product_df: pd.DataFrame) -> torch.Tensor:
        """Generate SentenceTransformer embeddings for products"""
        
        print("Generating product embeddings...")
        embeddings = self.sentence_model.encode(
            product_df['product_title_native'].tolist(), 
            show_progress_bar=True
        )
        return torch.tensor(embeddings, dtype=torch.float)
    
    def create_edge_indices(
        self,
        product_brand_edges_df: pd.DataFrame,
        product_category_edges_df: pd.DataFrame,
        product_shop_edges_df: pd.DataFrame,
        user_product_edges_df: pd.DataFrame,
        product_mapping: Dict,
        user_mapping: Dict,
        brand_mapping: Dict,
        category_mapping: Dict,
        shop_mapping: Dict
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        """Create edge index tensors for all relationships"""
        
        # Product-Brand edges
        pb_edges = []
        for _, row in product_brand_edges_df.iterrows():
            if (row['shop_product_id'] in product_mapping and 
                row['brand_native_name'] in brand_mapping):
                src = product_mapping[row['shop_product_id']]
                dst = brand_mapping[row['brand_native_name']]
                pb_edges.append([src, dst])
        
        # Product-Category edges (FIXED: using 'category_id')
        pc_edges = []
        for _, row in product_category_edges_df.iterrows():
            if (row['shop_product_id'] in product_mapping and 
                row['category_id'] in category_mapping):
                src = product_mapping[row['shop_product_id']]
                dst = category_mapping[row['category_id']]
                pc_edges.append([src, dst])
        
        # Product-Shop edges
        ps_edges = []
        for _, row in product_shop_edges_df.iterrows():
            if (row['shop_product_id'] in product_mapping and 
                row['shop_id'] in shop_mapping):
                src = product_mapping[row['shop_product_id']]
                dst = shop_mapping[row['shop_id']]
                ps_edges.append([src, dst])
        
        # User-Product edges
        up_edges = []
        for _, row in user_product_edges_df.iterrows():
            if (row['user_id'] in user_mapping and 
                row['shop_product_id'] in product_mapping):
                src = user_mapping[row['user_id']]
                dst = product_mapping[row['shop_product_id']]
                up_edges.append([src, dst])
        
        # Convert to tensors
        pb_edge_index = torch.tensor(pb_edges, dtype=torch.long).t() if pb_edges else torch.empty((2, 0), dtype=torch.long)
        pc_edge_index = torch.tensor(pc_edges, dtype=torch.long).t() if pc_edges else torch.empty((2, 0), dtype=torch.long)
        ps_edge_index = torch.tensor(ps_edges, dtype=torch.long).t() if ps_edges else torch.empty((2, 0), dtype=torch.long)
        up_edge_index = torch.tensor(up_edges, dtype=torch.long).t() if up_edges else torch.empty((2, 0), dtype=torch.long)
        
        print(f"Edge indices created:")
        print(f"  Product-Brand: {pb_edge_index.shape[1]}")
        print(f"  Product-Category: {pc_edge_index.shape[1]}")
        print(f"  Product-Shop: {ps_edge_index.shape[1]}")
        print(f"  User-Product: {up_edge_index.shape[1]}")
        
        return pb_edge_index, pc_edge_index, ps_edge_index, up_edge_index
    
    def build_hetero_graph(
        self,
        product_embeddings: torch.Tensor,
        pb_edges: torch.Tensor,
        pc_edges: torch.Tensor,
        ps_edges: torch.Tensor,
        up_edges: torch.Tensor,
        num_users: int,
        num_brands: int,
        num_categories: int,
        num_shops: int
    ) -> HeteroData:
        """Build HeteroData object"""
        
        data = HeteroData()
        
        # Add node features
        data['product'].x = product_embeddings
        data['user'].num_nodes = num_users
        data['brand'].num_nodes = num_brands
        data['category'].num_nodes = num_categories
        data['shop'].num_nodes = num_shops
        
        # Add edge indices
        data['product', 'has_brand', 'brand'].edge_index = pb_edges
        data['product', 'in_category', 'category'].edge_index = pc_edges
        data['product', 'sold_by', 'shop'].edge_index = ps_edges
        data['user', 'interacts', 'product'].edge_index = up_edges
        
        print("\nHeterogeneous graph built:")
        print(data)
        
        return data