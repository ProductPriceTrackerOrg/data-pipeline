# Cell 1: Import all required libraries
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import HeteroData
from torch_geometric.nn import SAGEConv, to_hetero
from torch_geometric.loader import LinkNeighborLoader
from sentence_transformers import SentenceTransformer
from sklearn.manifold import TSNE
from collections import defaultdict
import random

# Set random seeds for reproducibility
torch.manual_seed(42)
np.random.seed(42)
random.seed(42)


def create_node_mappings(product_df, brand_df, category_df, shop_df):
    """Create dictionaries to map original IDs to sequential indices starting from 0"""

    # Product mapping: shop_product_id -> index
    product_mapping = {pid: idx for idx, pid in enumerate(product_df['shop_product_id'].unique())}

    # Brand mapping: brand_native_name -> index
    brand_mapping = {brand: idx for idx, brand in enumerate(brand_df['brand_native_name'].unique())}

    # Category mapping: category_id -> index
    category_mapping = {cid: idx for idx, cid in enumerate(category_df['category_id'].unique())}

    # Shop mapping: shop_id -> index
    shop_mapping = {sid: idx for idx, sid in enumerate(shop_df['shop_id'].unique())}

    return product_mapping, brand_mapping, category_mapping, shop_mapping

def generate_product_embeddings(product_df):
    """Generate embeddings for product titles using SentenceTransformer"""

    # Load pre-trained sentence transformer model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Generate embeddings for all product titles
    embeddings = model.encode(product_df['product_title_native'].tolist())

    # Convert to PyTorch tensor
    embeddings_tensor = torch.tensor(embeddings, dtype=torch.float)

    return embeddings_tensor

def create_edge_indices(product_brand_edges_df, product_category_edges_df, product_shop_edges_df,
                       product_mapping, brand_mapping, category_mapping, shop_mapping):
    """Create edge index tensors using the node mappings"""

    # Product-Brand edges
    product_brand_edges = []
    for _, row in product_brand_edges_df.iterrows():
        src = product_mapping[row['shop_product_id']]
        dst = brand_mapping[row['brand_native_name']]
        product_brand_edges.append([src, dst])

    # Product-Category edges
    product_category_edges = []
    for _, row in product_category_edges_df.iterrows():
        src = product_mapping[row['shop_product_id']]
        dst = category_mapping[row['category_id']]  # Updated to match the column name in load_data.py
        product_category_edges.append([src, dst])

    # Product-Shop edges
    product_shop_edges = []
    for _, row in product_shop_edges_df.iterrows():
        src = product_mapping[row['shop_product_id']]
        dst = shop_mapping[row['shop_id']]
        product_shop_edges.append([src, dst])

    # Convert to PyTorch tensors and transpose to [2, num_edges] format
    product_brand_edge_index = torch.tensor(product_brand_edges, dtype=torch.long).t()
    product_category_edge_index = torch.tensor(product_category_edges, dtype=torch.long).t()
    product_shop_edge_index = torch.tensor(product_shop_edges, dtype=torch.long).t()

    return product_brand_edge_index, product_category_edge_index, product_shop_edge_index

def build_hetero_graph(product_embeddings, pb_edges, pc_edges, ps_edges,
                      num_brands, num_categories, num_shops):
    """Build a PyTorch Geometric HeteroData object"""

    data = HeteroData()

    # Add node features
    data['product'].x = product_embeddings
    # Other node types will get their features from learnable embeddings in the model
    data['brand'].num_nodes = num_brands
    data['category'].num_nodes = num_categories
    data['shop'].num_nodes = num_shops

    # Add edge indices
    data['product', 'has_brand', 'brand'].edge_index = pb_edges
    data['product', 'in_category', 'category'].edge_index = pc_edges
    data['product', 'sold_by', 'shop'].edge_index = ps_edges

    return data