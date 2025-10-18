"""
GNN model architecture
"""
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv

class PersonalizedHeteroGNN(nn.Module):
    def __init__(
        self, 
        num_users: int,
        num_brands: int,
        num_categories: int,
        num_shops: int,
        hidden_channels: int = 64, 
        out_channels: int = 32
    ):
        super().__init__()
        
        # Store dimensions
        self.num_users = num_users
        self.num_brands = num_brands
        self.num_categories = num_categories
        self.num_shops = num_shops
        
        # Learnable embeddings
        self.user_embedding = nn.Embedding(num_users, hidden_channels)
        self.brand_embedding = nn.Embedding(num_brands, hidden_channels)
        self.category_embedding = nn.Embedding(num_categories, hidden_channels)
        self.shop_embedding = nn.Embedding(num_shops, hidden_channels)
        
        # Product projection
        self.product_proj = nn.Linear(384, hidden_channels)
        
        # GNN layers
        self.conv1 = SAGEConv(hidden_channels, hidden_channels)
        self.conv2 = SAGEConv(hidden_channels, out_channels)
        
        self.dropout = nn.Dropout(0.2)
        
    def forward(self, x_dict, edge_index_dict):
        # Prepare node features
        node_features = {}
        
        node_features['product'] = F.relu(self.product_proj(x_dict['product']))
        node_features['user'] = self.user_embedding.weight
        node_features['brand'] = self.brand_embedding.weight
        node_features['category'] = self.category_embedding.weight
        node_features['shop'] = self.shop_embedding.weight
        
        # Get node counts
        num_products = node_features['product'].shape[0]
        num_users = node_features['user'].shape[0]
        num_brands = node_features['brand'].shape[0]
        num_categories = node_features['category'].shape[0]
        num_shops = node_features['shop'].shape[0]
        
        # Concatenate all features
        all_node_features = torch.cat([
            node_features['product'],
            node_features['user'],
            node_features['brand'],
            node_features['category'],
            node_features['shop']
        ], dim=0)
        
        # Adjust edge indices
        adjusted_edges = []
        
        # Product-Brand
        if ('product', 'has_brand', 'brand') in edge_index_dict:
            pb_edges = edge_index_dict[('product', 'has_brand', 'brand')].clone()
            pb_edges[1] += num_products + num_users
            adjusted_edges.extend([pb_edges, torch.stack([pb_edges[1], pb_edges[0]])])
        
        # Product-Category
        if ('product', 'in_category', 'category') in edge_index_dict:
            pc_edges = edge_index_dict[('product', 'in_category', 'category')].clone()
            pc_edges[1] += num_products + num_users + num_brands
            adjusted_edges.extend([pc_edges, torch.stack([pc_edges[1], pc_edges[0]])])
        
        # Product-Shop
        if ('product', 'sold_by', 'shop') in edge_index_dict:
            ps_edges = edge_index_dict[('product', 'sold_by', 'shop')].clone()
            ps_edges[1] += num_products + num_users + num_brands + num_categories
            adjusted_edges.extend([ps_edges, torch.stack([ps_edges[1], ps_edges[0]])])
        
        # User-Product
        if ('user', 'interacts', 'product') in edge_index_dict:
            up_edges = edge_index_dict[('user', 'interacts', 'product')].clone()
            up_edges[0] += num_products
            adjusted_edges.extend([up_edges, torch.stack([up_edges[1], up_edges[0]])])
        
        # Combine edges
        combined_edge_index = torch.cat(adjusted_edges, dim=1) if adjusted_edges else torch.empty((2, 0), dtype=torch.long)
        
        # Apply GNN
        x = all_node_features
        x = self.conv1(x, combined_edge_index)
        x = F.relu(x)
        x = self.dropout(x)
        x = self.conv2(x, combined_edge_index)
        
        # Split back
        output_dict = {}
        start_idx = 0
        
        output_dict['product'] = x[start_idx:start_idx + num_products]
        start_idx += num_products
        
        output_dict['user'] = x[start_idx:start_idx + num_users]
        start_idx += num_users
        
        output_dict['brand'] = x[start_idx:start_idx + num_brands]
        start_idx += num_brands
        
        output_dict['category'] = x[start_idx:start_idx + num_categories]
        start_idx += num_categories
        
        output_dict['shop'] = x[start_idx:start_idx + num_shops]
        
        return output_dict