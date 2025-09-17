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
from .prepare_data import create_node_mappings, generate_product_embeddings, create_edge_indices, build_hetero_graph

# Set random seeds for reproducibility
torch.manual_seed(42)
np.random.seed(42)
random.seed(42)

class HeteroGNN(nn.Module):
    def __init__(self, hidden_channels=64, out_channels=32, brand_mapping=None, category_mapping=None, shop_mapping=None):
        super().__init__()

        # Learnable embeddings for nodes without initial features
        self.brand_embedding = nn.Embedding(len(brand_mapping), hidden_channels)
        self.category_embedding = nn.Embedding(len(category_mapping), hidden_channels)
        self.shop_embedding = nn.Embedding(len(shop_mapping), hidden_channels)

        # Linear projection for product features (384 -> hidden_channels)
        self.product_proj = nn.Linear(384, hidden_channels)

        # Two SAGE convolution layers
        self.conv1 = SAGEConv(hidden_channels, hidden_channels)
        self.conv2 = SAGEConv(hidden_channels, out_channels)

        # Dropout for regularization
        self.dropout = nn.Dropout(0.3)

    def forward(self, x_dict, edge_index_dict):
        # Step 1: Prepare node features for all node types
        node_features = {}

        # Product nodes: project the SentenceTransformer embeddings
        node_features['product'] = F.relu(self.product_proj(x_dict['product']))

        # Other nodes: use learnable embeddings
        node_features['brand'] = self.brand_embedding.weight
        node_features['category'] = self.category_embedding.weight
        node_features['shop'] = self.shop_embedding.weight

        # Step 2: Create a unified node feature tensor and edge index
        # We'll concatenate all node features and adjust edge indices accordingly

        num_products = node_features['product'].shape[0]
        num_brands = node_features['brand'].shape[0]
        num_categories = node_features['category'].shape[0]
        num_shops = node_features['shop'].shape[0]

        # Concatenate all node features
        all_node_features = torch.cat([
            node_features['product'],
            node_features['brand'],
            node_features['category'],
            node_features['shop']
        ], dim=0)

        # Adjust edge indices to work with concatenated nodes
        adjusted_edges = []

        # Product-Brand edges (brand indices need offset)
        if ('product', 'has_brand', 'brand') in edge_index_dict:
            pb_edges = edge_index_dict[('product', 'has_brand', 'brand')].clone()
            pb_edges[1] += num_products  # Offset brand indices
            adjusted_edges.append(pb_edges)
            # Add reverse edges for undirected graph
            pb_edges_rev = torch.stack([pb_edges[1], pb_edges[0]])
            adjusted_edges.append(pb_edges_rev)

        # Product-Category edges
        if ('product', 'in_category', 'category') in edge_index_dict:
            pc_edges = edge_index_dict[('product', 'in_category', 'category')].clone()
            pc_edges[1] += num_products + num_brands  # Offset category indices
            adjusted_edges.append(pc_edges)
            # Add reverse edges
            pc_edges_rev = torch.stack([pc_edges[1], pc_edges[0]])
            adjusted_edges.append(pc_edges_rev)

        # Product-Shop edges
        if ('product', 'sold_by', 'shop') in edge_index_dict:
            ps_edges = edge_index_dict[('product', 'sold_by', 'shop')].clone()
            ps_edges[1] += num_products + num_brands + num_categories  # Offset shop indices
            adjusted_edges.append(ps_edges)
            # Add reverse edges
            ps_edges_rev = torch.stack([ps_edges[1], ps_edges[0]])
            adjusted_edges.append(ps_edges_rev)

        # Combine all edges
        if adjusted_edges:
            combined_edge_index = torch.cat(adjusted_edges, dim=1)
        else:
            combined_edge_index = torch.empty((2, 0), dtype=torch.long)

        # Step 3: Apply GNN layers
        x = all_node_features

        # First SAGE layer
        x = self.conv1(x, combined_edge_index)
        x = F.relu(x)
        x = self.dropout(x)

        # Second SAGE layer
        x = self.conv2(x, combined_edge_index)

        # Step 4: Split back into node type dictionaries
        output_dict = {}
        start_idx = 0

        output_dict['product'] = x[start_idx:start_idx + num_products]
        start_idx += num_products

        output_dict['brand'] = x[start_idx:start_idx + num_brands]
        start_idx += num_brands

        output_dict['category'] = x[start_idx:start_idx + num_categories]
        start_idx += num_categories

        output_dict['shop'] = x[start_idx:start_idx + num_shops]

        return output_dict

def setup_training_multitask(data, model, product_mapping, brand_mapping, category_mapping, shop_mapping):
    """Setup optimizer and training data for both product-brand and product-category prediction"""

    # Optimizer
    optimizer = torch.optim.Adam(model.parameters(), lr=0.005)

    # Get edges for BOTH tasks
    pb_edge_index = data['product', 'has_brand', 'brand'].edge_index
    pc_edge_index = data['product', 'in_category', 'category'].edge_index

    # Split product-brand edges
    num_pb_edges = pb_edge_index.shape[1]
    perm_pb = torch.randperm(num_pb_edges)
    train_pb_edges = pb_edge_index[:, perm_pb[:int(0.8 * num_pb_edges)]]
    val_pb_edges = pb_edge_index[:, perm_pb[int(0.8 * num_pb_edges):]]

    # Split product-category edges
    num_pc_edges = pc_edge_index.shape[1]
    perm_pc = torch.randperm(num_pc_edges)
    train_pc_edges = pc_edge_index[:, perm_pc[:int(0.8 * num_pc_edges)]]
    val_pc_edges = pc_edge_index[:, perm_pc[int(0.8 * num_pc_edges):]]

    # Generate negative edges for BOTH tasks
    def generate_negative_edges(pos_edges, num_src_nodes, num_dst_nodes, num_neg=None):
        """Generate negative edges that don't exist in the graph"""
        if num_neg is None:
            num_neg = pos_edges.shape[1]

        existing_edges = set()
        for i in range(pos_edges.shape[1]):
            existing_edges.add((pos_edges[0, i].item(), pos_edges[1, i].item()))

        negative_edges = []
        while len(negative_edges) < num_neg:
            src_idx = torch.randint(0, num_src_nodes, (1,)).item()
            dst_idx = torch.randint(0, num_dst_nodes, (1,)).item()

            if (src_idx, dst_idx) not in existing_edges:
                negative_edges.append([src_idx, dst_idx])

        return torch.tensor(negative_edges, dtype=torch.long).t()

    # Generate negative edges for both tasks
    train_pb_neg_edges = generate_negative_edges(
        train_pb_edges, len(product_mapping), len(brand_mapping)
    )
    val_pb_neg_edges = generate_negative_edges(
        val_pb_edges, len(product_mapping), len(brand_mapping)
    )

    train_pc_neg_edges = generate_negative_edges(
        train_pc_edges, len(product_mapping), len(category_mapping)
    )
    val_pc_neg_edges = generate_negative_edges(
        val_pc_edges, len(product_mapping), len(category_mapping)
    )

    return (optimizer,
            train_pb_edges, val_pb_edges, train_pb_neg_edges, val_pb_neg_edges,
            train_pc_edges, val_pc_edges, train_pc_neg_edges, val_pc_neg_edges)

def train_model_multitask(model, data, training_components, num_epochs=50,
                         brand_weight=0.5, category_weight=0.5):
    """Train the GNN model on both product-brand and product-category prediction"""

    optimizer = training_components[0]
    train_pb_edges, val_pb_edges, train_pb_neg_edges, val_pb_neg_edges = training_components[1:5]
    train_pc_edges, val_pc_edges, train_pc_neg_edges, val_pc_neg_edges = training_components[5:9]

    train_losses = []
    val_losses = []
    brand_losses = []
    category_losses = []

    for epoch in range(num_epochs):
        # Training phase
        model.train()
        optimizer.zero_grad()

        # Forward pass on entire graph
        out = model({'product': data['product'].x}, data.edge_index_dict)

        # === TASK 1: Product-Brand Link Prediction ===
        # Prepare brand training data (positive + negative)
        all_train_pb_edges = torch.cat([train_pb_edges, train_pb_neg_edges], dim=1)
        all_train_pb_labels = torch.cat([
            torch.ones(train_pb_edges.shape[1]),    # Positive = 1
            torch.zeros(train_pb_neg_edges.shape[1]) # Negative = 0
        ])

        # Calculate brand predictions
        src_emb_brand = out['product'][all_train_pb_edges[0]]
        dst_emb_brand = out['brand'][all_train_pb_edges[1]]
        pred_brand = (src_emb_brand * dst_emb_brand).sum(dim=-1)

        # Brand loss
        loss_brand = F.binary_cross_entropy_with_logits(pred_brand, all_train_pb_labels)

        # === TASK 2: Product-Category Link Prediction ===
        # Prepare category training data (positive + negative)
        all_train_pc_edges = torch.cat([train_pc_edges, train_pc_neg_edges], dim=1)
        all_train_pc_labels = torch.cat([
            torch.ones(train_pc_edges.shape[1]),    # Positive = 1
            torch.zeros(train_pc_neg_edges.shape[1]) # Negative = 0
        ])

        # Calculate category predictions
        src_emb_category = out['product'][all_train_pc_edges[0]]
        dst_emb_category = out['category'][all_train_pc_edges[1]]
        pred_category = (src_emb_category * dst_emb_category).sum(dim=-1)

        # Category loss
        loss_category = F.binary_cross_entropy_with_logits(pred_category, all_train_pc_labels)

        # === COMBINE LOSSES ===
        total_loss = brand_weight * loss_brand + category_weight * loss_category

        # Backward pass
        total_loss.backward()
        optimizer.step()

        # === VALIDATION PHASE ===
        model.eval()
        with torch.no_grad():
            out_val = model({'product': data['product'].x}, data.edge_index_dict)

            # Validation for brand prediction
            all_val_pb_edges = torch.cat([val_pb_edges, val_pb_neg_edges], dim=1)
            all_val_pb_labels = torch.cat([
                torch.ones(val_pb_edges.shape[1]),
                torch.zeros(val_pb_neg_edges.shape[1])
            ])

            src_emb_brand_val = out_val['product'][all_val_pb_edges[0]]
            dst_emb_brand_val = out_val['brand'][all_val_pb_edges[1]]
            pred_brand_val = (src_emb_brand_val * dst_emb_brand_val).sum(dim=-1)
            val_loss_brand = F.binary_cross_entropy_with_logits(pred_brand_val, all_val_pb_labels)

            # Validation for category prediction
            all_val_pc_edges = torch.cat([val_pc_edges, val_pc_neg_edges], dim=1)
            all_val_pc_labels = torch.cat([
                torch.ones(val_pc_edges.shape[1]),
                torch.zeros(val_pc_neg_edges.shape[1])
            ])

            src_emb_category_val = out_val['product'][all_val_pc_edges[0]]
            dst_emb_category_val = out_val['category'][all_val_pc_edges[1]]
            pred_category_val = (src_emb_category_val * dst_emb_category_val).sum(dim=-1)
            val_loss_category = F.binary_cross_entropy_with_logits(pred_category_val, all_val_pc_labels)

            # Combined validation loss
            val_loss_total = brand_weight * val_loss_brand + category_weight * val_loss_category

        # Store all losses
        train_losses.append(total_loss.item())
        val_losses.append(val_loss_total.item())
        brand_losses.append(loss_brand.item())
        category_losses.append(loss_category.item())

        # Print progress
        if epoch % 2 == 0:
            print(f"Epoch {epoch:02d}")
            print(f"  Total Loss - Train: {total_loss.item():.4f}, Val: {val_loss_total.item():.4f}")
            print(f"  Brand Loss: {loss_brand.item():.4f}, Category Loss: {loss_category.item():.4f}")

    return train_losses, val_losses, brand_losses, category_losses