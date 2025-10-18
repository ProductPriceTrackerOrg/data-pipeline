from big_query.load_data import transform_data, load_data_from_bigquery
import os
import matplotlib.pyplot as plt
import seaborn as sns
import torch
import pandas as pd
from training.prepare_data import create_node_mappings, generate_product_embeddings, create_edge_indices, build_hetero_graph
from training.model_building import HeteroGNN, setup_training_multitask, train_model_multitask



raw_df = load_data_from_bigquery()
final_dataframes = transform_data(raw_df)

product_df = final_dataframes['products']
brand_df = final_dataframes['brands']
category_df = final_dataframes['categories']
shop_df = final_dataframes['shops']
product_brand_edges_df = final_dataframes['product_brand_edges']
product_category_edges_df = final_dataframes['product_category_edges']
product_shop_edges_df = final_dataframes['product_shop_edges']

print(f"Products: {len(product_df)}")
print(f"Brands: {len(brand_df)}")
print(f"Categories: {len(category_df)}")
print(f"Shops: {len(shop_df)}")

# Get the set of all valid, existing product IDs from your main product table
valid_product_ids = set(product_df['shop_product_id'])
print(f"Found {len(valid_product_ids)} unique, valid product IDs.")

# Filter all edge DataFrames to only include rows with valid product IDs
original_brand_edges = len(product_brand_edges_df)
product_brand_edges_df = product_brand_edges_df[product_brand_edges_df['shop_product_id'].isin(valid_product_ids)]
print(f"Filtered brand edges: {original_brand_edges} -> {len(product_brand_edges_df)}")

original_category_edges = len(product_category_edges_df)
product_category_edges_df = product_category_edges_df[product_category_edges_df['shop_product_id'].isin(valid_product_ids)]
print(f"Filtered category edges: {original_category_edges} -> {len(product_category_edges_df)}")

original_shop_edges = len(product_shop_edges_df)
product_shop_edges_df = product_shop_edges_df[product_shop_edges_df['shop_product_id'].isin(valid_product_ids)]
print(f"Filtered shop edges: {original_shop_edges} -> {len(product_shop_edges_df)}")

# Create all mappings
product_mapping, brand_mapping, category_mapping, shop_mapping = create_node_mappings(
    product_df, brand_df, category_df, shop_df
)

print("Node mappings created")

product_embeddings = generate_product_embeddings(product_df)

print(f"Product embeddings shape: {product_embeddings.shape}")
print(f"Embedding dimension: {product_embeddings.shape[1]}")
print(f"Sample embedding (first 5 dims): {product_embeddings[0][:5]}")


# Create edge indices
pb_edges, pc_edges, ps_edges = create_edge_indices(
    product_brand_edges_df, product_category_edges_df, product_shop_edges_df,
    product_mapping, brand_mapping, category_mapping, shop_mapping
)

print("Edge index tensors created:")
print(f"Product-Brand edges shape: {pb_edges.shape}")
print(f"Product-Category edges shape: {pc_edges.shape}")
print(f"Product-Shop edges shape: {ps_edges.shape}")

# Build the graph
data = build_hetero_graph(
    product_embeddings, pb_edges, pc_edges, ps_edges,
    len(brand_mapping), len(category_mapping), len(shop_mapping)
)

print("Heterogeneous graph created:")
print(data)
print(f"\nGraph structure:")
for node_type in data.node_types:
    if hasattr(data[node_type], 'x'):
        print(f"  {node_type}: {data[node_type].x.shape[0]} nodes, {data[node_type].x.shape[1]} features")
    else:
        print(f"  {node_type}: {data[node_type].num_nodes} nodes, learnable embeddings")

for edge_type in data.edge_types:
    print(f"  {edge_type}: {data[edge_type].edge_index.shape[1]} edges")

# Initialize the model
model = HeteroGNN(hidden_channels=64, out_channels=32, brand_mapping=brand_mapping, category_mapping=category_mapping, shop_mapping=shop_mapping)

print("Heterogeneous GNN model created successfully!")
print(f"Model parameters: {sum(p.numel() for p in model.parameters()):,}")

# Test forward pass to make sure everything works
print("\nTesting forward pass...")
with torch.no_grad():
    test_x_dict = {'product': data['product'].x}
    test_output = model(test_x_dict, data.edge_index_dict)
    print("✓ Forward pass successful!")
    print(f"Output shapes:")
    for node_type, tensor in test_output.items():
        print(f"  {node_type}: {tensor.shape}")

# Setup training for multi-task learning
training_data = setup_training_multitask(data, model, product_mapping, brand_mapping, category_mapping, shop_mapping)
optimizer = training_data[0]
train_pb_edges, val_pb_edges, train_pb_neg_edges, val_pb_neg_edges = training_data[1:5]
train_pc_edges, val_pc_edges, train_pc_neg_edges, val_pc_neg_edges = training_data[5:9]

# Train the model with multi-task learning
print("Starting multi-task training...")
train_losses, val_losses, brand_losses, category_losses = train_model_multitask(
    model, data, training_data, num_epochs=96, brand_weight=0.4, category_weight=0.6
)
print("Multi-task training completed!")

# Plot comprehensive training results
plt.figure(figsize=(15, 10))

# Plot 1: Combined losses
plt.subplot(2, 2, 1)
plt.plot(train_losses, label='Training Loss', color='blue')
plt.plot(val_losses, label='Validation Loss', color='red')
plt.title('Combined Training and Validation Loss')
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.legend()
plt.grid(True)

# Plot 2: Individual task losses
plt.subplot(2, 2, 2)
plt.plot(brand_losses, label='Brand Task Loss', color='green')
plt.plot(category_losses, label='Category Task Loss', color='orange')
plt.title('Individual Task Losses')
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.legend()
plt.grid(True)

# Plot 3: Loss difference (overfitting detection)
plt.subplot(2, 2, 3)
loss_diff = [val - train for val, train in zip(val_losses, train_losses)]
plt.plot(loss_diff, color='purple')
plt.title('Validation - Training Loss\n(Higher = More Overfitting)')
plt.xlabel('Epoch')
plt.ylabel('Loss Difference')
plt.grid(True)
plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)

# Plot 4: Learning curves smoothed
plt.subplot(2, 2, 4)
window = 5
if len(train_losses) >= window:
    train_smooth = pd.Series(train_losses).rolling(window).mean()
    val_smooth = pd.Series(val_losses).rolling(window).mean()
    plt.plot(train_smooth, label='Training (Smoothed)', color='blue')
    plt.plot(val_smooth, label='Validation (Smoothed)', color='red')
    plt.title('Smoothed Learning Curves')
    plt.xlabel('Epoch')
    plt.ylabel('Loss')
    plt.legend()
    plt.grid(True)

plt.tight_layout()
plt.show()

print(f"\nTraining Summary:")
print(f"Final Combined Loss - Train: {train_losses[-1]:.4f}, Val: {val_losses[-1]:.4f}")
print(f"Final Brand Task Loss: {brand_losses[-1]:.4f}")
print(f"Final Category Task Loss: {category_losses[-1]:.4f}")
print(f"Overfitting Check: {val_losses[-1] - train_losses[-1]:.4f} (lower is better)")