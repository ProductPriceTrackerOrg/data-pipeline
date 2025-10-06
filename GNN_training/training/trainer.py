"""
GNN training logic
"""
import torch
import torch.nn.functional as F
from typing import Tuple, Dict
from scripts.utils import generate_negative_edges

class GNNTrainer:
    def __init__(
        self, 
        model, 
        data, 
        product_mapping: Dict,
        user_mapping: Dict,
        brand_mapping: Dict,
        category_mapping: Dict,
        learning_rate: float = 0.002
    ):
        self.model = model
        self.data = data
        self.product_mapping = product_mapping
        self.user_mapping = user_mapping
        self.brand_mapping = brand_mapping
        self.category_mapping = category_mapping
        self.optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
        
    def setup_training_data(self) -> Tuple:
        """Setup training and validation splits with negative sampling"""
        
        # User-Product edges
        up_edge_index = self.data['user', 'interacts', 'product'].edge_index
        num_up = up_edge_index.shape[1]
        perm = torch.randperm(num_up)
        train_up = up_edge_index[:, perm[:int(0.8 * num_up)]]
        val_up = up_edge_index[:, perm[int(0.8 * num_up):]]
        
        # Product-Brand edges
        pb_edge_index = self.data['product', 'has_brand', 'brand'].edge_index
        num_pb = pb_edge_index.shape[1]
        perm_pb = torch.randperm(num_pb)
        train_pb = pb_edge_index[:, perm_pb[:int(0.8 * num_pb)]]
        val_pb = pb_edge_index[:, perm_pb[int(0.8 * num_pb):]]
        
        # Product-Category edges
        pc_edge_index = self.data['product', 'in_category', 'category'].edge_index
        num_pc = pc_edge_index.shape[1]
        perm_pc = torch.randperm(num_pc)
        train_pc = pc_edge_index[:, perm_pc[:int(0.8 * num_pc)]]
        val_pc = pc_edge_index[:, perm_pc[int(0.8 * num_pc):]]
        
        # Generate negative edges
        train_up_neg = generate_negative_edges(train_up, len(self.user_mapping), len(self.product_mapping))
        val_up_neg = generate_negative_edges(val_up, len(self.user_mapping), len(self.product_mapping))
        
        train_pb_neg = generate_negative_edges(train_pb, len(self.product_mapping), len(self.brand_mapping))
        val_pb_neg = generate_negative_edges(val_pb, len(self.product_mapping), len(self.brand_mapping))
        
        train_pc_neg = generate_negative_edges(train_pc, len(self.product_mapping), len(self.category_mapping))
        val_pc_neg = generate_negative_edges(val_pc, len(self.product_mapping), len(self.category_mapping))
        
        print(f"Training data prepared:")
        print(f"  User-Product: {train_up.shape[1]} train, {val_up.shape[1]} val")
        print(f"  Product-Brand: {train_pb.shape[1]} train, {val_pb.shape[1]} val")
        print(f"  Product-Category: {train_pc.shape[1]} train, {val_pc.shape[1]} val")
        
        return (train_up, val_up, train_up_neg, val_up_neg,
                train_pb, val_pb, train_pb_neg, val_pb_neg,
                train_pc, val_pc, train_pc_neg, val_pc_neg)
    
    def train(
        self, 
        num_epochs: int = 120,
        user_product_weight: float = 0.4,
        brand_weight: float = 0.3,
        category_weight: float = 0.3
    ) -> Dict:
        """Train the GNN model"""
        
        # Setup training data
        training_data = self.setup_training_data()
        (train_up, val_up, train_up_neg, val_up_neg,
         train_pb, val_pb, train_pb_neg, val_pb_neg,
         train_pc, val_pc, train_pc_neg, val_pc_neg) = training_data
        
        train_losses = []
        val_losses = []
        
        print(f"\nStarting training for {num_epochs} epochs...")
        
        for epoch in range(num_epochs):
            # Training phase
            self.model.train()
            self.optimizer.zero_grad()
            
            out = self.model({'product': self.data['product'].x}, self.data.edge_index_dict)
            
            # User-Product loss
            all_train_up = torch.cat([train_up, train_up_neg], dim=1)
            all_train_up_labels = torch.cat([
                torch.ones(train_up.shape[1]),
                torch.zeros(train_up_neg.shape[1])
            ])
            pred_up = (out['user'][all_train_up[0]] * out['product'][all_train_up[1]]).sum(dim=-1)
            loss_up = F.binary_cross_entropy_with_logits(pred_up, all_train_up_labels)
            
            # Product-Brand loss
            all_train_pb = torch.cat([train_pb, train_pb_neg], dim=1)
            all_train_pb_labels = torch.cat([
                torch.ones(train_pb.shape[1]),
                torch.zeros(train_pb_neg.shape[1])
            ])
            pred_pb = (out['product'][all_train_pb[0]] * out['brand'][all_train_pb[1]]).sum(dim=-1)
            loss_pb = F.binary_cross_entropy_with_logits(pred_pb, all_train_pb_labels)
            
            # Product-Category loss
            all_train_pc = torch.cat([train_pc, train_pc_neg], dim=1)
            all_train_pc_labels = torch.cat([
                torch.ones(train_pc.shape[1]),
                torch.zeros(train_pc_neg.shape[1])
            ])
            pred_pc = (out['product'][all_train_pc[0]] * out['category'][all_train_pc[1]]).sum(dim=-1)
            loss_pc = F.binary_cross_entropy_with_logits(pred_pc, all_train_pc_labels)
            
            # Combined loss
            total_loss = (user_product_weight * loss_up + 
                         brand_weight * loss_pb + 
                         category_weight * loss_pc)
            
            total_loss.backward()
            self.optimizer.step()
            
            # Validation
            self.model.eval()
            with torch.no_grad():
                out_val = self.model({'product': self.data['product'].x}, self.data.edge_index_dict)
                
                all_val_up = torch.cat([val_up, val_up_neg], dim=1)
                all_val_up_labels = torch.cat([torch.ones(val_up.shape[1]), torch.zeros(val_up_neg.shape[1])])
                pred_up_val = (out_val['user'][all_val_up[0]] * out_val['product'][all_val_up[1]]).sum(dim=-1)
                val_loss_up = F.binary_cross_entropy_with_logits(pred_up_val, all_val_up_labels)
                
                all_val_pb = torch.cat([val_pb, val_pb_neg], dim=1)
                all_val_pb_labels = torch.cat([torch.ones(val_pb.shape[1]), torch.zeros(val_pb_neg.shape[1])])
                pred_pb_val = (out_val['product'][all_val_pb[0]] * out_val['brand'][all_val_pb[1]]).sum(dim=-1)
                val_loss_pb = F.binary_cross_entropy_with_logits(pred_pb_val, all_val_pb_labels)
                
                all_val_pc = torch.cat([val_pc, val_pc_neg], dim=1)
                all_val_pc_labels = torch.cat([torch.ones(val_pc.shape[1]), torch.zeros(val_pc_neg.shape[1])])
                pred_pc_val = (out_val['product'][all_val_pc[0]] * out_val['category'][all_val_pc[1]]).sum(dim=-1)
                val_loss_pc = F.binary_cross_entropy_with_logits(pred_pc_val, all_val_pc_labels)
                
                val_loss_total = (user_product_weight * val_loss_up + 
                                 brand_weight * val_loss_pb + 
                                 category_weight * val_loss_pc)
            
            train_losses.append(total_loss.item())
            val_losses.append(val_loss_total.item())
            
            if epoch % 10 == 0:
                print(f"Epoch {epoch:03d} | Train: {total_loss.item():.4f} | Val: {val_loss_total.item():.4f}")
        
        print("\nTraining completed!")
        
        return {
            'train_losses': train_losses,
            'val_losses': val_losses,
            'final_train_loss': train_losses[-1],
            'final_val_loss': val_losses[-1]
        }