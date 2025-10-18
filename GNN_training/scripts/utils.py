"""
Utility functions
"""
import torch
import random
import numpy as np

def set_random_seeds(seed: int = 42):
    """Set random seeds for reproducibility"""
    torch.manual_seed(seed)
    np.random.seed(seed)
    random.seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)

def generate_negative_edges(
    pos_edges: torch.Tensor, 
    num_src: int, 
    num_dst: int, 
    num_neg: int = None
) -> torch.Tensor:
    """Generate negative edges that don't exist in the graph"""
    if num_neg is None:
        num_neg = pos_edges.shape[1]
    
    existing = set()
    for i in range(pos_edges.shape[1]):
        existing.add((pos_edges[0, i].item(), pos_edges[1, i].item()))
    
    neg_edges = []
    attempts = 0
    max_attempts = num_neg * 10
    
    while len(neg_edges) < num_neg and attempts < max_attempts:
        src_idx = torch.randint(0, num_src, (1,)).item()
        dst_idx = torch.randint(0, num_dst, (1,)).item()
        
        if (src_idx, dst_idx) not in existing:
            neg_edges.append([src_idx, dst_idx])
        attempts += 1
    
    return torch.tensor(neg_edges, dtype=torch.long).t() if neg_edges else torch.empty((2, 0), dtype=torch.long)