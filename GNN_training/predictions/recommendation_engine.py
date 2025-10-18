"""
Generate recommendations from embeddings
"""
import torch
import torch.nn.functional as F
from typing import List, Dict

class RecommendationEngine:
    def __init__(self, embeddings, mappings, product_df, user_df):
        self.embeddings = embeddings
        self.mappings = mappings
        self.product_df = product_df
        self.user_df = user_df
        
        # Create reverse mappings
        self.reverse_product_mapping = {v: k for k, v in mappings['product_mapping'].items()}
        self.reverse_user_mapping = {v: k for k, v in mappings['user_mapping'].items()}
        
        # Create lookup dictionaries
        self.id_to_title = dict(zip(product_df['shop_product_id'], product_df['product_title_native']))
    
    def get_personalized_recommendations_for_all_users(self, top_k: int = 10) -> List[Dict]:
        """Generate personalized recommendations for all users"""
        
        print(f"Generating recommendations for {len(self.mappings['user_mapping'])} users...")
        
        all_recommendations = []
        
        for user_idx, user_id in self.reverse_user_mapping.items():
            user_embedding = self.embeddings['user'][user_idx].unsqueeze(0)
            
            # Calculate similarities with all products
            similarities = F.cosine_similarity(
                user_embedding,
                self.embeddings['product'],
                dim=1
            )
            
            # Get top-K
            top_similarities, top_indices = similarities.topk(min(top_k, len(similarities)))
            
            # Create recommendations
            for rank, (idx, similarity) in enumerate(zip(top_indices, top_similarities), 1):
                product_id = self.reverse_product_mapping[idx.item()]
                
                all_recommendations.append({
                    'user_id': str(user_id),
                    'recommended_variant_id': int(product_id),
                    'recommendation_score': float(similarity.item()),
                    'recommendation_type': 'personalized_gnn'
                })
        
        print(f"Generated {len(all_recommendations)} personalized recommendations")
        return all_recommendations
    
    def get_similar_products_for_all_products(self, top_k: int = 5) -> List[Dict]:
        """Generate similar product recommendations for all products"""
        
        print(f"Generating similar products for {len(self.mappings['product_mapping'])} products...")
        
        all_recommendations = []
        
        for source_idx, source_id in self.reverse_product_mapping.items():
            source_embedding = self.embeddings['product'][source_idx].unsqueeze(0)
            
            # Calculate similarities
            similarities = F.cosine_similarity(
                source_embedding,
                self.embeddings['product'],
                dim=1
            )
            
            # Exclude self
            similarities[source_idx] = -1.0
            
            # Get top-K
            top_similarities, top_indices = similarities.topk(min(top_k, len(similarities)))
            
            # Create recommendations
            for rank, (idx, similarity) in enumerate(zip(top_indices, top_similarities), 1):
                recommended_id = self.reverse_product_mapping[idx.item()]
                
                all_recommendations.append({
                    'source_shop_product_id': int(source_id),
                    'recommended_shop_product_id': int(recommended_id),
                    'recommendation_score': float(similarity.item()),
                    'recommendation_type': 'similar_gnn'
                })
        
        print(f"Generated {len(all_recommendations)} product recommendations")
        return all_recommendations
    
    def get_recommendations_for_user(self, user_id: int, top_k: int = 10) -> Dict:
        """Get recommendations for a single user"""
        
        if user_id not in self.mappings['user_mapping']:
            return {'error': f'User {user_id} not found'}
        
        user_idx = self.mappings['user_mapping'][user_id]
        user_embedding = self.embeddings['user'][user_idx].unsqueeze(0)
        
        similarities = F.cosine_similarity(
            user_embedding,
            self.embeddings['product'],
            dim=1
        )
        
        top_similarities, top_indices = similarities.topk(top_k)
        
        recommendations = []
        for rank, (idx, similarity) in enumerate(zip(top_indices, top_similarities), 1):
            product_id = self.reverse_product_mapping[idx.item()]
            recommendations.append({
                'rank': rank,
                'product_id': int(product_id),
                'product_title': self.id_to_title.get(product_id, 'Unknown'),
                'similarity_score': float(similarity.item())
            })
        
        return {
            'user_id': user_id,
            'recommendations': recommendations
        }
    
    def get_similar_products(self, product_id: int, top_k: int = 5) -> Dict:
        """Get similar products for a single product"""
        
        if product_id not in self.mappings['product_mapping']:
            return {'error': f'Product {product_id} not found'}
        
        product_idx = self.mappings['product_mapping'][product_id]
        product_embedding = self.embeddings['product'][product_idx].unsqueeze(0)
        
        similarities = F.cosine_similarity(
            product_embedding,
            self.embeddings['product'],
            dim=1
        )
        
        similarities[product_idx] = -1.0
        
        top_similarities, top_indices = similarities.topk(top_k)
        
        similar_products = []
        for rank, (idx, similarity) in enumerate(zip(top_indices, top_similarities), 1):
            similar_id = self.reverse_product_mapping[idx.item()]
            similar_products.append({
                'rank': rank,
                'product_id': int(similar_id),
                'product_title': self.id_to_title.get(similar_id, 'Unknown'),
                'similarity_score': float(similarity.item())
            })
        
        return {
            'query_product_id': product_id,
            'query_product_title': self.id_to_title.get(product_id, 'Unknown'),
            'similar_products': similar_products
        }