"""
Save trained model and artifacts
"""
import torch
import os
from datetime import datetime

class ModelSaver:
    def __init__(self, models_dir: str = "models"):
        self.models_dir = models_dir
        os.makedirs(models_dir, exist_ok=True)
    
    def save_model(
        self,
        model,
        mappings: dict,
        training_results: dict,
        model_name: str = "personalized_gnn"
    ) -> str:
        """Save model, mappings, and training results"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_path = os.path.join(self.models_dir, f"{model_name}_{timestamp}_model.pth")
        data_path = os.path.join(self.models_dir, f"{model_name}_{timestamp}_data.pth")
        
        # Save model weights
        torch.save(model.state_dict(), model_path)
        
        # Save mappings and config
        save_data = {
            'product_mapping': mappings['product_mapping'],
            'user_mapping': mappings['user_mapping'],
            'brand_mapping': mappings['brand_mapping'],
            'category_mapping': mappings['category_mapping'],
            'shop_mapping': mappings['shop_mapping'],
            'model_config': {
                'num_users': len(mappings['user_mapping']),
                'num_brands': len(mappings['brand_mapping']),
                'num_categories': len(mappings['category_mapping']),
                'num_shops': len(mappings['shop_mapping']),
                'hidden_channels': 64,
                'out_channels': 32
            },
            'training_results': training_results,
            'timestamp': timestamp
        }
        
        torch.save(save_data, data_path)
        
        print(f"Model saved successfully:")
        print(f"  Model weights: {model_path}")
        print(f"  Mappings & config: {data_path}")
        
        # Also save as "latest" for easy loading
        latest_model_path = os.path.join(self.models_dir, f"{model_name}_latest_model.pth")
        latest_data_path = os.path.join(self.models_dir, f"{model_name}_latest_data.pth")
        
        torch.save(model.state_dict(), latest_model_path)
        torch.save(save_data, latest_data_path)
        
        return model_path