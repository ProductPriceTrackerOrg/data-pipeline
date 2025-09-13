"""
This file contains the model architecture definition and loading utilities.
It recreates the exact same ANN architecture used during training.
"""

import torch
import torch.nn as nn
from sentence_transformers import SentenceTransformer
import pickle
import os

class ProductCategoryANN(nn.Module):
    """
    Same ANN architecture as used in training.
    Must match exactly for proper model loading.
    """
    def __init__(self, input_dim=384, hidden1_dim=256, hidden2_dim=128, num_classes=28):
        super(ProductCategoryANN, self).__init__()
        
        # Define layers - exact same as training
        self.fc1 = nn.Linear(input_dim, hidden1_dim)
        self.relu1 = nn.ReLU()
        self.dropout1 = nn.Dropout(0.5)
        
        self.fc2 = nn.Linear(hidden1_dim, hidden2_dim)
        self.relu2 = nn.ReLU()
        self.dropout2 = nn.Dropout(0.5)
        
        self.fc3 = nn.Linear(hidden2_dim, num_classes)
    
    def forward(self, x):
        x = self.fc1(x)
        x = self.relu1(x)
        x = self.dropout1(x)
        
        x = self.fc2(x)
        x = self.relu2(x)
        x = self.dropout2(x)
        
        x = self.fc3(x)
        return x

class ModelLoader:
    """
    Handles loading of the trained model and required components.
    """
    def __init__(self, model_path='ann_category_classifier.pth'):
        self.model_path = model_path
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model = None
        self.sbert_model = None
        self.category_mapping = None
        
    def load_model(self):
        """
        Load the trained ANN model.
        """
        try:
            # Initialize model architecture
            self.model = ProductCategoryANN()
            
            # Load trained weights
            if os.path.exists(self.model_path):
                checkpoint = torch.load(self.model_path, map_location=self.device)
                self.model.load_state_dict(checkpoint)
                self.model.to(self.device)
                self.model.eval()  # Set to evaluation mode
                print(f"Model loaded successfully from {self.model_path}")
            else:
                raise FileNotFoundError(f"Model file not found: {self.model_path}")
                
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            raise
    
    def load_sbert_model(self):
        """
        Load the SBERT model for generating embeddings.
        """
        try:
            self.sbert_model = SentenceTransformer('all-MiniLM-L6-v2')
            print("SBERT model loaded successfully")
        except Exception as e:
            print(f"Error loading SBERT model: {str(e)}")
            raise
    
    def initialize_category_mapping(self):
        """
        Initialize the category ID to name mapping.
        This matches the label encoding from your training notebook.
        """
        self.category_mapping = {
            0: "Bags, Sleeves & Backpacks",
            1: "CPUs",
            2: "Cables & Adapters", 
            3: "Camera Accessories",
            4: "Cameras & Drones",
            5: "Car Accessories",
            6: "Cases & Screen Protectors",
            7: "Chargers & Power Banks",
            8: "Gaming Peripherals",
            9: "Graphic Cards",
            10: "Headphones & Earbuds",
            11: "Health & Personal Care Electronics",
            12: "Keyboards",
            13: "Laptops",
            14: "Memory",
            15: "Mice",
            16: "Mobile Phones",
            17: "Monitors",
            18: "Motherboards",
            19: "Networking",
            20: "Power Supplies & PC Cooling",
            21: "Printers & Scanners",
            22: "Smart Home & Office Accessories",
            23: "Smart Watches & Accessories",
            24: "Speakers",
            25: "Storage",
            26: "Tablets",
            27: "Webcams & Microphones"
        }
        print("Category mapping initialized")
    
    def initialize_all(self):
        """
        Initialize all required components.
        """
        print("Initializing prediction pipeline...")
        self.load_model()
        self.load_sbert_model() 
        self.initialize_category_mapping()
        print("Pipeline initialization complete!")