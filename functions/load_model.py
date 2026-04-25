import pickle
import os 
from pathlib import Path
import torch

def load_nn_model(architecture, name) : 
    # load pytorch neural network model 
    # Returns model and feature_names

    features_path = os.getcwd() + f"/modeling/model_features/features_{name}.pkl"
    with open(features_path, "rb") as file :
        feature_names = pickle.load(file)

    state_path = Path(os.getcwd() + f"/modeling/model_states/{name}.pth")
    model = architecture(input_dim = len(feature_names)).to("cpu")
    state_dict = torch.load(state_path, weights_only = False)
    model.load_state_dict(state_dict())
    model.eval()

    return model, feature_names