# Generates a counter to the deck provided
#
# Very simple approach: load a bunch of decks, convert to playable decks, evaluate versus an
# opponent target deck, then choose top decks. 

#%%
# Set root dir as cwd
import os
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

#%%
# Load neural network model
from functions.load_model import load_nn_model 
from modeling.architectures import LogitSymmetric_256_128_64_1
architecture = LogitSymmetric_256_128_64_1

model_name = "NNsym_41M_12kranked"
model, feature_names = load_nn_model(architecture = architecture, name = model_name)

#%%
from functions.get_cards import get_player_cards
# Specify player tag and get available cards 

#player_tag = "%23V08PUV8GJ" # Me 

player_tag = "%23VC20G22LU" # Nxxx 

player_data = get_player_cards(player_tag, feature_names)
available_cards = player_data["col_to_level"]
print(player_data["name_to_level"])

#%%
# Load and preprocess decks for evaluation
from functions.load_data_from_parquet import load_player_unique_decks 
from functions.get_cards import convert_to_available_evohero_decks 
from functions.get_cards import swap_decks_format
import numpy as np

ladder_minimum = 12000
ranked_minimum = 0
filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , ladder_minimum)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", ranked_minimum)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", ranked_minimum)]]

# Load deck data - comes in (N, C) format
eval_decks, _, _ = load_player_unique_decks(game_lim = 1_000_000, filters = filters)

# Convert to (N, 8) format
eval_decks = swap_decks_format(eval_decks, feature_names)

# Get rid of duplicate decks
eval_decks = np.unique(eval_decks, axis = 0)

# Creates legal decks with available evos and heroes without changing the cards themselves. Rejects decks that have unavailable base cards. 
eval_decks = convert_to_available_evohero_decks(eval_decks, available_cards, feature_names)

# Convert back to (N, C) format
eval_decks = swap_decks_format(eval_decks, feature_names)

N, C = eval_decks.shape

print("Final # eval decks: ", N)

#%%
# Specify opponent target deck and build the evaluation matrix
import numpy as np

# Deck to be countered: 
opponent_deck = [
    "Plr Evo Witch",
    "Plr Golden Knight",
    "Plr Evo Royal Ghost",
    "Plr Fire Spirit",
    "Plr Goblin Hut",
    "Plr Goblin Barrel",
    "Plr Guards",
    "Plr Vines"
]

eval_matrix = np.zeros((N, 2*C)) # to be fed into the model

# Format the opponent deck information to put into the eval matrix
opponent_deck_indices = np.array([i for i, feature in enumerate(feature_names) if feature in opponent_deck])
opponent_deck_tile = np.tile(opponent_deck_indices, (N, 1)) # repeats deck N times to match the eval decks shape
eval_matrix[:, C:] = swap_decks_format(opponent_deck_tile, feature_names)

# Put the eval decks into the eval matrix
eval_matrix[:, :C] = eval_decks

#%%
# Evaluate the matchups 
import torch

# Convert eval matrix to tensor
eval_tensor = torch.tensor(eval_matrix, dtype = torch.float32)

eval_probs = torch.sigmoid(model(eval_tensor)).detach().numpy()

# %%
# Sort the matchup probabilities by descending and view the decks
sorted_i = np.argsort(eval_probs)[::-1]
sorted_evals = np.sort(eval_probs)[::-1]

# Worst decks: 
#sorted_i = np.argsort(eval_probs)
#sorted_evals = np.sort(eval_probs)

# View top k
k = 10
top_k_decks = swap_decks_format(eval_matrix[sorted_i[:k], :C], feature_names)
for ki in range(k) : 
    print(sorted_evals[ki], [feature_names[i] for i in top_k_decks[ki, :]])

# %%
