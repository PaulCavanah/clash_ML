# Calculate combination code frequencies for 4-card combinations from a parent population of decks

#%%
import os

# Set root dir as cwd
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

#%% 
# Load parent dataset - only base cards and unique deck instances for each player
from functions.load_data_from_parquet import load_onehot, load_player_unique_decks
import numpy as np

parent_name = "ranked"

if parent_name == "midladder" : 
    ladder_minimum = 5000
    ladder_maximum = 10000
    filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">", ladder_minimum), ("player_trophies", "<", ladder_maximum)]]
elif parent_name == "ranked" : 
    ladder_minimum = 12000
    ranked_minimum = 0
    filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , ladder_minimum)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", ranked_minimum)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", ranked_minimum)]]

X_decks, y, feature_names = load_player_unique_decks(game_lim = None, base_only = True, filters = filters)
# X_decks is of shape (N, C) where N is the number of decks and C is the number of cards
N, C = X_decks.shape

# Convert one-hot (N x C) to indices (N x 8). Importantly, np.where() outputs the indices as sorted for each row.
deck_cards = np.reshape(np.where(X_decks > 0)[1], shape = (N, 8))

# %%
# Get quad combination code frequencies
from itertools import combinations, permutations
from tqdm import tqdm

r = 4 # combinations (e.g. r = 1 are singles, r = 2 are pairs, r = 3 are triplets, r = 4 are quads)
deck_len = 8 

deck_combos = np.array(list(combinations(np.arange(deck_len), r))) # indices of each combination of r cards in a deck of 8 (e.g. (for pairs it's 8 choose 2 = 28))
all_combos = np.array(list(combinations(np.arange(C), r))) # card numbers of each combination of r cards in the total number of available cards (e.g. for pairs it's 121 choose 2 = 7260)

# Encoding scheme: 
counts_encoded = np.zeros(C**r, dtype = np.int64)
# Such that 
# singles (a) is simply a*1
# Pairs(a, b) is a*C + b
# Triplets(a, b, c) is a*C*C + b*C + c
# etc... 
# Can be generalized as sum(i*(C**i) for i in range(r))
# No code can be greater than C**r, which is the length of the counts_encoded array (which is sparse)
# IMPORTANT: ENCODING ASSUMES THAT EACH COMBINATION IS IN ASCENDING ORDER (e.g. 12, 37, 43 and not 37, 43, 12). to fulfill
# this assumption, sort the deck matrix (as a row of 8 indices) in ascending order by columns/axis 1.

# Process decks in chunks, encoding the combinations as above and then counting them
chunk_size = 200_000
for start in tqdm(range(0, N, chunk_size), f"r = {r}") : 

    end = min(start + chunk_size, N)
    deck_cards_chunk = deck_cards[start:end, :]

    # Get all combinations in the deck chunk
    deck_chunk_combos = deck_cards_chunk[:, deck_combos] # chunk_size x num_combos x r 

    # Encode each combination using the encoding scheme
    codes = np.sum([deck_chunk_combos[:, :, i].astype(np.int64)*(C**i) for i in range(r)], axis = 0)

    # Counts the codes and adds them to the counts matrix
    counts_encoded += np.bincount(codes.ravel(), minlength = C**r) # bincount() counts the number of appearances of each code, .ravel() just flattens codes into 1d to be counted

# Use the same encoding scheme to get the codes from all possible combos
all_combo_codes = np.sum([all_combos[:, i]*(C**i) for i in range(r)], axis = 0)

# Get frequencies of each code 
encoded_frequencies = counts_encoded / N

zcpf = 2 # "zero-count punishing factor" - the greater this value, the more a deck is punished for having combinations that never occurred in the parent distribution
encoded_frequencies += 1/(C**(r+zcpf)) # avoid zeros with small floating point values less than the expected probability of achieving a combination by chance (assuming zcpf > 0)

#%%
# Pickle the quad combo code frequencies
import pickle 
from pathlib import Path 
frequency_dir = Path(os.getcwd() + "/commonality/frequencies/")
frequency_dir.mkdir(parents = True, exist_ok = True)

filename = frequency_dir / f"r{r}_{parent_name}.pkl"
with open(filename, "wb") as f : 
    pickle.dump(encoded_frequencies, f)

