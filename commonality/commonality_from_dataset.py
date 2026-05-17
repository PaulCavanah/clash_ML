# Calculate combination code frequencies for 4-card combinations from a parent population of decks

#%%
# Set root dir as cwd
import os
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

X, y, feature_names, tags = load_onehot(game_lim = None, base_only = True, filters = filters, include_tags = True)

#X_decks, y, feature_names = load_player_unique_decks(game_lim = None, base_only = True, filters = filters)
# X_decks is of shape (N, C) where N is the number of decks and C is the number of cards

N_games = X.shape[0]
C = X.shape[1] // 2

# %%
# Get quad combination code frequencies
from itertools import combinations
from tqdm import tqdm
import pandas as pd

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

N = 0 # number of decks counted

# Process games in chunks, encoding the combinations as above and then counting them
chunk_size = 5_000_000 # Use however big of a size as long as it fits in memory

for start in tqdm(range(0, N_games, chunk_size), f"r = {r}") : 
    end = min(start + chunk_size, N_games)
    ccs = end-start # chunk size current (needed because the end chunk will usually be shorter than chunk_size)

    # First, get player unique decks from the chunk    
    X_cat = np.concatenate([X[start:end, :C], X[start:end, C:]], axis = 0) # Concatenate player and opponent
    tags_cat = np.concatenate([tags[start:end, 0], tags[start:end, 1]], axis = 0) # Concatenate player and opponent tags
    tags_with_decks = np.concatenate([np.reshape(tags_cat, (ccs*2, 1)), X_cat], axis = 1) # Concatenate tags with deck data
    nonunique_df = pd.DataFrame(tags_with_decks) # all games, formatted as pandas dataframe 
    unique_df = nonunique_df.drop_duplicates() # eliminate rows in which the player has the same deck (should remove player sampling bias)
    X_decks = unique_df.iloc[:, 1:] # Take tags out of the data (so it's only card data)
    
    N_chunk = X_decks.shape[0] # number of decks in chunk
    N += N_chunk 

    # Convert one-hot (N x C) to indices (N x 8). Importantly, np.where() outputs the indices as sorted for each row.
    deck_cards = np.reshape(np.where(X_decks > 0)[1], newshape = (N_chunk, 8))

    # Get all combinations in the deck chunk
    chunk_combos = deck_cards[:, deck_combos] # chunk_size x num_combos x r 

    # Encode each combination using the encoding scheme
    codes = np.sum([chunk_combos[:, :, i].astype(np.int64)*(C**i) for i in range(r)], axis = 0)

    # Counts the codes and adds them to the counts matrix
    counts_encoded += np.bincount(codes.ravel(), minlength = C**r) # bincount() counts the number of appearances of each code, .ravel() just flattens codes into 1d to be counted

#%%

print(f"Number of decks in parent sample: {N}")

# Use the same encoding scheme to get the codes from all possible combos
all_combo_codes = np.sum([all_combos[:, i]*(C**i) for i in range(r)], axis = 0)

# Get frequencies of each code 
encoded_frequencies = counts_encoded / N

zcpf = 2 # "zero-count punishing factor" - the greater this value, the more a deck is punished for having combinations that never occurred in the parent distribution
encoded_frequencies += 1/(C**(r+zcpf)) # avoid zeros with small floating point values less than the expected probability of achieving a combination by chance (assuming zcpf > 0)

#%%
# Get the 99.9% criterion by calculating commonality for random decks vs. parent decks 
# I.e. the point at which of all the samples greater than the criterion, 99.9% are observed decks (remaining are random decks)

from functions.get_random_deck import random_deck 

available_cards = range(len(feature_names))
num_decks = 100000

# Load randomly generated decks 
ran_d = random_deck(available_cards, feature_names, num_decks = num_decks, output_format = "indices")
ran_d = np.sort(ran_d, axis = 1) # sort to uphold encoding assumption of ascending card indices

# Load a random sample of parent population games and get their decks
rng = np.random.default_rng()
random_games = rng.choice(N_games, size = num_decks//2, replace = False) # num decks divided by 2 because player and opponent both have decks
obs_games = X[random_games, :] 
obs_decks_OH = np.concatenate([obs_games[:, :C], obs_games[:, C:]], axis = 0)
obs_d = np.reshape(np.where(obs_decks_OH > 0)[1], newshape = (obs_decks_OH.shape[0], 8))

# Calculate scores 
deck_combos = np.array(list(combinations(np.arange(deck_len), r))) # indices of each combination of r cards in a deck of 8 (e.g. (for pairs it's 8 choose 2 = 28))

# Get all combinations for each deck
ran_d_combos = ran_d[:, deck_combos] # num_decks x num_combos x r 
obs_d_combos = obs_d[:, deck_combos] # num_decks x num_combos x r

# Encode each deck's specific combinations as int < C^r (combination codes) using the encoding scheme 
ran_combo_codes = np.sum([ran_d_combos[:, :, i].astype(np.int64)*(C**i) for i in range(r)], axis = 0) # num_decks x num_combos
obs_combo_codes = np.sum([obs_d_combos[:, :, i].astype(np.int64)*(C**i) for i in range(r)], axis = 0) # num_decks x num_combos

# Lookup the frequencies of the combination codes and take the log and then sum across log combo freqs to get the score 
ran_commonality = np.sum(np.log(encoded_frequencies[ran_combo_codes]), axis = 1)
obs_commonality = np.sum(np.log(encoded_frequencies[obs_combo_codes]), axis = 1)

#%%

# Find the criterion between random and parent decks and calculate the loss of the criterion

all_samples = np.concatenate((ran_commonality, obs_commonality), axis = 0) 
labels = np.concatenate((1*np.ones((num_decks, ), dtype = np.uint8), 2*np.ones((num_decks, ), dtype = np.uint8))) # 1 = random, 2 = observed

# start criterion at median and move upwards (the target should always be in positive direction)
target = 0.999
criterion = np.median(all_samples)
prop_observed = np.mean(labels[np.argwhere(all_samples > criterion)] == 2)
step = (np.median(obs_commonality) - np.median(ran_commonality))/100 # Arbitrary

while prop_observed < target : 
    criterion += step 
    prop_observed = np.mean(labels[np.argwhere(all_samples > criterion)] == 2)

obs_loss = np.sum(obs_commonality < criterion)/num_decks
print(f"criterion: {criterion}, accuracy: {prop_observed}, loss: {obs_loss}")

# Plot results as histogram: 
import matplotlib.pyplot as plt

plt.hist(ran_commonality, bins = 30, label = "Random decks", color = "black", alpha = 0.5)
plt.hist(obs_commonality, bins = 30, label = "Ranked decks", color = "red", alpha = 0.5)
ymin, ymax = plt.ylim()
plt.vlines(criterion, ymin = ymin, ymax = ymax, color = "red", label = "99.9% criterion")
plt.xlabel('Commonality')
plt.ylabel('Count')
plt.title(f'Card commonality score, n = {num_decks} decks')
plt.legend(loc = "upper right")
plt.show()

#%%
# Pickle the quad combo code frequencies and the criterion
import pickle 
from pathlib import Path 
frequency_dir = Path(os.getcwd() + "/commonality/frequencies/")
frequency_dir.mkdir(parents = True, exist_ok = True)

data = {"criterion" : criterion, "encoded_frequencies" : encoded_frequencies}

filename = frequency_dir / f"r{r}_{parent_name}.pkl"
with open(filename, "wb") as f : 
    pickle.dump(data, f)


# %%
