
# Given an input deck, find a deck that counters it well
# Initial approach: brute force search iterating over deck slots and cards, with multiple passes

# What to do now: 
# In summary - CONDENSE and modularize this process for future 
# Components right now: 
# 1. Imports
# 2. Get feature names from model
# 3. Get base, evos, hero/champion column ids from feature names
# 4. Get collisions of column ids 
# 5. Load model
# 6. Set player deck / column indices of player cards
# 7. Run algorithm
# 8. Evaluate result

# All of this is too jumbled 

# Ultimately, I should have a script or process where I just put in a deck with
# levels, and I choose from some options:  
# 1. choose a player tag and these are the cards to choose from
# 2. choose any cards
# 3. choose only base cards
# 4. etc... 

#%% 
# Imports and finding root dir
import torch
import torch.nn as nn
import torch.nn.functional as F
import os 
import pandas as pd
import numpy as np

# Set root dir as cwd
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

# Specify model architecture and name, then load model
from modeling.architectures import *
from functions.load_model import load_nn_model

architecture = LogitSymmetric_256_128_64_1
model_name = "NNsym_20M_ladder5k10k_pretrain"
model, feature_names = load_nn_model(architecture, model_name)

# Get available cards from a player tag
from functions.get_cards import get_player_cards
from functions.get_API_token import get_API_token

TOKEN = get_API_token()
#tag = "%2389QUL8YCQ" # Some random ladder player
tag = "%23G9YV9GR8R" # Mohamed Light
col_to_level, name_to_level = get_player_cards(tag, TOKEN, feature_names)
opp_half = len(feature_names) // 2 
available_cards = {col : level for col, level in col_to_level.items() if col >= opp_half} # as column ids 
# cards are only included on the opponent side

#%%
# Get the cards (as column ids) that will be used by the search algorithm
from functions.search_utilities import get_features_for_search
base, evos, heros, card_collisions = get_features_for_search(available_cards, feature_names)

#%%
# Set player input deck
# input_deck = {
# "Plr Evo Archers" : 1,
# "Plr Hero Knight" : 1,
# "Plr Evo Tesla" : 1,
# "Plr Electro Spirit" : 1,
# "Plr Skeletons" : 1,
# "Plr The Log" : 1,
# "Plr Fireball" : 1,
# "Plr X-Bow" : 1,
# }

input_deck = []

# Get column indices of player cards mapped to the levels
player_ind = {i : input_deck[feature] for i, feature in enumerate(feature_names) if feature in input_deck}

#%% 
# Set parameters for algorithm

num_cycles = 3
num_slots = 8 

#%% 
# Construct tensor for model input
input_size = len(feature_names)
tens_in = torch.tensor(np.zeros((1, input_size)), dtype = torch.float32) # tensor needs to be 2D to match architecture (which is trained on batches of data)
for card, level in player_ind.items() : 
    tens_in[0, card] = level 

cards_in_deck = np.zeros([num_slots], dtype = np.float32) # indices of cards currently in the deck after iteration and selection 
cards_in_deck[:] = np.nan 

slot_logit = np.zeros((num_slots, input_size), dtype = np.float32) # updated every slot/pass iteration
slot_logit[:] = np.nan 

#%%
# Run algorithm

for cycle in range(num_cycles) : 
    for slot in range(num_slots) : 
        if not np.isnan(cards_in_deck[slot]) : 
            tens_in[0, int(cards_in_deck[slot])] = 0 # current card removed in tensor during search

        # Includes cards already in the deck as well as evo/hero variants of these cards
        collisions = [collision for collisions in [card_collisions[int(card)] for card in cards_in_deck if not np.isnan(card)] for collision in collisions]

        # Base cards are always included in search: 
        cards_to_search = {base_card for base_card in base if base_card not in collisions}

        if slot == 0 or slot == 2 : # for evo slot, add evos to search 
            cards_to_search |= {evo_card for evo_card in evos if evo_card not in collisions}
        elif slot == 1 : # for hero/champion slot, add heros/champions to search
            cards_to_search |= {hero_card for hero_card in heros if hero_card not in collisions}

        for card in cards_to_search :
            #tens_in[0, card] = col_to_level[card] # set slot to card level
            tens_in[0, card] = 1
            slot_logit[slot, card] = model(tens_in) # evaluate
            tens_in[0, card] = 0 # empty slot when done
            
        cards_in_deck[slot] = np.nanargmin(slot_logit[slot, :])
        #name_collisions = [collision for collision in [feature_name_collisions[card] for card in cards_in_deck]]
        tens_in[0, int(cards_in_deck[slot])] = 1 # set current card in tensor to the minimum

        slot_logit[slot, :] = np.nan # reset logits for next cycle


#%% 
# Look at deck output
features_np = np.array(feature_names)
print("Player deck: ", input_deck)
print("Opponent deck: ", {feature_names[card] : col_to_level[card] for card in cards_in_deck.astype(np.int16)})

# %%
# Look at estimated win probability
eval_tens = torch.tensor(np.zeros((1, input_size)), dtype = torch.float32) # tensor needs to be 2D to match architecture (which is trained on batches of data)
for card, level in player_ind.items() : 
    eval_tens[0, card] = level 
for card in cards_in_deck : 
    eval_tens[0, int(card)] = col_to_level[int(card)]

print(F.sigmoid(model(eval_tens)))

# %%
print(cards_in_deck)

# %%
test_deck = [0, 1, 2]
test_dict = {0: [1, 2, 3], 1: [4, 5], 2: [6]}

print([card for cards in [test_dict[i] for i in test_deck] for card in cards])
# %%
test_dict = {"A" : 0, "B" : 1}
test_dict |= {"C" : 2, "D" : 3}
print(test_dict)

# %%
