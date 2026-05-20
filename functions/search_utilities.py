import numpy as np

def mappings_from_available(available_cards, feature_names) : 
    # A function that takes in available_cards and outputs information about the mappings between these cards
    #
    # Input : 
    # available_cards - a list of the available cards (as column ids corresponding to feature_names, e.g. 167) or dict of available cards mapped to levels
    # feature_names - a list of all the card features (as str, e.g. "Plr Knight") 
    # Returns : 
    # dict with keys: 
    #   base - a list of available base card features as indices
    #   evos - a list of available evo card features as indices
    #   heroes - a list of available hero/champion card features as indices
    #   feature_collisions - a dictionary of card id : [card ids that collide with card id]
    #   base_to_evos - an ndarray of size (num_features, ) where the index is the base card and the value is the corresponding evo, otherwise 1000
    #   base_to_heroes - an ndarray of size (num_features, ) where the index is the base card and the value is the corresponding hero, otherwise 1000 
    #   all_to_base - an ndarray of size (num_features, ) where the index is the base/evo/hero card and the value is the corresponding base card, otherwise 1000

    available_cards = [i for i in available_cards if "Plr" in feature_names[i]] # only Plr columns matter here
    C = len(feature_names)//2 # number of possible features on one side

    nan_substitute = 1000 # needed to preserve the int datatype for the below, while still having an exceptional value (i.e. if card / card relation doesn't exist in an array)

    output = {
        "base" : [],
        "evos" : [],
        "heroes" : [], # including heroes and champions
        "feature_collisions" : {i : [] for i in available_cards}, # Note that feature collisions include the features themselves - this is a feature not a bug which usefully prevents duplicate of a card with itself during search (as well as with the evo/hero versions of itself)
        "base_to_evos" : nan_substitute*np.ones((C, ),  dtype = np.uint16), # [base] = evo
        "base_to_heroes" : nan_substitute*np.ones((C, ), dtype = np.uint16), # [base/hero] = hero
        "all_to_base" : nan_substitute*np.ones((C, ), dtype = np.uint16) # [base/evo/hero] = base
    }

    for i in available_cards : 
        feature = feature_names[i]

        feature_split = feature.split(" ") # e.g. ["Plr", "Hero", "Mega", "Minion"]

        if "Evo" in feature_split : 
            output["evos"].append(i)
            name_start = 2 #index of name start 
        elif "Hero" in feature_split : 
            output["heroes"].append(i)
            name_start = 2
        elif " ".join(feature_split[1:]) in ["Skeleton King", "Archer Queen", "Goblinstein", "Golden Knight", "Little Prince", "Mighty Miner", "Monk", "Boss Bandit"]: 
            output["heroes"].append(i)
            name_start = 1 
        else : 
            output["base"].append(i)
            name_start = 1

        base_name = " ".join(feature_split[name_start:]) # E.g. "Mega Minion"

        # Get collisions and cross-feature mappings for this feature
        for j in available_cards : 
            feature_j = feature_names[j]
            feature_split_j = feature_j.split(" ")[1:] # Gets rid of "Plr"         
            if ("Evo" in feature_split_j or "Hero" in feature_split_j) and " ".join(feature_split_j[1:]) == base_name and name_start == 1: # j is a evo/hero equivalent of i, which is a base card 
                output["feature_collisions"][i].append(j)
                output["all_to_base"][j] = i
                if "Evo" in feature_split_j : # j is an evo equivalent of i 
                    output["base_to_evos"][i] = j 
                elif "Hero" in feature_split_j : # j is the hero equivalent of i
                    output["base_to_heroes"][i] = j
            elif " ".join(feature_split_j) == base_name and name_start == 1: # j is the base card equivalent of i, which is a base card
                output["feature_collisions"][i].append(j)
                output["all_to_base"][i] = j
        
    return output