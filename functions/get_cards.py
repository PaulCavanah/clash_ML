import requests
import numpy as np
from functions.get_API_token import get_API_token
from functions.search_utilities import mappings_from_available 

def get_all_cards(base_only = False) : 
    # Queries the API for all cards that currently exist in the game

    # Inputs: 
    # base_only - a bool, if True, only returns base cards  

    # Returns: 
    # dict with keys: 
    #   "card_types" - a dictionary of (card_id, evo_type) : card_name
    #    "feature_names" - a list of column names (e.g. Plr Evo Knight, Opp Electro Dragon)

    output = {
        "card_types" : dict(), 
        "feature_names" : list()
    }

    TOKEN = get_API_token()

    # Load card names from API 
    url = f"https://api.clashroyale.com/v1/cards"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers = headers)
    card_data = r.json()

    # Create a dict, where the key is a tuple (id, evo level) and
    # the value is the column name (e.g. "Evo Knight") 

    card_types = dict() # (id, evohero) : card_name

    for card in card_data["items"] : 
        name = card["name"]
        id = card["id"]
        if "maxEvolutionLevel" in card : 
            evo_type = card["maxEvolutionLevel"]
        else : 
            evo_type = 0 #default
        
        card_types[(id, 0)] = f"{name}"  #Add base no matter what

        if not base_only : 
            if evo_type == 1 : # Evo available (but no hero)
                card_types[(id, 1)] = f"Evo {name}"
            elif evo_type == 2 : # Hero available (but no evo)
                card_types[(id, 2)] = f"Hero {name}"
            elif evo_type == 3 : # Both evo and hero available
                card_types[(id, 1)] = f"Evo {name}" 
                card_types[(id, 2)] = f"Hero {name}"
        
    output["card_types"] = card_types

    # Create one-hot column names (for player and opponent) from the card types
    output["feature_names"] = ["Plr " + card_name for card_name in card_types.values()] + ["Opp " + card_name for card_name in card_types.values()]

    return output

def get_player_cards(tag, feature_names = None) : 
    # Queries the API for which cards that a specific player has within feature_names (if given)
    
    # Returns:
    # dict with keys: 
    #   col_to_level - a dictionary of card column : level
    #   name_to_level - a dictionary of card name : level 
    #   feature_names - a list of feature_names 

    if not feature_names : 
        card_data = get_all_cards()
        feature_names = card_data["feature_names"]

    output = {
        "col_to_level" : dict(),
        "name_to_level" : dict(),
        "feature_names" : feature_names
    }

    TOKEN = get_API_token()

    name_to_col = {name : col for col, name in enumerate(feature_names)}

    # Load player data from API
    url = f"https://api.clashroyale.com/v1/players/{tag}" 
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers = headers)
    player_data  = r.json() # json -> dict
    card_data = player_data["cards"]

    col_to_level = dict()
    name_to_level = dict() 
  
    for card in card_data : 
        name = card["name"]
        level = card["level"] + (16 - card["maxLevel"])

        for side in ["Plr", "Opp"] :
            if f"{side} {name}" not in feature_names : 
                print(name, " not found, skipping")
                break

            # If player has evo, hero, or both, add them 
            if "maxEvolutionLevel" in card and "evolutionLevel" in card: 
                if card["evolutionLevel"] == 1 : # only Evo
                    col_to_level[name_to_col[f"{side} Evo {name}"]] = level
                    name_to_level[f"{side} Evo {name}"] = level
                elif card["evolutionLevel"] == 2 : # only Hero
                    col_to_level[name_to_col[f"{side} Hero {name}"]] = level
                    name_to_level[f"{side} Hero {name}"] = level
                elif card["evolutionLevel"] == 3 : # Both Evo and Hero
                    col_to_level[name_to_col[f"{side} Evo {name}"]] = level
                    name_to_level[f"{side} Evo {name}"] = level
                    col_to_level[name_to_col[f"{side} Hero {name}"]] = level
                    name_to_level[f"{side} Hero {name}"] = level

            #Add base card no matter what
            col_to_level[name_to_col[f"{side} {name}"]] = level 
            name_to_level[f"{side} {name}"] = level

        output["col_to_level"] = col_to_level
        output["name_to_level"] = name_to_level

    return output

def convert_to_available_evohero_decks(X, available_cards, feature_names) : 
    # Used to convert a matrix of decks in the wild to ones that a particular player could use
    # Inputs: 
    # X - a matrix of shape (N, 8) where N is the number of decks
    # available_cards - a list of card indices corresponding to feature_names
    # feature_names - a list of feature names which the data correspond to 
    # Returns:
    # X - a matrix of shape (F, 8) where F is the number of final decks

    # Get mappings about base, evos, and heroes from available cards
    card_mappings = mappings_from_available(available_cards, feature_names)

    # Convert all cards in the decks to base equivalents
    X = convert_to_base_equivalents(X, feature_names)

    # Identify and eliminate decks that have at least one base card that isn't available
    valid_decks = ~np.any(~np.isin(X, card_mappings["base"]), axis = 1)
    X = X[valid_decks, :]
        
    # Set legality for decks (regarding number of heroes and evos)
    hero_limit = 1
    evo_limit = 2

    # Heroes 
    # Get a mapping of base to base if no hero is available, and base to hero if it is available
    available_hero_mask = card_mappings["base_to_heroes"] != 1000 # 1000 = nan substitute
    base_to_heroes = card_mappings["base_to_heroes"]
    base_to_heroes[~available_hero_mask] = card_mappings["all_to_base"][~available_hero_mask]
    # Apply mapping to decks 
    X = base_to_heroes[X]
    # Get another mask which is True for where heroes are present
    hero_presence_mask = np.isin(X, card_mappings["heroes"])
    # Do cumulative sum along each row of the hero presence mask to get which hero is the first, the second, etc... in each row 
    hero_cumsum = np.cumsum(hero_presence_mask, axis = 1)
    # For those counts that are greater than the limit of heroes, replace with base 
    X[hero_cumsum > hero_limit] = card_mappings["all_to_base"][X[hero_cumsum > hero_limit]]

    # Evos 
    # Get a mapping of base to base if no evo is available, and base to evo if it is available 
    available_evo_mask = card_mappings["base_to_evos"] != 1000 # 1000 = nan substitute
    base_to_evos = card_mappings["base_to_evos"]
    base_to_evos[~available_evo_mask] = card_mappings["all_to_base"][~available_evo_mask] 
    # Combine with an identity mapping of hero to hero to protect the hero cards applied previously from being overwritten
    base_to_evos[card_mappings["heroes"]] = card_mappings["heroes"]
    # Apply mapping to decks 
    X = base_to_evos[X]
    # Get another mask which is True for where evos are 
    evo_presence_mask = np.isin(X, card_mappings["evos"])
    # Do cumulative sum along each row of the evo presence mask to get which evo is the first, the second, etc... 
    evo_cumsum = np.cumsum(evo_presence_mask, axis = 1)
    # For those counts that are greater than the limit of evos, replace with base 
    X[evo_cumsum > evo_limit] = card_mappings["all_to_base"][X[evo_cumsum > evo_limit]]


    return X 

def swap_decks_format(X, feature_names) : 
    # Convert format of input deck data into the opposite type
    # Input : 
    # X - deck data matrix of either size (N, 8) or (N, C)
    # feature_names - list of features corresponding to X 
    # Returns : 
    # The conversion of (N, 8) -> (N, C) or (N, C) -> (N, 8)

    N = X.shape[0]
    C = len(feature_names) // 2

    if X.shape == (N, 8) : # (N, 8) -> (N, C)
        X_new = np.zeros((N, C), dtype = np.uint8) # Onehot matrix
        rows, _ = np.indices(X.shape) # Use subscripting with (N, 8) card values as column indices
        X_new[rows, X] = 1 
        return X_new
    elif X.shape == (N, C) : # (N, C) -> (N, 8)
        return np.reshape(np.where(X)[1], (X.shape[0], 8))
    else : 
        Exception(f"Didn't recognize deck data shape: {X.shape} with features C = {C}")

def convert_to_base_equivalents(data, feature_names) :
    # Converts a deck or a data matrix into the base card equivalents

    # Inputs: 
    # data - in either shape (N, 8), (N, C), or (N, C*2)
    # feature_names - a list of feature names (includes Plr and Opp)

    # Returns: 
    # data_converted - matrix of same shape as input data, but only base cards

    # Get array of mappings from 
    card_relations = mappings_from_available(range(len(feature_names)), feature_names)

    card_to_base = card_relations["all_to_base"]
    
    # Convert to base equivalents, depending on the format of the input data
    N = data.shape[0] # number of decks/games
    C = len(feature_names) // 2 # number of card features (base, evos, and heros)

    if data.shape == (N, 8) : # deck data from a player formatted as indices 
        return card_to_base[data]
    
    elif data.shape == (N, C) : # deck data from a player formatted as one hot
        base_data = np.zeros(data.shape, dtype = np.uint8) # base card one-hot matrix equivalent of deck data
        rows, columns = np.where(data > 0) # get subscripts
        rows = rows.astype(np.uint64)
        columns = card_to_base[columns].astype(np.uint64) # convert column subscripts to base 
        base_data[rows, columns] = 1 # Assign base card equivalents to 1
        return base_data
    
    elif data.shape == (N, C*2) : #(N, C*2) - game data
        base_data = np.zeros(data.shape, dtype = np.uint8) # base card one-hot matrix equivalent of game data
        plr_rows, plr_columns = np.where(data[:, :C] > 0) # get subscripts
        plr_rows = plr_rows.astype(np.uint64)
        plr_columns = card_to_base[plr_columns].astype(np.uint64) # convert column subscripts to base card indices
        base_data[plr_rows, plr_columns] = 1 # Assign base card equivalents to 1 on plr side

        opp_rows, opp_columns = np.where(data[:, C:] > 0) # get subscripts
        opp_rows = opp_rows.astype(np.uint64)
        opp_columns = (card_to_base[opp_columns] + C).astype(np.uint64) # + C because opp columns are other half of features
        base_data[opp_rows, opp_columns] = 1 # Assign base card equivalents to 1 on opp side

        return base_data
    else : 
        Exception(f"Data in unexpected shape: {data.shape}. Expected {(N, 8)}, {(N, C)}, or {(N, C*2)}")


