import requests

def get_all_cards(TOKEN, base_only = False) : 
    # Queries the API for all cards that exist in the game

    # Inputs: 
    # TOKEN - a str API token
    # base_only - a bool, if True, only returns base cards  

    # Returns: 
    # card_types - a dictionary of (card_id, evo_type) : card_name
    # OH_columns - a list of column names (e.g. Plr Evo Knight, Opp Electro Dragon)

    # Load card names from API 
    url = f"https://api.clashroyale.com/v1/cards"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers = headers)
    card_data = r.json()

    # Create a dict, where the key is a tuple (id, evo level) and
    # the value is the column name (e.g. "Evo Knight") 

    card_types = dict()

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

    # Create one-hot column names (for player and opponent) from the card types
    OH_columns = ["Plr " + card_name for card_name in card_types.values()] + ["Opp " + card_name for card_name in card_types.values()]

    return card_types, OH_columns


def get_player_cards(tag, TOKEN, feature_names) : 
    # Queries the API for which cards that a specific player has
    
    # Returns:
    # col_to_level - a dictionary of card column : level
    # name_to_level - a dictionary of card name : level 

    name_to_col = {name : col for col, name in enumerate(feature_names)}

    # Load player data from API
    url = f"https://api.clashroyale.com/v1/players/{tag}" 
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers = headers)
    player_data  = r.json() # json -> dict
    card_data = player_data["cards"]

    col_to_level = {} # column index of input feature vector : card level
    name_to_level = {} # name from input feature vector : card level

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

    return col_to_level, name_to_level

