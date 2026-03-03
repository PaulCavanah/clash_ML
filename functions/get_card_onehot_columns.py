def get_card_onehot_columns(TOKEN) : 
    import requests

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
        
        card_types[(id, 0)] = f"{name}"  #Add default no matter what

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