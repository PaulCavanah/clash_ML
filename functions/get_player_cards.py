import requests

def get_player_cards(tag, TOKEN, feature_names) : 
    # Returns:
    # col_to_level and name_to_level 

    name_to_col = {name : col for col, name in enumerate(feature_names)}

    url = f"https://api.clashroyale.com/v1/players/{tag}" 
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers = headers)
    player_data  = r.json() # json -> dict

    col_to_level = {} # column index of input feature vector : card level
    name_to_level = {} # name from input feature vector : card level

    card_data = player_data["cards"]

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



