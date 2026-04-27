

def get_features_for_search(available_cards, feature_names) : 
    # Input : 
    # available_cards - a list of the available cards (as column ids corresponding to feature_names, e.g. 172)
    # feature_names - a list of all the card features (as str, e.g. "Opp Knight") 
    # Returns : 
    # base - a dict of available base card features for search (as ids) : level
    # evos - a list of available evo card features for search (as ids) : level
    # heros - a list of available hero/champion card features for search (as ids) : level
    # feature_collisions - a dictionary of card id : [card ids that collide with card id]

    # column ids of available cards 
    base = dict()
    evos = dict()
    hero = dict() # includes heros and champions
    feature_collisions = {i : [] for i in available_cards} # column id : [column ids that collide]
    # Note that feature collisions include the features themselves - this is a feature not a bug which usefully prevents duplicate of a card with itself during search (as well as with the evo/hero versions of itself)

    for i in available_cards : # only opp columns matter here since player deck is constant for this algorithm
        feature = feature_names[i]

        feature_split = feature.split(" ") # e.g. ["Opp", "Hero", "Mega", "Minion"]

        if "Evo" in feature_split : 
            evos[i] = available_cards[i]
            name_start = 2 #index of name start 
        elif "Hero" in feature_split or " ".join(feature_split[1:]) in ["Skeleton King", "Archer Queen", "Goblinstein", "Golden Knight", "Little Prince", "Mighty Miner", "Monk", "Boss Bandit"]: 
            hero[i] = available_cards[i]
            name_start = 2
        else : 
            base[i] = available_cards[i]
            name_start = 1

        base_name = " ".join(feature_split[name_start:]) # E.g. "Mega Minion"

        # Get collisions for this feature
        for j in available_cards : 
            feature_j = feature_names[j]
            feature_split_j = feature_j.split(" ")[1:] # Get rid of "Opp" 
            if "Evo" in feature_split_j or "Hero" in feature_split_j :
                feature_split_j.pop(0) # Get rid of "Evo" or "Hero"
            if base_name == " ".join(feature_split_j) : # Only the base name remains
                feature_collisions[i].append(j)

    return base, evos, hero, feature_collisions