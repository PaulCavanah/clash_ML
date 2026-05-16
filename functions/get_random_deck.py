
import numpy as np 
from functions.search_utilities import get_features_for_search

def random_deck(available_cards, feature_names, num_decks = 1, output_format = "indices") : 
    # Generates one or more random decks. 
    # 
    # Input : 
    # available_cards: a list of indices of available cards corresponding to the feature_names 
    # feature_names: a list of str names of features (must be all features - player and opponent)
    # num_decks: the number of decks to randomly generate (default = 1)
    # output_format: str - options: 
    #   "indices" (default) - returns ndarray of card indices of shape (num_decks, 8)
    #   "OH half" - returns ndarray of one-hots of shape (num_decks, len(feature_names//2))
    #   "OH Plr" - returns ndarray of one-hots of shape (num_decks, len(feature_names)) where the deck is on the Plr side
    #   "OH Opp" - returns ndarray of one-hots of shape (num_decks, len(feature_names)) where the deck is on the Opp side
    # Returns : 
    # ndarray with deck information (see output_format)

    if output_format not in ["indices", "OH half", "OH Plr", "OH Opp"] : 
        Exception("Output must be of str: 'indices', 'OH half', 'OH Plr', 'OH Opp'")

    num_slots = 8 

    half = len(feature_names) // 2 

    if output_format == "OH Opp" : # uses opponent-side (Opp) card indices
        available_cards = [card for card in available_cards if card >= half]
        base, evos, heros, card_collisions = get_features_for_search(available_cards, feature_names)
    else : # in all other cases, uses player-side (Plr) card indices 
        available_cards = [card for card in available_cards if card < half]
        base, evos, heros, card_collisions = get_features_for_search(available_cards, feature_names)

    nan_substitute = 10000 # I want dtype of card ndarray to be int, so I'm using a nan substitute as 10000 (a value that card ids can never be) so I don't have to convert to float type just to use nans
    cards_in_deck = np.ones([num_decks, num_slots], dtype = np.uint16) * nan_substitute # indices of cards currently in the deck(s) after selection 

    if output_format == "OH half" : 
        OH_mat = np.zeros((num_decks, half), dtype = np.uint8)
    else : 
        OH_mat = np.zeros((num_decks, len(feature_names)), dtype = np.uint8)

    max_heros = 1  
    max_evos = 2 
    
    for deck in range(num_decks) : 
        num_heros = 0 ; 
        num_evos = 0 ; 
        for slot in range(num_slots) : 

            # Includes cards already in the deck as well as evo/hero variants of these cards
            collisions = [collision for collisions in [card_collisions[card] for card in cards_in_deck[deck, :] if card != nan_substitute] for collision in collisions]    

            cards_to_search = [base_card for base_card in base if base_card not in collisions]
            cards_to_search += [evo_card for evo_card in evos if evo_card not in collisions] # adds nothing if base only
            cards_to_search += [hero_card for hero_card in heros if hero_card not in collisions] # adds only champions if base only
            
            # Pick a random card until it meets base, evo, or hero requirement 
            while True : 
                cards_in_deck[deck, slot] = np.random.choice(cards_to_search)
                if cards_in_deck[deck, slot] in base : 
                    break 
                elif cards_in_deck[deck, slot] in evos and num_evos < max_evos : 
                    num_evos += 1 
                    break 
                elif cards_in_deck[deck, slot] in heros and num_heros < max_heros : 
                    num_heros += 1 
                    break 
        
        OH_mat[deck, cards_in_deck[deck, :]] = 1 

    if output_format == "indices" : 
        return cards_in_deck
    else : 
        return OH_mat
