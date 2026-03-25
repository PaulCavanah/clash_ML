import os 
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import gc

def load_data_from_parquet(num_batches) : 
    # Load in X and Y data from the parquet files: 
    # Due to card updates, the schema evolves - parquet files may have different columns
    # The approach to merging these schemas is to load in each parquet file individually
    # with its unique one hot columns as a dataframe, add the dataframe to a list,
    # then concatenate the list of dataframes and fill the NaNs with false

    parquet_dir = Path(os.getcwd() + "/data/parquet")

    parquet_filenames = [filepath.name for filepath in parquet_dir.glob("*.parquet")][0:num_batches]

    dfs = []

    for filename in parquet_filenames : 
        pf = pq.ParquetFile(parquet_dir / filename)
        columns = pf.schema.names
        X_columns = [column for column in columns if column[0:3] in ("Plr", "Opp")]
        Y_columns = ["player_crowns", "opponent_crowns"]

        # only include ladder and ranked matches
        # filters = [[("gamemode", "==", "Ranked1v1_NewArena")],
        #             [("gamemode", "==", "Ladder")], 
        #             [("gamemode", "==", "Ranked1v1_NewArena2")]]
        filters = [[("gamemode", "==", "Ladder")]]

        df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = Y_columns + X_columns, filters = filters)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index = True)

    # Could expand memory bottleneck for systems with low RAM
    del dfs
    gc.collect()

    df.fillna(0, inplace = True)

    # X and Y
    X = df.iloc[:, 2:]
    y = df["player_crowns"] > df["opponent_crowns"]
    print("Loaded Data with shape:", f"X:{X.shape}, Y:{y.shape}" )

    # Could expand memory bottleneck for systems with low RAM
    del df
    gc.collect() 

    return X, y
