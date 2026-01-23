import pandas as pd # pyright: ignore[reportMissingModuleSource]


def add_features(df: pd.DataFrame):
    """Adds a has_votes column to training data for training classifier."""
    
    df = df.copy()

    df['has_votes'] = (df['Share'] > 0).astype('int')

    return df