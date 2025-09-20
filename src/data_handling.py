import pandas as pd
from typing import Dict

# Parameters

HURRICANE_THRESHOLD = 32.6 #m/s

def hurricane_events(df: pd.DataFrame, wind_col: str) -> pd.DataFrame:
    """
    Discard any adjacent days that match our criteria for a hurricane event.
    Function takes the entire dataset, sorts it for the "wind column", and loops over to test if max gust exceeds our hurricane threshold.
    """

    if df.empty:
        return pd.DataFrame(columns=["year", "events"])

    # Ensure datetime & sort
    d = df[["time", wind_col]].copy()
    d["time"] = pd.to_datetime(d["time"], utc=True)
    d = d.dropna(subset=[wind_col]).sort_values("time")

    # Reindex to a complete daily range so missing days break runs
    daily = d.set_index("time")[wind_col].asfreq("D")  # keeps NaN for missing days

    in_event = False
    counts: Dict[int, int] = {}
    for day, val in daily.items():
        above = pd.notna(val) and (float(val) > HURRICANE_THRESHOLD)
        if above and not in_event:
            counts[day.year] = counts.get(day.year, 0) + 1
            in_event = True
        elif not above:
            in_event = False

    if not counts:
        return pd.DataFrame(columns=["year", "events"])
    return pd.DataFrame(sorted(counts.items()), columns=["year", "events"])
