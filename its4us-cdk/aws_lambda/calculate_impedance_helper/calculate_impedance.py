import pandas as pd
import logging
from time import time

logger = logging.getLogger(__name__)

def calculate_impedance(PARAMS, travelTypes, baseImpedance, waze_df):

    WAZE_FIELDS = PARAMS["WAZE_FIELDS"]
    errors = []

    print("Creating factor tables:", time())

    base_impedance_dict = [record.data() for record in baseImpedance]
    base_impedance_df = pd.DataFrame(base_impedance_dict)
    
    if base_impedance_df.empty:
        error_msg = "No base impedance data available"
        logger.warning(error_msg)
        errors.append({"type": "EmptyDataWarning", "message": error_msg})
        return pd.DataFrame(), errors
    
    base_impedance_df[WAZE_FIELDS["stmAdaPathLinkID"]] \
        = base_impedance_df[WAZE_FIELDS["stmAdaPathLinkID"]].astype("int").astype("str")
    base_impedance_df[travelTypes] = base_impedance_df[travelTypes].astype("float")

    print("Creating waze tables:", time())

    if not waze_df.empty:
        waze_df[WAZE_FIELDS["stmAdaPathLinkID"]] \
            = waze_df[WAZE_FIELDS["stmAdaPathLinkID"]].astype("int").astype("str")
        waze_df[WAZE_FIELDS["factor"]] = waze_df[WAZE_FIELDS["factor"]].astype("float")

        # Broadcast factor across all travel types
        waze_df[travelTypes] = pd.DataFrame([waze_df[WAZE_FIELDS["factor"]].values for travelType in travelTypes]).T
        
        # Group by link: MUL uses product, ADD uses sum
        mul_waze = waze_df[waze_df[WAZE_FIELDS["type"]] == PARAMS["MUL"]]. \
            groupby(WAZE_FIELDS["stmAdaPathLinkID"], as_index = False).prod(numeric_only = True)
        add_waze = waze_df[waze_df[WAZE_FIELDS["type"]] == PARAMS["ADD"]]. \
            groupby(WAZE_FIELDS["stmAdaPathLinkID"], as_index = False).sum(numeric_only = True)
    else:
        mul_waze = pd.DataFrame(columns = [WAZE_FIELDS["stmAdaPathLinkID"]])
        add_waze = pd.DataFrame(columns = [WAZE_FIELDS["stmAdaPathLinkID"]])

    print("Applying waze factors (vectorized):", time())
    
    # ========== VECTORIZED WAZE FACTOR APPLICATION ==========
    
    # Apply MUL factors: merge and multiply
    if not mul_waze.empty:
        # Merge base impedance with MUL waze factors on link ID
        merged = base_impedance_df.merge(
            mul_waze[[WAZE_FIELDS["stmAdaPathLinkID"]] + travelTypes],
            on=WAZE_FIELDS["stmAdaPathLinkID"],
            how="left",
            suffixes=("", "_mul")
        )
        
        # Multiply each travel type by its MUL factor (fillna(1) for no Waze data)
        for travel_type in travelTypes:
            mul_col = f"{travel_type}_mul"
            if mul_col in merged.columns:
                base_impedance_df[travel_type] = merged[travel_type] * merged[mul_col].fillna(1.0)
    
    # Apply ADD factors: merge and add
    if not add_waze.empty:
        # Merge base impedance with ADD waze factors on link ID
        merged = base_impedance_df.merge(
            add_waze[[WAZE_FIELDS["stmAdaPathLinkID"]] + travelTypes],
            on=WAZE_FIELDS["stmAdaPathLinkID"],
            how="left",
            suffixes=("", "_add")
        )
        
        # Add each travel type's ADD factor (fillna(0) for no Waze data)
        for travel_type in travelTypes:
            add_col = f"{travel_type}_add"
            if add_col in merged.columns:
                base_impedance_df[travel_type] = base_impedance_df[travel_type] + merged[add_col].fillna(0.0)
    
    impedance = base_impedance_df

    return impedance, errors