import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Constants
NA_VALUES = ["na", "Na", "N/a", "Not safe to measure"]


# Constants
NA_VALUES = ["na", "Na", "N/a", "Not safe to measure"]


def clean_na_values(df):
    """
    Replace various NA representations with np.nan.
    
    Modifies df in-place and returns it.
    """
    return df.replace(NA_VALUES, np.nan)


def normalize_link_id(series, PARAMS):
    """
    Normalize link ID series to string format.
    
    Returns normalized series.
    """
    return (
        series
        .astype(PARAMS["FLOAT_DATATYPE"])
        .fillna(0)
        .astype(PARAMS["INT_DATATYPE"])
        .astype(PARAMS["STR_DATATYPE"])
    )


def convert_bounds_to_float(factor_df, FACTORS_FIELDS, PARAMS):
    """
    Convert constraint bound columns to float64 for numeric comparisons.
    
    Returns converted DataFrame.
    """
    return factor_df.astype({
        FACTORS_FIELDS["Lower Constraint Bound"]: PARAMS["FLOAT64_DATATYPE"],
        FACTORS_FIELDS["Upper Constraint Bound"]: PARAMS["FLOAT64_DATATYPE"]
    })


def prepare_sidewalk_data(sidewalks, travelTypes, speeds, PARAMS):
    """
    Convert sidewalk records to DataFrame and calculate base travel time.
    
    Returns:
        tuple: (sidewalk_df, errors)
    """
    SIDEWALK_LINK_ATTRS = PARAMS["SIDEWALK_LINK_ATTRS"]
    errors = []
    
    sidewalk_dict = [record.data() for record in sidewalks]
    sidewalk_df = pd.DataFrame(sidewalk_dict)
    
    if sidewalk_df.empty:
        error_msg = "No sidewalk data available from Neptune query"
        logger.warning(error_msg)
        errors.append({"type": "EmptyDataWarning", "message": error_msg})
        return pd.DataFrame(), errors
    
    # Calculate base travel time in SECONDS
    # Formula: (length_miles * 3600_seconds_per_hour) / speed_mph = seconds
    sidewalk_df[travelTypes] = pd.DataFrame([
        sidewalk_df[SIDEWALK_LINK_ATTRS["stmAdaPathLinkLength"]].
        astype(PARAMS["FLOAT64_DATATYPE"]).values * 3600 
        for travelType in travelTypes
    ]).T
    sidewalk_df[travelTypes] = sidewalk_df[travelTypes].divide(speeds)
    
    # Normalize link IDs to strings
    sidewalk_df[SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"]] = normalize_link_id(
        sidewalk_df[SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"]], PARAMS
    )
    
    # Clean NA values
    sidewalk_df = clean_na_values(sidewalk_df)
    
    return sidewalk_df, errors


def prepare_factors(factors, PARAMS):
    """
    Split factors into MUL/ADD and enumerated/non-enumerated categories.
    
    Returns:
        dict: Dictionary with keys:
            - mul_enumerated
            - mul_nonenumerated
            - add_enumerated
            - add_nonenumerated
    """
    FACTORS_FIELDS = PARAMS["FACTORS_FIELDS"]
    
    mul_factors = factors[factors[FACTORS_FIELDS["Impedance Effect Type"]] == PARAMS["MUL"]]
    add_factors = factors[factors[FACTORS_FIELDS["Impedance Effect Type"]] == PARAMS["ADD"]]
    
    return {
        "mul_enumerated": mul_factors[mul_factors[FACTORS_FIELDS["Units"]] == FACTORS_FIELDS["Enumerated"]],
        "mul_nonenumerated": mul_factors[mul_factors[FACTORS_FIELDS["Units"]] != FACTORS_FIELDS["Enumerated"]],
        "add_enumerated": add_factors[add_factors[FACTORS_FIELDS["Units"]] == FACTORS_FIELDS["Enumerated"]],
        "add_nonenumerated": add_factors[add_factors[FACTORS_FIELDS["Units"]] != FACTORS_FIELDS["Enumerated"]]
    }


def apply_mul_enumerated_factors(sidewalk_df, factor_df, travelTypes, FACTORS_FIELDS):
    """
    Apply MUL enumerated factors (categorical lookups) using vectorized map.
    
    Modifies sidewalk_df in-place.
    """
    if factor_df.empty:
        return
    
    for var_name in factor_df[FACTORS_FIELDS["Variable Name"]].unique():
        if var_name not in sidewalk_df.columns:
            continue
        
        var_factors = factor_df[factor_df[FACTORS_FIELDS["Variable Name"]] == var_name]
        
        for travel_type in travelTypes:
            factor_lookup = dict(zip(
                var_factors[FACTORS_FIELDS["Enumeration"]],
                var_factors[travel_type]
            ))
            sidewalk_df[travel_type] = (
                sidewalk_df[travel_type] * 
                sidewalk_df[var_name].map(factor_lookup).fillna(1.0)
            )


def apply_mul_nonenumerated_factors(sidewalk_df, factor_df, travelTypes, PARAMS, FACTORS_FIELDS, errors):
    """
    Apply MUL non-enumerated factors (numeric ranges) using vectorized boolean masks.
    
    Modifies sidewalk_df in-place and appends to errors list.
    """
    if factor_df.empty:
        return
    
    for idx, factor_row in factor_df.iterrows():
        var_name = factor_row[FACTORS_FIELDS["Variable Name"]]
        if var_name not in sidewalk_df.columns:
            continue
        
        try:
            lower = float(factor_row[FACTORS_FIELDS["Lower Constraint Bound"]])
            upper = float(factor_row[FACTORS_FIELDS["Upper Constraint Bound"]])
            
            # Create boolean mask for rows where this factor applies
            mask = (
                sidewalk_df[var_name].notna() &
                (sidewalk_df[var_name].astype(PARAMS["FLOAT_DATATYPE"]) >= lower) &
                (sidewalk_df[var_name].astype(PARAMS["FLOAT_DATATYPE"]) <= upper)
            )
            
            # Apply factor to matching rows
            for travel_type in travelTypes:
                sidewalk_df.loc[mask, travel_type] = (
                    sidewalk_df.loc[mask, travel_type] * factor_row[travel_type]
                )
        
        except (ValueError, TypeError) as e:
            error_msg = f"Failed to apply MUL non-enumerated factor for {var_name}: {str(e)}"
            logger.error(error_msg)
            errors.append({
                "type": "FactorApplicationError",
                "message": error_msg,
                "variable": var_name,
                "factorType": "MUL_nonenumerated"
            })


def apply_add_enumerated_factors(df, factor_df, travelTypes, FACTORS_FIELDS):
    """
    Apply ADD enumerated factors (categorical lookups) using vectorized map.
    
    Modifies df in-place.
    """
    if factor_df.empty:
        return
    
    for var_name in factor_df[FACTORS_FIELDS["Variable Name"]].unique():
        if var_name not in df.columns:
            continue
        
        var_factors = factor_df[factor_df[FACTORS_FIELDS["Variable Name"]] == var_name]
        
        for travel_type in travelTypes:
            factor_lookup = dict(zip(
                var_factors[FACTORS_FIELDS["Enumeration"]],
                var_factors[travel_type]
            ))
            df[travel_type] = (
                df[travel_type] + 
                df[var_name].map(factor_lookup).fillna(0.0)
            )


def apply_add_nonenumerated_factors(df, factor_df, travelTypes, PARAMS, FACTORS_FIELDS, errors):
    """
    Apply ADD non-enumerated factors (numeric ranges) using vectorized boolean masks.
    
    Modifies df in-place and appends to errors list.
    """
    if factor_df.empty:
        return
    
    for idx, factor_row in factor_df.iterrows():
        var_name = factor_row[FACTORS_FIELDS["Variable Name"]]
        if var_name not in df.columns:
            continue
        
        try:
            lower = float(factor_row[FACTORS_FIELDS["Lower Constraint Bound"]])
            upper = float(factor_row[FACTORS_FIELDS["Upper Constraint Bound"]])
            
            # Create boolean mask for rows where this factor applies
            mask = (
                df[var_name].notna() &
                (df[var_name].astype(PARAMS["FLOAT_DATATYPE"]) >= lower) &
                (df[var_name].astype(PARAMS["FLOAT_DATATYPE"]) <= upper)
            )
            
            # Apply ADD factor to matching rows
            for travel_type in travelTypes:
                df.loc[mask, travel_type] = (
                    df.loc[mask, travel_type] + factor_row[travel_type]
                )
        
        except (ValueError, TypeError) as e:
            error_msg = f"Failed to apply ADD non-enumerated factor for {var_name}: {str(e)}"
            logger.error(error_msg)
            errors.append({
                "type": "FactorApplicationError",
                "message": error_msg,
                "variable": var_name,
                "factorType": "ADD_nonenumerated"
            })


def process_defects_and_ramps(impedance, defects_list, factor_dict, travelTypes, PARAMS, errors):
    """
    Process defects, ramps, curbs, etc. and apply ADD factors.
    
    Args:
        impedance: Current impedance DataFrame (will be modified)
        defects_list: List of defect datasets [defects, ramps, curbs, curbCuts, crossings, busStops]
        factor_dict: Dictionary with 'add_enumerated' and 'add_nonenumerated' keys
        
    Returns:
        Modified impedance DataFrame
    """
    SIDEWALK_LINK_ATTRS = PARAMS["SIDEWALK_LINK_ATTRS"]
    FACTORS_FIELDS = PARAMS["FACTORS_FIELDS"]
    
    for defect in defects_list:
        additional_attribute_dict = [record.data()["n"] for record in defect]
        
        if not additional_attribute_dict:
            continue
        
        additional_attribute_df = pd.DataFrame(additional_attribute_dict)
        
        # Clean NA values
        additional_attribute_df = clean_na_values(additional_attribute_df)
        
        # Initialize travel type columns
        additional_attribute_df[travelTypes] = 0.0
        
        # Set link ID
        additional_attribute_df[SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"]] = normalize_link_id(
            additional_attribute_df[SIDEWALK_LINK_ATTRS["stmAssetDefectReportPedLinkID"]], 
            PARAMS
        )
        
        # Filter factors to only those present in this defect type
        present_add_enumerated = factor_dict["add_enumerated"][
            factor_dict["add_enumerated"][FACTORS_FIELDS["Variable Name"]].isin(additional_attribute_df.columns)
        ]
        present_add_nonenumerated = factor_dict["add_nonenumerated"][
            factor_dict["add_nonenumerated"][FACTORS_FIELDS["Variable Name"]].isin(additional_attribute_df.columns)
        ]
        present_add_nonenumerated = convert_bounds_to_float(
            present_add_nonenumerated, FACTORS_FIELDS, PARAMS
        )
        
        # Apply ADD factors
        apply_add_enumerated_factors(
            additional_attribute_df, present_add_enumerated, 
            travelTypes, FACTORS_FIELDS
        )
        apply_add_nonenumerated_factors(
            additional_attribute_df, present_add_nonenumerated,
            travelTypes, PARAMS, FACTORS_FIELDS, errors
        )
        
        # Aggregate with main impedance by summing
        impedance = pd.concat([
            impedance, 
            additional_attribute_df[[SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"]] + travelTypes]
        ]).groupby([SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"]], as_index=False).sum()
    
    return impedance


def cleanup_node_ids(impedance, SIDEWALK_LINK_ATTRS):
    """
    Clean up node IDs using regex replacement.
    
    Modifies impedance DataFrame in-place.
    """
    pat = r"(n\w+)(n\w+)"
    repl = r"\2"
    impedance[SIDEWALK_LINK_ATTRS["ID(na)"]] = (
        impedance[SIDEWALK_LINK_ATTRS["ID(na)"]].str.replace(pat, repl, regex=True)
    )
    impedance[SIDEWALK_LINK_ATTRS["ID(nb)"]] = (
        impedance[SIDEWALK_LINK_ATTRS["ID(nb)"]].str.replace(pat, repl, regex=True)
    )


def calculate_impedance(PARAMS, factors, travelTypes, speeds, sidewalks, defects, ramps, curbs, 
                        curbCuts, crossings, busStops):
    """
    Calculate impedance for sidewalk links with factors applied.
    
    Returns:
        tuple: (impedance_df, errors_list)
    """
    SIDEWALK_LINK_ATTRS = PARAMS["SIDEWALK_LINK_ATTRS"]
    FACTORS_FIELDS = PARAMS["FACTORS_FIELDS"]
    
    try:
        # Step 1: Prepare sidewalk data and calculate base travel time
        sidewalk_df, errors = prepare_sidewalk_data(sidewalks, travelTypes, speeds, PARAMS)
        if sidewalk_df.empty:
            return pd.DataFrame(), errors
        
        # Step 2: Prepare factors (split into categories)
        factor_dict = prepare_factors(factors, PARAMS)
        
        # Filter factors to only those present in sidewalk data
        present_mul_enumerated = factor_dict["mul_enumerated"][
            factor_dict["mul_enumerated"][FACTORS_FIELDS["Variable Name"]].isin(sidewalk_df.columns)
        ]
        present_mul_nonenumerated = factor_dict["mul_nonenumerated"][
            factor_dict["mul_nonenumerated"][FACTORS_FIELDS["Variable Name"]].isin(sidewalk_df.columns)
        ]
        present_mul_nonenumerated = convert_bounds_to_float(
            present_mul_nonenumerated, FACTORS_FIELDS, PARAMS
        )
        
        # Step 3: Apply MUL factors (enumerated and non-enumerated)
        apply_mul_enumerated_factors(sidewalk_df, present_mul_enumerated, travelTypes, FACTORS_FIELDS)
        apply_mul_nonenumerated_factors(sidewalk_df, present_mul_nonenumerated, travelTypes, PARAMS, FACTORS_FIELDS, errors)
        
        impedance = sidewalk_df
        
        # Step 4: Process defects/ramps and apply ADD factors
        defects_list = [defects, ramps, curbs, curbCuts, crossings, busStops]
        impedance = process_defects_and_ramps(
            impedance, defects_list, factor_dict, travelTypes, PARAMS, errors
        )
        
        # Step 5: Clean up node IDs
        cleanup_node_ids(impedance, SIDEWALK_LINK_ATTRS)
        
        return impedance, errors
    
    except Exception as e:
        error_msg = f"Unexpected error during impedance calculation: {str(e)}"
        logger.exception(error_msg)
        errors = errors if 'errors' in locals() else []
        errors.append({
            "type": "UnexpectedError",
            "message": error_msg,
            "exception": type(e).__name__
        })
        return pd.DataFrame(), errors
