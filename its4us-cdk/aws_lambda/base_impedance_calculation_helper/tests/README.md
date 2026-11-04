# Impedance Calculation Tests

This directory contains tests for the impedance calculation Lambda functions.

## Test Structure

### base_impedance_calculation_helper/tests/ (11 tests)
Tests for base impedance calculation from sidewalk data and factors.csv:
- `test_base_travel_time_calculation`: Verifies base travel time = length / speed for each travel type
- `test_multiplicative_factors_applied`: Verifies MUL factors (width, slope, material) multiply correctly
- `test_additive_factors_applied`: Verifies ADD factors (ramps, defects) sum correctly (in seconds, not hours)
- `test_combined_multiplicative_and_additive_factors`: **Real-world scenario** - sidewalk with both MUL and ADD factors
- `test_multiple_ramps_on_same_link`: Tests aggregation of multiple ADD factors on same link
- `test_empty_sidewalk_data`: Edge case - handles empty input gracefully
- `test_constraint_boundaries_inbx`: Tests INBX constraint logic with boundary values
- `test_multiple_sidewalks_different_factors`: **Multi-link batch** - 3 sidewalks with different factor combinations
- `test_eq_constraint_for_numeric_values`: Tests EQ constraint on numeric values (width=0)
- `test_no_factor_matches_out_of_range`: Tests values outside all factor ranges (no factors applied)
- `test_null_and_missing_values`: Tests handling of null/missing attribute values

### calculate_impedance_helper/tests/ (3 tests)
Tests for applying Waze real-time data to base impedance:
- `test_waze_multiplicative_factors`: Verifies Waze MUL factors are grouped by link and multiplied
- `test_waze_additive_factors`: Verifies Waze ADD factors are grouped by link and summed
- `test_waze_combined_mul_and_add_factors`: **Real-world scenario** - applies both MUL and ADD Waze factors

## Running Tests

From `its4us-cdk/` directory with venv activated:

```bash
# Run all base impedance tests (7 tests)
pytest aws_lambda/base_impedance_calculation_helper/tests/ -v

# Run all Waze impedance tests (3 tests)
pytest aws_lambda/calculate_impedance_helper/tests/ -v

# Run specific test
pytest aws_lambda/base_impedance_calculation_helper/tests/test_calculate_impedance.py::TestBaseImpedanceCalculation::test_combined_multiplicative_and_additive_factors -v
```

## Test Data

Tests use fixtures defined in `fixtures/` directories:
- `constants.py`: PARAMS configuration matching production
- `factors_data.py`: Subset of factors.csv with representative rows
- `sidewalk_data.py`: Mock Neptune query results for sidewalk/ramp data
- `base_impedance_data.py`: Mock base impedance for Waze tests
- `waze_data.py`: Mock Waze alert data with MUL/ADD factors

## Test Coverage

✅ **Core calculations**: Base travel time, MUL factors, ADD factors  
✅ **Real-world scenarios**: Combined MUL+ADD factors for both sidewalk and Waze  
✅ **Edge cases**: Empty data, multiple ramps, boundary values  
✅ **Data aggregation**: Multiple ADD factors on same link

## Notes

- Tests mock Neptune database queries with `MagicMock`
- Tests call the **real** `calculate_impedance` functions (bug fixed: moved `apply_factors` definition before its first use)
- Tests follow the pattern from `import_waze` and `import_navigator` tests
- Focus is on business logic (calculations), not infrastructure (S3, CodePipeline)
- **All impedance values are in seconds**: Base travel time = (length_miles * 3600) / speed_mph, ADD factors in seconds
- Total: **14 tests** covering the most important calculation scenarios and edge cases
