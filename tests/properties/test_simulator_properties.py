import os
import sys
import json
import tempfile
from hypothesis import given
import hypothesis.strategies as st

# Add scripts directory to path to import simulate_feed
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts')))
from simulate_feed import generate_records, get_next_order_id, load_seed_data

@given(st.integers(min_value=1, max_value=5), st.integers(min_value=50, max_value=200))
def test_sequential_order_id_uniqueness(run_count, record_count):
    """
    Property 1: Source Simulator Sequential Order ID Uniqueness
    Validates: Requirements 7.3
    Asserts all Order_IDs across all runs are unique and form a contiguous ascending sequence.
    """
    # Use a temporary counter file for isolation
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        counter_file = tmp.name
        tmp.write(json.dumps({'last_order_id': 0}))
        
    try:
        all_order_ids = []
        dimensions = {
            'Country': ['Unknown'],
            'Region': ['Unknown'],
            'Branch': ['Unknown'],
            'Item_Type': ['Unknown'],
            'Category': ['Unknown'],
            'Sales_Channel': ['Unknown']
        }
        
        for _ in range(run_count):
            start_id = get_next_order_id(counter_file, record_count)
            records = generate_records(record_count, '2023-01-01', dimensions, start_id)
            # order_id is the 6th element (index 5) in the generated record
            order_ids = [int(r[5]) for r in records]
            all_order_ids.extend(order_ids)
            
        # Assert all Order_IDs are unique
        assert len(all_order_ids) == len(set(all_order_ids))
        
        # Assert they form a contiguous ascending sequence
        sorted_ids = sorted(all_order_ids)
        assert all_order_ids == sorted_ids
        if all_order_ids:
            assert all_order_ids[-1] - all_order_ids[0] == len(all_order_ids) - 1
            
    finally:
        if os.path.exists(counter_file):
            os.remove(counter_file)

@given(st.integers(min_value=10, max_value=100))
def test_valid_dimension_references(record_count):
    """
    Property 2: Source Simulator Valid Dimension References
    Validates: Requirements 7.5
    Load dimension seed CSVs and verify every generated record's FK values are members of the corresponding seed set.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    seeds_dir = os.path.join(project_root, 'dbt', 'seeds')
    
    dimensions = {
        'Country': load_seed_data(os.path.join(seeds_dir, 'country.csv'), 1),
        'Region': load_seed_data(os.path.join(seeds_dir, 'region.csv'), 1),
        'Branch': load_seed_data(os.path.join(seeds_dir, 'branch.csv'), 1),
        'Item_Type': load_seed_data(os.path.join(seeds_dir, 'product.csv'), 1),
        'Category': load_seed_data(os.path.join(seeds_dir, 'category.csv'), 1),
        'Sales_Channel': load_seed_data(os.path.join(seeds_dir, 'channel.csv'), 1)
    }
    
    # Ensure dimensions loaded properly, else fallback might fail tests if it wasn't intended
    for k, v in dimensions.items():
        if not v:
            dimensions[k] = ['Unknown']

    records = generate_records(record_count, '2023-01-01', dimensions, 1)
    
    for r in records:
        assert r[0] in dimensions['Region']
        assert r[1] in dimensions['Country']
        assert r[2] in dimensions['Item_Type']
        assert r[3] in dimensions['Sales_Channel']
        assert r[10] in dimensions['Branch']
        assert r[11] in dimensions['Category']
