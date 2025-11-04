#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Unit tests for loadcases_lc_synth_from_ranges.py
# Tests the synthetic loadcase file generator

import pytest
import os
import json
import tempfile
import uuid
from pathlib import Path
from loadcases_lc_synth_from_ranges import (
    write_synthetic_setfile,
    load_ranges,
    rand_core,
    synth_lc_id,
    synth_turbfil,
    synth_title,
    make_params,
    SCENARIO_NN_RANGE
)
import sys
import importlib.util

# Global test storage location
TEST_STORAGE_LOCATION = "/workspaces/simulation_management/VSM/io_dir_for_storage_test"


# Dynamically import loadcases_extract_ranges
def load_extract_ranges_module():
    """Dynamically load the extract_ranges module"""
    current_dir = Path(__file__).parent
    extract_ranges_path = current_dir / "loadcases_extract_ranges.py"
    spec = importlib.util.spec_from_file_location("loadcases_extract_ranges", extract_ranges_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sim_ranges_path():
    """Path to the sim_ranges.json file next to the generator script"""
    current_dir = Path(__file__).parent
    return str(current_dir / "sim_ranges.json")


@pytest.fixture
def test_output_dir():
    """Create a random simulation folder in TEST_STORAGE_LOCATION/vts_test_data"""
    # Use the global TEST_STORAGE_LOCATION constant
    base_dir = Path(TEST_STORAGE_LOCATION) / "vts_test_data"
    
    # Create random simulation folder name
    random_sim_folder = f"sim_{uuid.uuid4().hex[:8]}"
    test_dir = base_dir / random_sim_folder
    test_dir.mkdir(parents=True, exist_ok=True)
    
    yield test_dir
    
    # Cleanup: remove the test directory and its contents after test
    if test_dir.exists():
        for file in test_dir.iterdir():
            file.unlink()
        test_dir.rmdir()


@pytest.fixture
def random_set_filename():
    """Generate a random filename with .set extension"""
    return f"loadcase_{uuid.uuid4().hex[:8]}.set"


class TestLoadRanges:
    """Test loading and parsing sim_ranges.json"""
    
    def test_load_ranges_structure(self, sim_ranges_path):
        """Test that sim_ranges.json loads correctly and has expected structure"""
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        
        # Verify ranges exist and have expected keys
        assert 'n_rot' in ranges
        assert 'Vhub' in ranges
        assert 'Gen' in ranges
        assert 'Wdir' in ranges
        assert 'Turb' in ranges
        assert 'Vexp' in ranges
        assert 'rho' in ranges
        
        # Verify each range has min/max
        for key in ['n_rot', 'Vhub', 'Turb', 'Vexp', 'rho']:
            assert 'min' in ranges[key]
            assert 'max' in ranges[key]
            assert ranges[key]['min'] <= ranges[key]['max']
        
        # Verify enums
        assert 'Gen' in enums
        assert 'Wind' in enums
        assert isinstance(enums['Gen'], list)
        assert isinstance(enums['Wind'], list)
        
        # Verify wdir structure
        assert 'min' in wdir or 'values' in wdir
        
        # Verify turbfil pattern
        assert isinstance(tfp, dict)
        
        # Verify FAMILY_CODE and FAMILY_CODE_DEFAULT
        assert isinstance(fam_map, dict)
        assert len(fam_map) > 0, "FAMILY_CODE should not be empty"
        assert isinstance(fam_default, str)
        assert len(fam_default) == 2, "FAMILY_CODE_DEFAULT should be 2 digits"
        assert fam_default.isdigit(), "FAMILY_CODE_DEFAULT should be numeric"
    
    def test_load_ranges_family_code_structure(self, sim_ranges_path):
        """Test that FAMILY_CODE has expected wind type mappings"""
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        
        # Check that common wind types are mapped
        common_winds = ['ntm', 'etm', 'eog1', 'ecda', 'ecdb']
        for wind in common_winds:
            if wind in fam_map:
                code = fam_map[wind]
                assert isinstance(code, str), f"Family code for '{wind}' should be string"
                assert len(code) == 2, f"Family code for '{wind}' should be 2 characters"
                assert code.isdigit(), f"Family code for '{wind}' should be numeric"
    
    def test_load_ranges_family_code_values(self, sim_ranges_path):
        """Test that FAMILY_CODE values match expected codes"""
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        
        # Expected mappings based on the JSON file
        expected_mappings = {
            'ntm': '11',
            'etm': '13',
            'eog1': '32',
            'ecda': '14',
            'ecdb': '14',
            'ewsha': '15',
            'ewshb': '15',
            'ewsvp': '15',
            'ewsvn': '15',
            'edc1a': '33',
            'edc1b': '33'
        }
        
        for wind, expected_code in expected_mappings.items():
            if wind in fam_map:
                assert fam_map[wind] == expected_code, \
                    f"Wind '{wind}' should map to '{expected_code}', got '{fam_map[wind]}'"


class TestCoreGeneration:
    """Test core parameter generation"""
    
    def test_rand_core_returns_valid_params(self, sim_ranges_path):
        """Test that rand_core generates valid parameters within ranges"""
        import random
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(42)
        
        core = rand_core(ranges, enums, wdir, rnd)
        
        # Check all required keys exist
        assert 'n_rot' in core
        assert 'vhub' in core
        assert 'gen' in core
        assert 'wdir' in core
        assert 'turb' in core
        assert 'vexp' in core
        assert 'rho' in core
        assert 'wind' in core
        
        # Check values are within ranges
        assert ranges['n_rot']['min'] <= core['n_rot'] <= ranges['n_rot']['max']
        assert ranges['Vhub']['min'] <= core['vhub'] <= ranges['Vhub']['max']
        assert core['gen'] in enums['Gen']
        assert core['wind'] in enums['Wind']
        assert ranges['Turb']['min'] <= core['turb'] <= ranges['Turb']['max']
        assert ranges['Vexp']['min'] <= core['vexp'] <= ranges['Vexp']['max']
        assert ranges['rho']['min'] <= core['rho'] <= ranges['rho']['max']
    
    def test_rand_core_reproducibility(self, sim_ranges_path):
        """Test that rand_core is reproducible with same seed"""
        import random
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        
        rnd1 = random.Random(12345)
        core1 = rand_core(ranges, enums, wdir, rnd1)
        
        rnd2 = random.Random(12345)
        core2 = rand_core(ranges, enums, wdir, rnd2)
        
        assert core1 == core2


class TestLCIDGeneration:
    """Test loadcase ID generation"""
    
    def test_synth_lc_id_format(self, sim_ranges_path):
        """Test that LC IDs follow the expected pattern"""
        import random
        
        # Load ranges to get FAMILY_CODE
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(99)
        
        for i, wind in enumerate(['ntm', 'etm', 'eog1', 'ecda']):
            vhub = 13.5
            lc_id = synth_lc_id(i, wind, vhub, rnd, fam_map, fam_default)
            
            # Check format: <NN><family><speed3><variant>
            assert len(lc_id) >= 7  # minimum: 2+2+3+1
            # First two chars should be numeric (NN)
            assert lc_id[:2].isdigit()
            # Family code should match
            expected_family = fam_map.get(wind, fam_default)
            assert lc_id[2:4] == expected_family
            # Speed portion (3 digits)
            assert lc_id[4:7].isdigit()
            # Variant should be a letter
            assert lc_id[7].isalpha()
    
    def test_synth_lc_id_uses_family_code_from_json(self, sim_ranges_path):
        """Test that LC IDs use FAMILY_CODE from JSON file"""
        import random
        
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(42)
        
        # Verify FAMILY_CODE was loaded
        assert isinstance(fam_map, dict)
        assert len(fam_map) > 0
        
        # Test known wind types
        test_cases = {
            'ntm': '11',
            'etm': '13',
            'eog1': '32',
            'ecda': '14',
            'ewsha': '15'
        }
        
        for wind, expected_code in test_cases.items():
            if wind in fam_map:
                lc_id = synth_lc_id(0, wind, 12.5, rnd, fam_map, fam_default)
                assert lc_id[2:4] == expected_code, f"Wind '{wind}' should map to '{expected_code}'"
    
    def test_synth_lc_id_uses_default_for_unknown_wind(self, sim_ranges_path):
        """Test that unknown wind types use FAMILY_CODE_DEFAULT"""
        import random
        
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(77)
        
        # Verify default was loaded
        assert isinstance(fam_default, str)
        assert len(fam_default) == 2
        assert fam_default.isdigit()
        
        # Use an unknown wind type
        unknown_wind = 'xyz_unknown'
        lc_id = synth_lc_id(0, unknown_wind, 12.5, rnd, fam_map, fam_default)
        
        # Should use default
        assert lc_id[2:4] == fam_default


class TestTurbfilGeneration:
    """Test turbulence file name generation"""
    
    def test_synth_turbfil_format(self, sim_ranges_path):
        """Test that turbfil names follow expected pattern"""
        import random
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(77)
        
        turbfil = synth_turbfil(0, tfp, rnd)
        
        # Should be numeric followed by letter
        assert len(turbfil) >= 2
        assert turbfil[-1].isalpha()
        assert turbfil[:-1].isdigit()
        
        # Numeric part should be within range
        numeric_part = int(turbfil[:-1])
        if tfp.get('numeric_min') and tfp.get('numeric_max'):
            assert tfp['numeric_min'] <= numeric_part <= tfp['numeric_max']


class TestTitleGeneration:
    """Test loadcase title generation"""
    
    def test_synth_title_contains_relevant_info(self, sim_ranges_path):
        """Test that titles contain relevant wind scenario information"""
        import random
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(88)
        
        # Test NTM title
        title_ntm = synth_title('ntm', 12.5, 0, ranges, rnd)
        assert 'Prod' in title_ntm or 'm/s' in title_ntm
        
        # Test EOG title
        title_eog = synth_title('eog1', 12.5, 0, ranges, rnd)
        assert 'EOG' in title_eog or 'Start' in title_eog
        
        # Test ECD title
        title_ecd = synth_title('ecda', 12.5, 0, ranges, rnd)
        assert 'ECD' in title_ecd


class TestParamGeneration:
    """Test parameter block generation"""
    
    def test_make_params_returns_list(self, sim_ranges_path):
        """Test that make_params returns a valid list of parameter lines"""
        import random
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(55)
        
        core = rand_core(ranges, enums, wdir, rnd)
        params = make_params(core, pkeys, rnd)
        
        assert isinstance(params, list)
        assert len(params) > 0
        
        # Should contain at least time and mechbpar
        param_types = [p.split()[0] for p in params]
        assert 'time' in param_types
        assert 'mechbpar' in param_types
    
    def test_make_params_for_turbulent_scenarios(self, sim_ranges_path):
        """Test that turbulent scenarios get appropriate parameters"""
        import random
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        rnd = random.Random(66)
        
        # Test scenarios that should have vhub trajectories
        for wind in ['eog1', 'ecda', 'edc1a']:
            core = {
                'n_rot': 12.5,
                'vhub': 13.5,
                'gen': 2,
                'wdir': 0,
                'turb': 0.15,
                'vexp': 0.15,
                'rho': 1.225,
                'wind': wind
            }
            params = make_params(core, pkeys, rnd)
            param_str = ' '.join(params)
            
            # Should contain vhub or sgust for these scenarios
            assert 'vhub' in param_str or 'sgust' in param_str


class TestSyntheticFileGeneration:
    """Test complete synthetic file generation"""
    
    def test_write_synthetic_creates_file(self, sim_ranges_path, test_output_dir, random_set_filename):
        """Test that write_synthetic creates a valid file with expected structure"""
        output_file = test_output_dir / random_set_filename
        num_lcs = 50
        seed = 20251103
        
        result_path = write_synthetic_setfile(str(output_file), num_lcs, seed, sim_ranges_path)
        
        # Verify file was created
        assert os.path.exists(result_path)
        assert os.path.isfile(result_path)
        
        # Verify file has content
        assert os.path.getsize(result_path) > 0
        
        # Read and verify structure
        with open(result_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Should have header lines (5) plus loadcase content
        assert len(lines) >= 5
        
        # Check header structure
        assert 'Prep' in lines[0]
        assert 'IEC' in lines[1] or 'V' in lines[1]  # Vendor/IEC line
        assert 'onshore' in lines[2] or 'offshore' in lines[2]
        assert 'LC file' in lines[3] or 'n_rot' in lines[3]
        assert '[rpm]' in lines[4] or '[m/s]' in lines[4]
    
    def test_write_synthetic_generates_correct_number_of_lcs(self, sim_ranges_path, test_output_dir, random_set_filename):
        """Test that the correct number of loadcases are generated"""
        output_file = test_output_dir / random_set_filename
        num_lcs = 25
        seed = 12345
        
        write_synthetic_setfile(str(output_file), num_lcs, seed, sim_ranges_path)
        
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Count LC entries (look for pattern that starts LC lines)
        # Each LC should have a unique ID pattern
        import re
        # Match LC ID pattern: <NN><family><speed3><variant>
        lc_pattern = r'^\d{4}\d{3}[a-z]'
        lc_count = len(re.findall(lc_pattern, content, re.MULTILINE))
        
        assert lc_count == num_lcs
    
    def test_write_synthetic_reproducibility(self, sim_ranges_path, test_output_dir):
        """Test that same seed produces identical output"""
        output_file1 = test_output_dir / f"test1_{uuid.uuid4().hex[:8]}.set"
        output_file2 = test_output_dir / f"test2_{uuid.uuid4().hex[:8]}.set"
        
        num_lcs = 30
        seed = 99999
        
        write_synthetic_setfile(str(output_file1), num_lcs, seed, sim_ranges_path)
        write_synthetic_setfile(str(output_file2), num_lcs, seed, sim_ranges_path)
        
        # Read both files
        with open(output_file1, 'r', encoding='utf-8') as f:
            content1 = f.read()
        with open(output_file2, 'r', encoding='utf-8') as f:
            content2 = f.read()
        
        # Should be identical
        assert content1 == content2
        
        # Cleanup
        output_file1.unlink()
        output_file2.unlink()
    
    def test_write_synthetic_parameter_blocks(self, sim_ranges_path, test_output_dir, random_set_filename):
        """Test that parameter blocks (>>) are properly formatted"""
        output_file = test_output_dir / random_set_filename
        num_lcs = 20
        seed = 54321
        
        write_synthetic_setfile(str(output_file), num_lcs, seed, sim_ranges_path)
        
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Look for parameter continuation markers
        continuation_lines = [line for line in lines if '>>' in line]
        
        # Should have some parameter blocks
        assert len(continuation_lines) > 0
        
        # After each >> there should be parameter lines
        for i, line in enumerate(lines):
            if '>>' in line:
                # Next line should be a parameter (time, gridf, idle, etc.)
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    # Should be a known parameter type
                    assert any(next_line.startswith(p) for p in ['time', 'gridf', 'idle', 'mechbpar', 'vhub', 'sgust', 'stop'])
    
    def test_write_synthetic_validates_content(self, sim_ranges_path, test_output_dir, random_set_filename):
        """Test that generated content has valid values"""
        output_file = test_output_dir / random_set_filename
        num_lcs = 10
        seed = 11111
        
        write_synthetic_setfile(str(output_file), num_lcs, seed, sim_ranges_path)
        
        # Load ranges for validation
        ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_ranges_path)
        
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Skip header (first 5 lines)
        lc_lines = [line for line in lines[5:] if line.strip() and not line.strip().startswith(('time', 'gridf', 'idle', 'mechbpar', 'vhub', 'sgust', 'stop'))]
        
        for line in lc_lines:
            if '>>' in line:
                # Parse main LC line before >>
                parts = line.split('>>')[0].split()
                if len(parts) >= 9:
                    # Validate numeric values are within ranges
                    n_rot = float(parts[1])
                    vhub = float(parts[2])
                    gen = int(parts[3])
                    turb = float(parts[5])
                    vexp = float(parts[6])
                    rho = float(parts[7])
                    wind = parts[8]
                    
                    assert ranges['n_rot']['min'] <= n_rot <= ranges['n_rot']['max']
                    assert ranges['Vhub']['min'] <= vhub <= ranges['Vhub']['max']
                    assert gen in enums['Gen']
                    assert wind in enums['Wind']
                    assert ranges['Turb']['min'] <= turb <= ranges['Turb']['max']
                    assert ranges['Vexp']['min'] <= vexp <= ranges['Vexp']['max']
                    assert ranges['rho']['min'] <= rho <= ranges['rho']['max']


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_zero_loadcases(self, sim_ranges_path, test_output_dir, random_set_filename):
        """Test generating file with zero loadcases"""
        output_file = test_output_dir / random_set_filename
        
        write_synthetic_setfile(str(output_file), 0, 12345, sim_ranges_path)
        
        assert os.path.exists(output_file)
        
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Should have header only (5 lines)
        assert len(lines) == 5
    
    def test_large_number_of_loadcases(self, sim_ranges_path, test_output_dir, random_set_filename):
        """Test generating a large number of loadcases"""
        output_file = test_output_dir / random_set_filename
        num_lcs = 1000
        
        write_synthetic_setfile(str(output_file), num_lcs, 98765, sim_ranges_path)
        
        assert os.path.exists(output_file)
        
        # Verify file size is reasonable (should be substantial)
        file_size = os.path.getsize(output_file)
        assert file_size > 1000  # At least 1KB
    
    def test_different_seeds_produce_different_output(self, sim_ranges_path, test_output_dir):
        """Test that different seeds produce different outputs"""
        output_file1 = test_output_dir / f"seed1_{uuid.uuid4().hex[:8]}.set"
        output_file2 = test_output_dir / f"seed2_{uuid.uuid4().hex[:8]}.set"
        
        write_synthetic_setfile(str(output_file1), 50, 11111, sim_ranges_path)
        write_synthetic_setfile(str(output_file2), 50, 22222, sim_ranges_path)
        
        with open(output_file1, 'r', encoding='utf-8') as f:
            content1 = f.read()
        with open(output_file2, 'r', encoding='utf-8') as f:
            content2 = f.read()
        
        # Should be different
        assert content1 != content2
        
        # Cleanup
        output_file1.unlink()
        output_file2.unlink()


class TestExtractRangesIntegration:
    """Test integration between write_synthetic and extract_ranges"""
    
    def test_extract_ranges_can_read_generated_file(self, sim_ranges_path, test_output_dir):
        """Test that loadcases_extract_ranges.py can successfully parse a generated .set file"""
        # Generate a synthetic loadcase file
        output_file = test_output_dir / f"test_extract_{uuid.uuid4().hex[:8]}.set"
        num_lcs = 100
        seed = 77777
        
        write_synthetic_setfile(str(output_file), num_lcs, seed, sim_ranges_path)
        
        # Load the extract_ranges module
        extract_ranges = load_extract_ranges_module()
        
        # Create aggregation structure
        agg = {
            'ROWS': 0,
            'RANGES': {
                'n_rot': (float('inf'), float('-inf')),
                'Vhub':  (float('inf'), float('-inf')),
                'Gen':   (float('inf'), float('-inf')),
                'Wdir':  (float('inf'), float('-inf')),
                'Turb':  (float('inf'), float('-inf')),
                'Vexp':  (float('inf'), float('-inf')),
                'rho':   (float('inf'), float('-inf')),
            },
            'ENUMS': {
                'Gen': set(),
                'Wind': set(),
            },
            'WDIR_values': set(),
            'PARAM_KEYS': extract_ranges.Counter(),
            'TURBFIL': {
                'prefix_counts': extract_ranges.Counter(),
                'numeric_minmax': (None, None)
            },
            'FILES': []
        }
        
        # Parse the generated file
        extract_ranges.parse_file(str(output_file), agg)
        
        # Verify that parsing was successful
        assert agg['ROWS'] == num_lcs, f"Expected {num_lcs} rows, got {agg['ROWS']}"
        
        # Verify ranges were extracted
        assert agg['RANGES']['n_rot'][0] != float('inf')
        assert agg['RANGES']['Vhub'][0] != float('inf')
        assert agg['RANGES']['Turb'][0] != float('inf')
        
        # Verify enums were extracted
        assert len(agg['ENUMS']['Gen']) > 0
        assert len(agg['ENUMS']['Wind']) > 0
        
        # Verify parameter keys were found
        assert len(agg['PARAM_KEYS']) > 0
        assert 'time' in agg['PARAM_KEYS']
        assert 'mechbpar' in agg['PARAM_KEYS']
        
        # Cleanup
        output_file.unlink()
    
    def test_extract_ranges_output_matches_input_ranges(self, sim_ranges_path, test_output_dir):
        """Test that extracted ranges from generated file are within original ranges"""
        # Load original ranges
        original_ranges, original_enums, _, _, _, original_fam_map, original_fam_default = load_ranges(sim_ranges_path)
        
        # Generate a synthetic loadcase file
        output_file = test_output_dir / f"test_ranges_{uuid.uuid4().hex[:8]}.set"
        extracted_json = test_output_dir / f"extracted_{uuid.uuid4().hex[:8]}.json"
        num_lcs = 150
        seed = 88888
        
        write_synthetic_setfile(str(output_file), num_lcs, seed, sim_ranges_path)
        
        # Load the extract_ranges module
        extract_ranges = load_extract_ranges_module()
        
        # Create aggregation structure
        agg = {
            'ROWS': 0,
            'RANGES': {
                'n_rot': (float('inf'), float('-inf')),
                'Vhub':  (float('inf'), float('-inf')),
                'Gen':   (float('inf'), float('-inf')),
                'Wdir':  (float('inf'), float('-inf')),
                'Turb':  (float('inf'), float('-inf')),
                'Vexp':  (float('inf'), float('-inf')),
                'rho':   (float('inf'), float('-inf')),
            },
            'ENUMS': {
                'Gen': set(),
                'Wind': set(),
            },
            'WDIR_values': set(),
            'PARAM_KEYS': extract_ranges.Counter(),
            'TURBFIL': {
                'prefix_counts': extract_ranges.Counter(),
                'numeric_minmax': (None, None)
            },
            'FILES': [str(output_file)]
        }
        
        # Parse the generated file
        extract_ranges.parse_file(str(output_file), agg)
        
        # Verify extracted ranges are within original ranges
        assert agg['RANGES']['n_rot'][0] >= original_ranges['n_rot']['min']
        assert agg['RANGES']['n_rot'][1] <= original_ranges['n_rot']['max']
        
        assert agg['RANGES']['Vhub'][0] >= original_ranges['Vhub']['min']
        assert agg['RANGES']['Vhub'][1] <= original_ranges['Vhub']['max']
        
        assert agg['RANGES']['Turb'][0] >= original_ranges['Turb']['min']
        assert agg['RANGES']['Turb'][1] <= original_ranges['Turb']['max']
        
        assert agg['RANGES']['Vexp'][0] >= original_ranges['Vexp']['min']
        assert agg['RANGES']['Vexp'][1] <= original_ranges['Vexp']['max']
        
        assert agg['RANGES']['rho'][0] >= original_ranges['rho']['min']
        assert agg['RANGES']['rho'][1] <= original_ranges['rho']['max']
        
        # Verify extracted enums are subsets of original enums
        assert agg['ENUMS']['Gen'].issubset(set(original_enums['Gen']))
        assert agg['ENUMS']['Wind'].issubset(set(original_enums['Wind']))
        
        # Cleanup
        output_file.unlink()
        if extracted_json.exists():
            extracted_json.unlink()
    
    def test_roundtrip_generate_extract_generate(self, test_output_dir):
        """Test complete roundtrip: original ranges -> generate -> extract -> generate again"""
        # Get path to original sim_ranges.json
        current_dir = Path(__file__).parent
        original_ranges_path = str(current_dir / "sim_ranges.json")
        
        # Step 1: Generate first synthetic file from original ranges
        first_gen_file = test_output_dir / f"first_gen_{uuid.uuid4().hex[:8]}.set"
        write_synthetic_setfile(str(first_gen_file), 200, 99999, original_ranges_path)
        
        # Step 2: Extract ranges from first generated file
        extract_ranges = load_extract_ranges_module()
        extracted_json = test_output_dir / f"extracted_{uuid.uuid4().hex[:8]}.json"
        
        agg = {
            'ROWS': 0,
            'RANGES': {
                'n_rot': (float('inf'), float('-inf')),
                'Vhub':  (float('inf'), float('-inf')),
                'Gen':   (float('inf'), float('-inf')),
                'Wdir':  (float('inf'), float('-inf')),
                'Turb':  (float('inf'), float('-inf')),
                'Vexp':  (float('inf'), float('-inf')),
                'rho':   (float('inf'), float('-inf')),
            },
            'ENUMS': {
                'Gen': set(),
                'Wind': set(),
            },
            'WDIR_values': set(),
            'PARAM_KEYS': extract_ranges.Counter(),
            'TURBFIL': {
                'prefix_counts': extract_ranges.Counter(),
                'numeric_minmax': (None, None)
            },
            'FILES': [str(first_gen_file)]
        }
        
        extract_ranges.parse_file(str(first_gen_file), agg)
        
        # Save extracted ranges as JSON
        out = {
            'META': {
                'files': agg['FILES'],
                'rows': agg['ROWS']
            },
            'RANGES': {
                k: {'min': (None if v[0] == float('inf') else v[0]),
                    'max': (None if v[1] == float('-inf') else v[1])}
                for k, v in agg['RANGES'].items()
            },
            'ENUMS': {
                'Gen': sorted(agg['ENUMS']['Gen']),
                'Wind': sorted(agg['ENUMS']['Wind']),
            },
            'WDIR': {
                'min': (None if agg['RANGES']['Wdir'][0] == float('inf') else agg['RANGES']['Wdir'][0]),
                'max': (None if agg['RANGES']['Wdir'][1] == float('-inf') else agg['RANGES']['Wdir'][1]),
                'values': sorted(agg['WDIR_values'])
            },
            'PARAM_KEYS': dict(agg['PARAM_KEYS'].most_common()),
            'TURBFIL_PATTERN': {
                'prefix_counts': dict(agg['TURBFIL']['prefix_counts'].most_common()),
                'numeric_min': agg['TURBFIL']['numeric_minmax'][0],
                'numeric_max': agg['TURBFIL']['numeric_minmax'][1]
            }
        }
        
        with open(extracted_json, 'w', encoding='utf-8') as w:
            json.dump(out, w, indent=2)
        
        # Step 3: Generate second synthetic file from extracted ranges
        second_gen_file = test_output_dir / f"second_gen_{uuid.uuid4().hex[:8]}.set"
        # This should not raise an error
        write_synthetic_setfile(str(second_gen_file), 50, 11111, str(extracted_json))
        
        # Verify second generation succeeded
        assert os.path.exists(second_gen_file)
        assert os.path.getsize(second_gen_file) > 0
        
        # Verify content structure
        with open(second_gen_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        assert len(lines) >= 5  # Has header
        
        # Cleanup
        first_gen_file.unlink()
        second_gen_file.unlink()
        extracted_json.unlink()
    
    def test_extract_ranges_writes_family_code_to_json(self, sim_ranges_path, test_output_dir):
        """Test that extract_ranges properly writes FAMILY_CODE and FAMILY_CODE_DEFAULT to JSON"""
        # Generate a synthetic loadcase file
        output_file = test_output_dir / f"test_family_{uuid.uuid4().hex[:8]}.set"
        extracted_json = test_output_dir / f"extracted_family_{uuid.uuid4().hex[:8]}.json"
        
        write_synthetic_setfile(str(output_file), 100, 55555, sim_ranges_path)
        
        # Load the extract_ranges module
        extract_ranges = load_extract_ranges_module()
        
        # Create aggregation and extract
        agg = {
            'ROWS': 0,
            'RANGES': {
                'n_rot': (float('inf'), float('-inf')),
                'Vhub':  (float('inf'), float('-inf')),
                'Gen':   (float('inf'), float('-inf')),
                'Wdir':  (float('inf'), float('-inf')),
                'Turb':  (float('inf'), float('-inf')),
                'Vexp':  (float('inf'), float('-inf')),
                'rho':   (float('inf'), float('-inf')),
            },
            'ENUMS': {'Gen': set(), 'Wind': set()},
            'WDIR_values': set(),
            'PARAM_KEYS': extract_ranges.Counter(),
            'TURBFIL': {'prefix_counts': extract_ranges.Counter(), 'numeric_minmax': (None, None)},
            'FILES': [str(output_file)]
        }
        
        extract_ranges.parse_file(str(output_file), agg)
        
        # Build output JSON with FAMILY_CODE
        fam_code = dict(extract_ranges.DEFAULT_FAMILY_CODE)
        family_default = extract_ranges.DEFAULT_FAMILY_CODE_FALLBACK
        
        out = {
            'META': {'files': agg['FILES'], 'rows': agg['ROWS']},
            'RANGES': {
                k: {'min': (None if v[0] == float('inf') else v[0]),
                    'max': (None if v[1] == float('-inf') else v[1])}
                for k, v in agg['RANGES'].items()
            },
            'ENUMS': {
                'Gen': sorted(agg['ENUMS']['Gen']),
                'Wind': sorted(agg['ENUMS']['Wind']),
            },
            'WDIR': {
                'min': (None if agg['RANGES']['Wdir'][0] == float('inf') else agg['RANGES']['Wdir'][0]),
                'max': (None if agg['RANGES']['Wdir'][1] == float('-inf') else agg['RANGES']['Wdir'][1]),
                'values': sorted(agg['WDIR_values'])
            },
            'PARAM_KEYS': dict(agg['PARAM_KEYS'].most_common()),
            'TURBFIL_PATTERN': {
                'prefix_counts': dict(agg['TURBFIL']['prefix_counts'].most_common()),
                'numeric_min': agg['TURBFIL']['numeric_minmax'][0],
                'numeric_max': agg['TURBFIL']['numeric_minmax'][1]
            },
            'FAMILY_CODE': fam_code,
            'FAMILY_CODE_DEFAULT': family_default
        }
        
        # Write to JSON
        with open(extracted_json, 'w', encoding='utf-8') as w:
            json.dump(out, w, indent=2)
        
        # Verify the JSON file has FAMILY_CODE
        with open(extracted_json, 'r', encoding='utf-8') as f:
            loaded_json = json.load(f)
        
        assert 'FAMILY_CODE' in loaded_json
        assert 'FAMILY_CODE_DEFAULT' in loaded_json
        assert isinstance(loaded_json['FAMILY_CODE'], dict)
        assert len(loaded_json['FAMILY_CODE']) > 0
        assert isinstance(loaded_json['FAMILY_CODE_DEFAULT'], str)
        assert loaded_json['FAMILY_CODE_DEFAULT'] == '21'
        
        # Verify we can load it back with load_ranges
        re_ranges, re_enums, re_wdir, re_pkeys, re_tfp, re_fam_map, re_fam_default = load_ranges(str(extracted_json))
        
        assert isinstance(re_fam_map, dict)
        assert len(re_fam_map) > 0
        assert re_fam_default == '21'
        
        # Cleanup
        output_file.unlink()
        extracted_json.unlink()



if __name__ == '__main__':
    pytest.main([__file__, '-v'])
