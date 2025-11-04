#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# extract_ranges.py
# -----------------
# Parse LC list file(s) and write 'sim_ranges.json' capturing:
#   - numeric min/max per column,
#   - enumerations for 'Gen' and 'Wind',
#   - discrete Wdir values (and min/max),
#   - parameter-key frequencies (continuation blocks),
#   - Turbfil numeric span & prefix counts,
#   - FAMILY_CODE mapping (wind family -> two-digit family code).

# Usage:
#   python extract_ranges.py file.set
#   python extract_ranges.py file1.set file2.set -o sim_ranges.json

import argparse, json, re
from collections import Counter

# Parameter keys seen in continuation blocks
PARAM_KEYS_CANON = {
    'time','gridf','idle','stop','mechbpar','vhub','pitchpar','pitch',
    'sgust','tycoff','ago','g0gm','azim0','ovp','profdat','dfac'
}

# Default FAMILY_CODE if not overridden; edit or pass via --family-code
DEFAULT_FAMILY_CODE = {
    'ntm':'11', 'etm':'13', 'eog1':'32', 'ecda':'14', 'ecdb':'14',
    'ewsha':'15','ewshb':'15','ewsvp':'15','ewsvn':'15',
    'edc1a':'33','edc1b':'33'
}
DEFAULT_FAMILY_CODE_FALLBACK = '21'  # used for unknown families

def parse_family_code_arg(argval: str):
    """
    Parse --family-code string like:
      ntm=11,etm=13,eog1=32,ecda=14,ecdb=14,ewsha=15,ewshb=15,ewsvp=15,ewsvn=15,edc1a=33,edc1b=33
    """
    mapping = {}
    for item in argval.split(','):
        item = item.strip()
        if not item: continue
        if '=' not in item: 
            raise ValueError(f"--family-code item without '=': {item}")
        k, v = item.split('=', 1)
        k, v = k.strip(), v.strip()
        if not k or not v or not v.isdigit() or len(v) not in (1,2):
            raise ValueError(f"Invalid family mapping '{item}'. Expect wind=NN (1-2 digits).")
        mapping[k] = v.zfill(2)
    return mapping

def is_lc_row(parts):
    if len(parts) < 11 or parts[0].lower() in PARAM_KEYS_CANON:
        return False
    try:
        float(parts[1]); float(parts[2]); int(float(parts[3])); float(parts[4])
        float(parts[5]); float(parts[6]); float(parts[7])
        return True
    except Exception:
        return False

def update_minmax(cur, val):
    lo, hi = cur
    if val < lo: lo = val
    if val > hi: hi = val
    return (lo, hi)

def parse_file(path, agg):
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    # find the table header "LC file ..."
    i = 0
    while i < len(lines) and not lines[i].lower().strip().startswith('lc file'):
        i += 1
    if i < len(lines): i += 1
    # skip unit lines like "[rpm] [m/s] [deg]"
    while i < len(lines) and lines[i].strip().startswith('['):
        i += 1

    for j in range(i, len(lines)):
        line = lines[j].strip()
        if not line:
            continue
        parts = line.split()
        if is_lc_row(parts):
            agg['ROWS'] += 1
            n_rot = float(parts[1])
            vhub  = float(parts[2])
            gen   = int(float(parts[3]))
            wdir  = float(parts[4])
            turb  = float(parts[5])
            vexp  = float(parts[6])
            rho   = float(parts[7])
            wind  = parts[8]
            turbfil = parts[9]

            agg['RANGES']['n_rot'] = update_minmax(agg['RANGES']['n_rot'], n_rot)
            agg['RANGES']['Vhub']  = update_minmax(agg['RANGES']['Vhub'], vhub)
            agg['RANGES']['Gen']   = update_minmax(agg['RANGES']['Gen'], gen)
            agg['RANGES']['Wdir']  = update_minmax(agg['RANGES']['Wdir'], wdir)
            agg['RANGES']['Turb']  = update_minmax(agg['RANGES']['Turb'], turb)
            agg['RANGES']['Vexp']  = update_minmax(agg['RANGES']['Vexp'], vexp)
            agg['RANGES']['rho']   = update_minmax(agg['RANGES']['rho'],  rho)

            agg['ENUMS']['Gen'].add(gen)
            agg['ENUMS']['Wind'].add(wind)
            agg['WDIR_values'].add(wdir)

            m = re.match(r'([A-Za-z]*)(\d+)([A-Za-z]*)$', turbfil)
            if m:
                pre, mid, suf = m.groups()
                agg['TURBFIL']['prefix_counts'][pre] += 1
                val = int(mid)
                lo, hi = agg['TURBFIL']['numeric_minmax']
                if lo is None or val < lo: lo = val
                if hi is None or val > hi: hi = val
                agg['TURBFIL']['numeric_minmax'] = (lo, hi)
        else:
            # parameter continuation line
            key = parts[0].lower()
            agg['PARAM_KEYS'][key] += 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('inputs', nargs='+', help='One or more LC .txt files to analyze')
    ap.add_argument('-o','--out', default='sim_ranges.json', help='Output JSON (default: sim_ranges.json)')
    ap.add_argument('--family-code', default=None,
                    help='Override wind->family code mapping, e.g. '
                         'ntm=11,etm=13,eog1=32,ecda=14,ecdb=14,ewsha=15,ewshb=15,ewsvp=15,ewsvn=15,edc1a=33,edc1b=33')
    ap.add_argument('--family-default', default=DEFAULT_FAMILY_CODE_FALLBACK,
                    help='Default two-digit family code for unknown winds (default: 21)')
    args = ap.parse_args()

    fam_code = dict(DEFAULT_FAMILY_CODE)
    if args.family_code:
        fam_code.update(parse_family_code_arg(args.family_code))
    family_default = str(args.family_default).zfill(2)

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
        'PARAM_KEYS': Counter(),
        'TURBFIL': {'prefix_counts': Counter(), 'numeric_minmax': (None, None)},
        'FILES': []
    }

    for p in args.inputs:
        agg['FILES'].append(p)
        parse_file(p, agg)

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
        # >>> NEW: FAMILY_CODE written to JSON <<<
        'FAMILY_CODE': fam_code,
        'FAMILY_CODE_DEFAULT': family_default
    }

    with open(args.out, 'w', encoding='utf-8') as w:
        json.dump(out, w, indent=2)
    print(f"Wrote {args.out}")

if __name__ == '__main__':
    main()