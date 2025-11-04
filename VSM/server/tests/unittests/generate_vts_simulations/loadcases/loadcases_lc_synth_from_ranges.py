#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# lc_synth_from_ranges.py
# -----------------------
# Generate a synthetic LC list using ONLY 'sim_ranges.json' (no access to original files).

# - Reads RANGES, ENUMS, WDIR, PARAM_KEYS, TURBFIL_PATTERN, FAMILY_CODE from JSON.
# - Creates meaningful header lines 1–4 (+ units line).
# - Randomizes LC IDs with pattern <NN><family><speed3><variant>, where
#   <family> comes from FAMILY_CODE in sim_ranges.json (fallback allowed).

import argparse, json, random, math, datetime, sys

SCENARIO_NN_RANGE = (10, 99)  # two-digit scenario code <NN>

def upick(seq, rnd): return rnd.choice(seq)
def ufloat(lo, hi, nd, rnd): return round(rnd.uniform(lo, hi), nd)

def load_ranges(path):
    with open(path, 'r', encoding='utf-8') as f:
        j = json.load(f)

    ranges = j['RANGES']
    enums  = j['ENUMS']
    wdir   = j['WDIR']
    pkeys  = j.get('PARAM_KEYS', {})
    tfp    = j.get('TURBFIL_PATTERN', {})
    fam    = j.get('FAMILY_CODE', {})
    fam_def= j.get('FAMILY_CODE_DEFAULT', '21')
    if not fam:
        print("[WARN] FAMILY_CODE not found in sim_ranges.json; using minimal fallback mapping.", file=sys.stderr)
        fam = {'ntm':'11'}  # bare minimum
    fam_def = str(fam_def).zfill(2)
    return ranges, enums, wdir, pkeys, tfp, fam, fam_def

def clamp_range(dct, key, fallback):
    lo = dct.get(key, {}).get('min', None)
    hi = dct.get(key, {}).get('max', None)
    if lo is None or hi is None or not (lo <= hi):
        lo, hi = fallback
    return float(lo), float(hi)

def synth_vlabel(v):
    if v < 5.5:  return "Vin"
    if v < 14.0: return "Vrat"
    if v < 26.0: return "Vout"
    if v < 40.0: return "V40"
    if v < 50.0: return "V45"
    return "V50"

def synth_header_lines(rnd, on_off=None):
    prep = f"Prep{rnd.randint(1,999):03d} v{datetime.date.today():%Y.%m.%d} Prep5 version number"
    vendor = upick(["V90-3.0MW","V112-3.3MW","V126-3.45MW","V136-3.45MW","V150-4.2MW"], rnd)
    series = upick(["VCS VG","Mk-III","Mk-IV","Plus","Smart"], rnd)
    edition = upick([2,3,4], rnd)
    iec_class = upick(["IEC1A","IEC1B","IEC2A","IEC2B","IEC3A"], rnd)
    title = f"{vendor} {series}, IEC61400-1 Edition {edition}, {iec_class} Title of calculation"
    site = on_off if on_off else upick(["onshore","offshore"], rnd)
    hdr = "LC file n_rot Vhub Gen Wdir Turb Vexp rho Wind Turbfil Title"
    units = "[rpm] [m/s] [deg]"
    return [prep, title, site, hdr, units]

def rand_core(ranges, enums, wdir, rnd):
    n_rot_lo, n_rot_hi = clamp_range(ranges, 'n_rot', (0.0, 20.0))
    vhub_lo, vhub_hi   = clamp_range(ranges, 'Vhub',  (4.0,  50.0))
    gen_vals  = enums.get('Gen', [0,2]) or [0,2]
    wind_vals = enums.get('Wind', ['ntm']) or ['ntm']

    wind = upick(wind_vals, rnd)
    vhub = ufloat(vhub_lo, vhub_hi, 2, rnd)
    nrot = ufloat(n_rot_lo, n_rot_hi, 1, rnd)
    gen  = upick(gen_vals, rnd)

    if wdir.get('values'):
        wdir_val = upick(wdir['values'], rnd)
    else:
        w_lo = wdir.get('min', -60.0); w_hi = wdir.get('max', 60.0)
        if w_lo is None or w_hi is None or w_lo > w_hi: w_lo, w_hi = -60.0, 60.0
        wdir_val = int(round(rnd.uniform(w_lo, w_hi)))

    turb_lo, turb_hi = clamp_range(ranges, 'Turb', (0.10, 0.35))
    vexp_lo, vexp_hi = clamp_range(ranges, 'Vexp', (0.10, 0.35))
    rho_lo,  rho_hi  = clamp_range(ranges, 'rho',  (1.15, 1.30))

    turb = ufloat(turb_lo, turb_hi, 3, rnd)
    vexp = ufloat(vexp_lo, vexp_hi, 3, rnd)
    rho  = ufloat(rho_lo,  rho_hi,  3, rnd)

    return dict(n_rot=nrot, vhub=vhub, gen=gen, wdir=wdir_val, turb=turb, vexp=vexp, rho=rho, wind=wind)

def synth_lc_id(i, wind, vhub, rnd, fam_map, fam_default):
    fam = fam_map.get(wind, fam_default)
    speed3  = int(round(vhub * 10))            # e.g., 13.2 m/s -> 132
    variant = chr(ord('a') + (i % 26))         # a..z
    nn      = rnd.randint(*SCENARIO_NN_RANGE)  # <NN>
    return f"{nn:02d}{fam}{speed3:03d}{variant}"

def synth_turbfil(i, tfp, rnd):
    nmin = tfp.get('numeric_min', 9000) or 9000
    nmax = tfp.get('numeric_max', 9050) or 9050
    if nmin > nmax: nmin, nmax = nmax, nmin
    num = rnd.randint(nmin, nmax)
    return f"{num}{chr(ord('a') + (i % 26))}"

def synth_title(wind, vhub, wdir, ranges, rnd):
    vlab = synth_vlabel(vhub)
    if wind in {'ntm','etm'}:
        v_min = int(ranges['Vhub']['min']) if ranges['Vhub'].get('min') is not None else 3
        v_max = int(ranges['Vhub']['max']) if ranges['Vhub'].get('max') is not None else 50
        lo = max(v_min, int(round(vhub))-1)
        hi = min(v_max, int(round(vhub))+1)
        return f"Prod. {lo}-{hi} m/s Wdir={int(wdir)}"
    if wind == 'eog1':           return f"Start {vlab} with EOG"
    if wind in {'ecda','ecdb'}:  return f"ECD at {vlab}"
    if wind in {'ewsha','ewshb'}:return f"Extreme wind shear, {vlab}"
    if wind in {'ewsvp','ewsvn'}:return f"Extreme vertical wind shear, {vlab}"
    if wind.startswith('edc1'):  return f"Start {vlab} with EDC"
    return f"Synthetic {wind.upper()} case at {vlab}"

# ----- continuation blocks -----

def rand_time_line(rnd, total_hint=None):
    dt = upick([0.01,0.02], rnd)
    total = total_hint if total_hint else round(rnd.uniform(80, 300), 1)
    t2 = int(rnd.uniform(20, max(25, total-10)))
    return f"time {dt} {total} 10 {t2}"

def rand_gridf(rnd):  return f"gridf {int(rnd.uniform(20,150))} 9999"
def rand_idle(rnd):   return f"idle {int(rnd.uniform(20,60))} 0 0"
def rand_stop(rnd):   return f"stop {int(rnd.uniform(20,60))}"
def mechbpar():       return "mechbpar 9999 9999 0 0"
def rand_sgust(rnd):  return f"sgust {upick([1,2,3,4], rnd)} {int(rnd.uniform(3,6))} 0 100"

def rand_vhub_traj(vhub, rnd):
    lines, t = [], 20
    nseg = upick([3,4,5], rnd)
    for _ in range(nseg):
        dv = round(rnd.uniform(-5, 5), 1)
        t0, t1 = t, t + upick([2,3,4], rnd)
        lines.append(f"vhub {round(vhub+dv,1)} {t0} {t1}")
        t = t1 + upick([14,18,22], rnd)
    return lines

def make_params(core, pkeys_freq, rnd):
    params = [rand_time_line(rnd)]
    wind = core['wind']
    if wind in {'eog1','ecda','ecdb','edc1a','edc1b'}:
        params += rand_vhub_traj(core['vhub'], rnd)
        params += [rand_sgust(rnd)]
    # bias from observed frequencies (if any)
    if pkeys_freq:
        total = sum(pkeys_freq.values()) or 1
        p_gridf = pkeys_freq.get('gridf', 0)/total
        p_idle  = pkeys_freq.get('idle', 0)/total
    else:
        p_gridf, p_idle = 0.5, 0.4
    if rnd.random() < max(0.2, min(0.9, p_gridf + 0.15)):
        params.append(rand_gridf(rnd))
    if rnd.random() < max(0.1, min(0.8, p_idle)):
        params.append(rand_idle(rnd))
    params.append(mechbpar())
    if rnd.random() < 0.2:
        params.append(rand_stop(rnd))
    return params

def write_synthetic_setfile(out_path, num, seed, sim_json):
    rnd = random.Random(seed)
    ranges, enums, wdir, pkeys, tfp, fam_map, fam_default = load_ranges(sim_json)
    header = synth_header_lines(rnd)
    loadcase_ids = []
    with open(out_path, 'w', encoding='utf-8') as w:
        for h in header: w.write(h + "\n")

        for i in range(num):
            core = rand_core(ranges, enums, wdir, rnd)
            lc_id   = synth_lc_id(i, core['wind'], core['vhub'], rnd, fam_map, fam_default)
            turbfil = synth_turbfil(i, tfp, rnd)
            title   = synth_title(core['wind'], core['vhub'], core['wdir'], ranges, rnd)

            main = (f"{lc_id} {core['n_rot']:.1f} {core['vhub']:.2f} {core['gen']:d} "
                    f"{int(core['wdir'])} {core['turb']:.3f} {core['vexp']:.3f} {core['rho']:.3f} "
                    f"{core['wind']} {turbfil} {title}")

            params = make_params(core, pkeys, rnd)
            if params:
                w.write(main + " >>\n")
                for pl in params:
                    w.write(pl + "\n")
            else:
                w.write(main + "\n")
            loadcase_ids.append(lc_id)
    return out_path, loadcase_ids

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-r','--ranges', default='sim_ranges.json', help='Path to sim_ranges.json')
    ap.add_argument('-n','--num', type=int, default=200, help='Number of synthetic LCs')
    ap.add_argument('-o','--out', default='synthetic_LCs.txt', help='Output .txt file')
    ap.add_argument('--seed', type=int, default=2025, help='Random seed')
    args = ap.parse_args()

    out,loadcase_ids = write_synthetic_setfile(args.out, args.num, args.seed, args.ranges)
    print(f"Wrote {out}")
    newline = '\n'
    print(f"Loadcase IDs: {newline.join(loadcase_ids)}")

if __name__ == '__main__':
    main()