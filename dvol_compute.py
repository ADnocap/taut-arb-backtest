"""Compute DVOL (30-day implied volatility index) from options snapshot data.

Uses VIX-style variance swap methodology (Carr-Madan model-free approach):
  1. Black-76 pricing for converting IV → option price
  2. Per-expiry variance via discrete strike integration
  3. Two-expiry interpolation to target 30-day constant maturity
"""

import math
from scipy.stats import norm


# ---------------------------------------------------------------------------
# Black-76 pricing
# ---------------------------------------------------------------------------

def black76_price(F: float, K: float, T: float, sigma: float,
                  option_type: str, r: float = 0.0) -> float:
    """Black-76 option price.

    Args:
        F: Forward price
        K: Strike price
        T: Time to expiry in years
        sigma: Implied volatility (decimal, e.g. 0.55)
        option_type: 'C' or 'P'
        r: Risk-free rate (default 0)

    Returns:
        Option price in same units as F.
    """
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        return 0.0

    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    discount = math.exp(-r * T)

    if option_type.upper() == "C":
        return discount * (F * norm.cdf(d1) - K * norm.cdf(d2))
    else:
        return discount * (K * norm.cdf(-d2) - F * norm.cdf(-d1))


# ---------------------------------------------------------------------------
# Per-expiry variance (Carr-Madan model-free)
# ---------------------------------------------------------------------------

def compute_expiry_variance(options: list[dict], T: float, F: float,
                            r: float = 0.0) -> tuple[float | None, int]:
    """Compute model-free variance for a single expiry using Carr-Madan formula.

    Args:
        options: List of dicts with keys: strike, option_type, mark_iv,
                 and optionally mark_price, underlying_price
        T: Time to expiry in years
        F: Forward price for this expiry
        r: Risk-free rate

    Returns:
        (variance, n_otm_strikes) or (None, 0) if insufficient data.
        n_otm_strikes: number of OTM strikes used (each side).
    """
    if T <= 0 or F <= 0 or not options:
        return None, 0

    # --- Step 1: Deduplicate by strike ---
    # Group all options by strike, keeping best IV per type
    by_strike: dict[float, dict[str, dict]] = {}
    for opt in options:
        K = opt["strike"]
        iv = opt.get("mark_iv", 0)
        if iv <= 0 or iv > 5.0:
            continue
        otype = opt.get("option_type", "").upper()
        if otype not in ("C", "P"):
            continue
        if K not in by_strike:
            by_strike[K] = {}
        # Keep the entry (if multiple of same type, last wins)
        by_strike[K][otype] = opt

    unique_strikes = sorted(by_strike.keys())
    if len(unique_strikes) < 3:
        return None, 0

    # --- Step 2: Find K0 (highest strike <= F) ---
    K0 = unique_strikes[0]
    for k in unique_strikes:
        if k <= F:
            K0 = k
        else:
            break

    # --- Step 3: Select OTM option per strike and price via Black-76 ---
    # For each unique strike, pick the OTM type and compute Q from IV
    selected: list[tuple[float, float]] = []  # (strike, iv)
    otm_put_count = 0
    otm_call_count = 0

    for K in unique_strikes:
        types_avail = by_strike[K]

        if K < K0:
            # OTM put preferred
            opt = types_avail.get("P") or types_avail.get("C")
            otm_put_count += 1
        elif K > K0:
            # OTM call preferred
            opt = types_avail.get("C") or types_avail.get("P")
            otm_call_count += 1
        else:
            # At K0: use both if available for straddle average
            opt = types_avail.get("P") or types_avail.get("C")

        if opt is None:
            continue
        iv = opt.get("mark_iv", 0)
        if iv <= 0 or iv > 5.0:
            continue
        selected.append((K, iv))

    min_otm = 3
    if otm_put_count < min_otm or otm_call_count < min_otm:
        return None, min(otm_put_count, otm_call_count)

    # --- Step 4: Compute contributions using Black-76 prices ---
    sel_strikes = [s[0] for s in selected]
    contributions = []

    for idx, (K, iv) in enumerate(selected):
        # Determine option type for Black-76 pricing
        if K < K0:
            Q = black76_price(F, K, T, iv, "P", r)
        elif K > K0:
            Q = black76_price(F, K, T, iv, "C", r)
        else:
            # At K0: average of put and call prices
            # Check if both types available for better straddle
            types_avail = by_strike[K]
            if "P" in types_avail and "C" in types_avail:
                iv_p = types_avail["P"].get("mark_iv", 0)
                iv_c = types_avail["C"].get("mark_iv", 0)
                if iv_p > 0 and iv_c > 0:
                    Q = (black76_price(F, K, T, iv_c, "C", r) +
                         black76_price(F, K, T, iv_p, "P", r)) / 2.0
                else:
                    Q = black76_price(F, K, T, iv, "P" if K <= F else "C", r)
            else:
                Q = (black76_price(F, K, T, iv, "C", r) +
                     black76_price(F, K, T, iv, "P", r)) / 2.0

        if Q <= 0:
            continue

        # Delta K on deduplicated strike list
        if idx == 0:
            dK = sel_strikes[1] - sel_strikes[0]
        elif idx == len(sel_strikes) - 1:
            dK = sel_strikes[-1] - sel_strikes[-2]
        else:
            dK = (sel_strikes[idx + 1] - sel_strikes[idx - 1]) / 2.0

        contributions.append((K, dK, Q))

    if len(contributions) < 6:
        return None, min(otm_put_count, otm_call_count)

    # sigma^2 = (2/T) * sum(dK_i / K_i^2 * Q(K_i)) - (1/T) * (F/K0 - 1)^2
    variance_sum = sum(dK / (K * K) * Q for K, dK, Q in contributions)
    variance = (2.0 / T) * variance_sum - (1.0 / T) * (F / K0 - 1.0) ** 2

    if variance <= 0:
        return None, min(otm_put_count, otm_call_count)

    return variance, min(otm_put_count, otm_call_count)


# ---------------------------------------------------------------------------
# Per-hour DVOL computation
# ---------------------------------------------------------------------------

T_TARGET = 30.0 / 365.25  # 30-day target maturity
T_MIN = 2.0 / 365.25      # Minimum 2 days
T_MAX = 90.0 / 365.25     # Maximum 90 days


def compute_dvol_at_hour(
    options_rows: list[dict],
    snapshot_hour_ms: int,
    spot_price: float,
    forward_prices: dict[str, float] | None = None,
) -> dict | None:
    """Compute DVOL for a single hourly snapshot.

    Args:
        options_rows: List of option dicts with keys:
            strike, expiry_date (ISO str), option_type, mark_iv,
            mark_price (optional), underlying_price (optional)
        snapshot_hour_ms: Snapshot hour in milliseconds
        spot_price: Current spot/index price
        forward_prices: Optional dict mapping expiry_date → forward price.
            If not provided, uses spot_price as forward proxy.

    Returns:
        Dict with {dvol, quality, near_expiry, far_expiry, n_near_strikes,
        n_far_strikes} or None if computation fails.
    """
    if not options_rows or spot_price <= 0:
        return None

    # Group options by expiry
    by_expiry: dict[str, list[dict]] = {}
    for opt in options_rows:
        exp = opt.get("expiry_date")
        if not exp:
            continue
        by_expiry.setdefault(exp, []).append(opt)

    # Compute T for each expiry
    expiry_data = []
    for exp_str, opts in by_expiry.items():
        T = _expiry_to_T(exp_str, snapshot_hour_ms)
        if T is None or T < T_MIN or T > T_MAX:
            continue
        # Forward price: from futures if available, else spot
        F = spot_price
        if forward_prices and exp_str in forward_prices:
            F = forward_prices[exp_str]

        expiry_data.append({
            "expiry": exp_str,
            "T": T,
            "F": F,
            "options": opts,
        })

    if len(expiry_data) < 2:
        return None

    # Sort by T
    expiry_data.sort(key=lambda x: x["T"])

    # Find two expiries bracketing T_TARGET
    near = None
    far = None
    for i in range(len(expiry_data) - 1):
        if expiry_data[i]["T"] <= T_TARGET <= expiry_data[i + 1]["T"]:
            near = expiry_data[i]
            far = expiry_data[i + 1]
            break

    # If T_TARGET not bracketed, use two closest
    if near is None or far is None:
        # T_TARGET is below all expiries or above all
        if T_TARGET <= expiry_data[0]["T"] and len(expiry_data) >= 2:
            near = expiry_data[0]
            far = expiry_data[1]
        elif T_TARGET >= expiry_data[-1]["T"] and len(expiry_data) >= 2:
            near = expiry_data[-2]
            far = expiry_data[-1]
        else:
            return None

    # Compute variance for each
    var_near, n_near = compute_expiry_variance(near["options"], near["T"], near["F"])
    var_far, n_far = compute_expiry_variance(far["options"], far["T"], far["F"])

    if var_near is None or var_far is None:
        return None

    # Interpolate: de-annualize → linear interpolate total variance → re-annualize
    T1 = near["T"]
    T2 = far["T"]
    total_var_1 = var_near * T1
    total_var_2 = var_far * T2

    if abs(T2 - T1) < 1e-10:
        # Same expiry — just use near
        var_30d = var_near
    else:
        # Linear interpolation of total variance
        w = (T_TARGET - T1) / (T2 - T1)
        # Clamp weight to [0, 1] for extrapolation cases
        w = max(0.0, min(1.0, w))
        total_var_target = total_var_1 + w * (total_var_2 - total_var_1)
        var_30d = total_var_target / T_TARGET

    if var_30d <= 0:
        return None

    dvol = math.sqrt(var_30d)

    # Quality assessment
    if n_near >= 5 and n_far >= 5:
        quality = "high"
    elif n_near >= 3 and n_far >= 3:
        quality = "medium"
    else:
        quality = "low"

    return {
        "dvol": dvol,
        "quality": quality,
        "near_expiry": near["expiry"],
        "far_expiry": far["expiry"],
        "n_near_strikes": n_near,
        "n_far_strikes": n_far,
    }


def _expiry_to_T(expiry_str: str, snapshot_ms: int) -> float | None:
    """Convert expiry ISO string to time-to-expiry in years."""
    import datetime as _dt
    try:
        # Parse ISO: '2025-09-25T08:00:00+00:00'
        exp_dt = _dt.datetime.fromisoformat(expiry_str)
        snap_dt = _dt.datetime.fromtimestamp(snapshot_ms / 1000, tz=_dt.timezone.utc)
        delta = (exp_dt - snap_dt).total_seconds()
        if delta <= 0:
            return None
        return delta / (365.25 * 24 * 3600)
    except (ValueError, TypeError):
        return None
