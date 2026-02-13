"""Volatility-of-Volatility (VoV) computation.

Computes:
  - Daily DVOL resampling from hourly data
  - 30-day rolling standard deviation of daily log-returns (annualized)
  - f_VoV scaling factor for tail probability corrections
"""

import math


def resample_dvol_daily(hourly_dvol: list[dict]) -> list[dict]:
    """Resample hourly DVOL to daily — last available close per UTC day.

    Args:
        hourly_dvol: List of dicts with keys: timestamp (ms), dvol (decimal).
            Must be sorted by timestamp ascending.

    Returns:
        List of dicts with keys: date (YYYY-MM-DD str), timestamp (ms of last
        hour in that day), dvol (last close of the day).
    """
    import datetime as _dt

    by_day: dict[str, dict] = {}
    for row in hourly_dvol:
        ts = row["timestamp"]
        dvol = row.get("dvol") or row.get("close")
        if dvol is None or dvol <= 0:
            continue
        dt = _dt.datetime.fromtimestamp(ts / 1000, tz=_dt.timezone.utc)
        day_key = dt.strftime("%Y-%m-%d")
        # Keep last (latest timestamp) per day
        if day_key not in by_day or ts > by_day[day_key]["timestamp"]:
            by_day[day_key] = {"date": day_key, "timestamp": ts, "dvol": dvol}

    return sorted(by_day.values(), key=lambda x: x["date"])


def compute_vov_series(daily_dvol: list[dict], window: int = 30) -> list[dict]:
    """Compute VoV time series from daily DVOL values.

    VoV_t = annualized 30-day rolling std of daily log-returns of DVOL.

    Args:
        daily_dvol: Sorted list of dicts with keys: date, timestamp, dvol.
        window: Rolling window size in days (default 30).

    Returns:
        List of dicts with keys: date, timestamp, dvol_daily, log_return,
        vov, f_vov. vov and f_vov are None when insufficient data.
    """
    if len(daily_dvol) < 2:
        return []

    # Compute log returns
    records = []
    for i in range(len(daily_dvol)):
        rec = {
            "date": daily_dvol[i]["date"],
            "timestamp": daily_dvol[i]["timestamp"],
            "dvol_daily": daily_dvol[i]["dvol"],
            "log_return": None,
            "vov": None,
            "f_vov": None,
        }
        if i > 0:
            prev_dvol = daily_dvol[i - 1]["dvol"]
            curr_dvol = daily_dvol[i]["dvol"]
            if prev_dvol > 0 and curr_dvol > 0:
                rec["log_return"] = math.log(curr_dvol / prev_dvol)
        records.append(rec)

    # Rolling std of log returns
    for i in range(len(records)):
        if i < window:
            continue

        # Gather log returns in window [i-window+1, i]
        returns = []
        for j in range(i - window + 1, i + 1):
            lr = records[j]["log_return"]
            if lr is not None:
                returns.append(lr)

        if len(returns) < 20:
            # Insufficient valid returns in window
            records[i]["vov"] = None
            records[i]["f_vov"] = 1.0
            continue

        # Standard deviation
        mean_r = sum(returns) / len(returns)
        var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
        std = math.sqrt(var) if var > 0 else 0.0

        # Annualize
        vov = std * math.sqrt(365.0)
        records[i]["vov"] = vov

    return records


def compute_vov_bar(vov_values: list[float]) -> float:
    """Full-sample mean of VoV values (excluding NaN/None).

    Args:
        vov_values: List of VoV values (may contain None).

    Returns:
        Mean VoV, or 1.0 if no valid values.
    """
    valid = [v for v in vov_values if v is not None and not math.isnan(v)]
    if not valid:
        return 1.0
    return sum(valid) / len(valid)


def compute_f_vov(vov_t: float | None, vov_bar: float,
                  alpha: float = 0.75) -> float:
    """Compute f_VoV scaling factor.

    f_VoV = min((VoV_t / VoV_bar)^alpha, 2.0)

    Args:
        vov_t: Current VoV value (None → 1.0)
        vov_bar: Long-run mean VoV
        alpha: Scaling exponent (default 0.75)

    Returns:
        f_VoV factor, capped at 2.0. Returns 1.0 if vov_t is None.
    """
    if vov_t is None or vov_bar <= 0:
        return 1.0
    ratio = vov_t / vov_bar
    return min(ratio ** alpha, 2.0)


def add_f_vov_to_series(records: list[dict], alpha: float = 0.75) -> list[dict]:
    """Compute VoV_bar from the series and add f_VoV to each record.

    Args:
        records: Output from compute_vov_series().
        alpha: Scaling exponent.

    Returns:
        Same records list with f_vov populated.
    """
    vov_bar = compute_vov_bar([r["vov"] for r in records])
    for rec in records:
        rec["f_vov"] = compute_f_vov(rec["vov"], vov_bar, alpha)
    return records
