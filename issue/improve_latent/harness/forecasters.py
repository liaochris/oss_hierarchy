"""Pre-period-OUTCOMES-only forecasters for the post-period latent opens path (issue/improve_latent).
Each takes the pre-period opens series y (event-time ordered, oldest->newest), horizon h, and the
pooled parameters fitted once across all repos' pre-periods, and returns h post-period forecast levels.

These are the methods whose per-org input is the org's OWN pre-series (the reversion magnitude, if any,
must be inferable from that series or from a pooled level/variance object -- NOT from the control
group's post-period). Contrast with the cross-fitted methods in fit_cv_forecasters.py, which learn the
pooled reversion CURVE from control post-periods. Parameters are ESTIMATED (method-of-moments for the
shrinkage reliability weight; pooled MLE-by-SSE for the damped-trend smoothing constants), never tuned.
"""
import numpy as np
from scipy.optimize import minimize


def FitPooledParameters(pre_series_by_repo):
    series = [np.asarray(y, float) for y in pre_series_by_repo.values() if len(y) >= 2]
    pre_means = np.array([y.mean() for y in series])
    within_var = np.array([y.var(ddof=1) / len(y) for y in series])   # sampling var of the pre-mean
    grand_mean = float(pre_means.mean())
    # method-of-moments between-unit (signal) variance = cross-sectional dispersion minus mean sampling noise
    sigma2_between = max(float(pre_means.var(ddof=1) - within_var.mean()), 0.0)
    alpha, beta, phi = FitPooledDampedTrend(series)
    return {"grand_mean": grand_mean, "sigma2_between": sigma2_between,
            "alpha": alpha, "beta": beta, "phi": phi}


def DampedTrendOneStepSSE(params, series):
    alpha, beta, phi = params
    total = 0.0
    for y in series:
        if len(y) < 3:
            continue
        level, slope = y[0], y[1] - y[0]
        for t in range(1, len(y)):
            forecast = level + phi * slope
            error = y[t] - forecast
            total += error * error
            level_new = forecast + alpha * error
            slope = phi * slope + beta * error
            level = level_new
    return total


def FitPooledDampedTrend(series):
    best, best_sse = (0.5, 0.1, 0.9), np.inf
    for start in [(0.5, 0.1, 0.9), (0.3, 0.05, 0.7), (0.7, 0.2, 0.85)]:
        result = minimize(DampedTrendOneStepSSE, start, args=(series,), method="Nelder-Mead",
                          bounds=[(0.01, 0.99), (0.0, 0.9), (0.1, 0.98)],
                          options={"maxiter": 400, "xatol": 1e-3, "fatol": 1e-2})
        if result.fun < best_sse:
            best, best_sse = tuple(result.x), result.fun
    alpha, beta, phi = best
    return float(np.clip(alpha, 0.01, 0.99)), float(np.clip(beta, 0.0, 0.9)), float(np.clip(phi, 0.1, 0.98))


def Prep(y):
    y = np.asarray(y, float)
    return y[np.isfinite(y)]


def Flat(y, h, params):
    # drop delta, no reversion: forecast the recent pre-period mean, flat (the pure "flat-forward").
    y = Prep(y)
    return np.full(h, max(float(y.mean()), 0.0) if len(y) else 0.0)


def VasicekShrink(y, h, params):
    # Vasicek/empirical-Bayes reliability shrinkage of the org pre-mean toward the pooled grand mean:
    # w_i = between-var / (between-var + within-var_i); flat forecast at the shrunk level.
    y = Prep(y)
    if len(y) < 2:
        return Flat(y, h, params)
    within_var_i = y.var(ddof=1) / len(y)
    w = params["sigma2_between"] / (params["sigma2_between"] + within_var_i) if (params["sigma2_between"] + within_var_i) > 0 else 0.0
    level = w * y.mean() + (1.0 - w) * params["grand_mean"]
    return np.full(h, max(level, 0.0))


def PermTrans(y, h, params):
    # permanent-transitory / Kelley reliability WITHIN the series: shrink the last value toward the org's
    # own mean by gamma = 1 - transitory/total (transitory var from first differences). Flat at permanent.
    y = Prep(y)
    if len(y) < 3:
        return Flat(y, h, params)
    total = float(y.var())
    transitory = float(np.var(np.diff(y)) / 2.0)
    gamma = float(np.clip(1.0 - transitory / total, 0.0, 1.0)) if total > 0 else 0.0
    permanent = gamma * float(y[-1]) + (1.0 - gamma) * float(y.mean())
    return np.full(h, max(permanent, 0.0))


def DampedTrend(y, h, params):
    # ETS(A,Ad,N): run the level/slope recursion with the POOLED-estimated (alpha,beta,phi), per-series
    # init, then forecast level + (phi + ... + phi^k) * slope. Damping bounds a transient pre-slope.
    y = Prep(y)
    if len(y) < 3:
        return Flat(y, h, params)
    alpha, beta, phi = params["alpha"], params["beta"], params["phi"]
    level, slope = y[0], y[1] - y[0]
    for t in range(1, len(y)):
        forecast = level + phi * slope
        error = y[t] - forecast
        level = forecast + alpha * error
        slope = phi * slope + beta * error
    return np.array([max(level + sum(phi ** i for i in range(1, k + 1)) * slope, 0.0) for k in range(1, h + 1)])


METHODS = {
    "flat": Flat,
    "vasicek_shrink": VasicekShrink,
    "perm_trans": PermTrans,
    "damped_trend": DampedTrend,
}
