# -*- coding: utf-8 -*-
"""
Created on Tue Mar  3 10:46:35 2026

@author: EspenNordsveen
"""


def handler(data):
    import base64
    import re

    from io import BytesIO
    from typing import Iterable, Literal, Optional, Tuple

    import numpy as np
    import pandas as pd
    import pymc as pm  # type: ignore

    from pymc.sampling.jax import sample_numpyro_nuts  # type: ignore

    def wide_to_daily_mix_combined(
        df: pd.DataFrame,
        date_col: str = "Date",
        passthrough_cols: Optional[Iterable[str]] = ("Net CV"),
        # When your wide columns look like: "191212 - Combustible Waste to EFW - BIFFA WASTE LTD - SWANSEA"
        # we parse them as:
        #   waste_type = "191212 - Combustible Waste to EFW"
        #   customer   = "BIFFA WASTE LTD - SWANSEA"
        #
        # Normalization options:
        #   "per_customer": within each (date, customer), fractions sum to 1 across waste types
        #   "per_day": within each date, fractions sum to 1 across all (waste_type, customer) combos
        #   "none": no normalization at all
        normalize_scope: Literal["per_customer", "per_day", "none"] = "per_day",
        # How to combine (category, customer) in the wide output column label
        combine_label_sep: str = " | ",
        # Optional filter: include only columns that start with 6-digit EWC code
        require_ewc_prefix: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Transform a wide table with columns like:
            "191212 - Combustible Waste to EFW - BIFFA WASTE LTD - SWANSEA"
        into:
          (1) a wide daily table with combined columns "<waste_type>{sep}<customer>" and normalized values
          (2) a long table with [date, waste_type, customer, value, value_norm]

        Parameters
        ----------
        df : pd.DataFrame
            Input data. Must contain a date column, passthrough columns (optional),
            and many waste columns following the "code - description - customer..." pattern.
        date_col : str
            The date column name (will be converted to datetime).
        passthrough_cols : iterable[str] or str or None
            Columns carried through and joined at the end. If str is provided, it's treated as a single column.
        normalize_scope : {"per_customer", "per_day", "none"}
            Normalization strategy:
                - "per_customer": for each (date, customer), sum of values across waste types = 1
                - "per_day": for each date, sum across all (waste_type, customer) = 1
                - "none": no normalization
        combine_label_sep : str
            Separator used when building combined wide column labels: "<waste_type>{sep}<customer>".
        require_ewc_prefix : bool
            If True, only columns with a 6-digit code followed by " - " are considered waste columns.

        Returns
        -------
        wide_combined : pd.DataFrame
            A wide table with one row per date, columns for each "<waste_type>{sep}<customer>" (normalized per chosen scope),
            plus passthrough columns.
        long_df : pd.DataFrame
            Long table with columns:
                [date_col, "waste_type", "customer", "value", "value_norm" (if normalized)]

        Notes
        -----
        - This function does NOT weight customers by volume. If you need mass-weighted normalization, we can add a weight column.
        - If `normalize_scope="per_customer"`, each customer contributes equally (composition focus).
          If `normalize_scope="per_day"`, the whole day's combined vector sums to 1 (portfolio mix).
        """

        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])

        # --- Normalize passthrough input robustly ---
        if passthrough_cols is None:
            passthrough_cols = ()
        elif isinstance(passthrough_cols, str):
            passthrough_cols = (passthrough_cols,)
        else:
            passthrough_cols = tuple(passthrough_cols)

        # Identify candidate waste columns
        cols = df.columns.tolist()
        non_waste_set = set([date_col] + list(passthrough_cols))
        candidates = [c for c in cols if c not in non_waste_set]

        # Optional: only accept columns that start with a 6-digit EWC code + " - "
        ewc_pattern = re.compile(r"^\d{6} - ")
        if require_ewc_prefix:
            waste_cols = [c for c in candidates if isinstance(c, str) and ewc_pattern.match(c)]
        else:
            # fallback: require at least 3 parts when splitting " - "
            waste_cols = [c for c in candidates if isinstance(c, str) and len(c.split(" - ")) >= 3]

        if not waste_cols:
            raise ValueError("No waste columns detected. Check 'date_col', 'passthrough_cols' and naming pattern.")

        # Build mapping: column -> (waste_type, customer)
        # waste_type = "<code> - <description>"
        # customer   = join(rest with ' - ')
        mapping = {}
        for c in waste_cols:
            parts = c.split(" - ")
            if len(parts) < 3:
                # Skip malformed; or raise if you prefer strictness
                continue
            waste_code = parts[0].strip()
            waste_desc = parts[1].strip()
            waste_type = f"{waste_code} - {waste_desc}"
            customer = " - ".join(p.strip() for p in parts[2:])
            mapping[c] = (waste_type, customer)

        # Melt to long
        long_df = df[[date_col] + list(passthrough_cols) + list(mapping.keys())].melt(
            id_vars=[date_col] + list(passthrough_cols),
            value_vars=list(mapping.keys()),
            var_name="col",
            value_name="value",
        )

        # Attach parsed fields
        long_df["waste_type"] = long_df["col"].map(lambda x: mapping[x][0])
        long_df["customer"] = long_df["col"].map(lambda x: mapping[x][1])
        long_df.drop(columns="col", inplace=True)

        # Ensure numeric values
        long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")

        # ---------------------------
        # Normalization
        # ---------------------------
        value_col = "value"
        if normalize_scope == "per_customer":
            # For each (date, customer), normalize across waste types
            sums = long_df.groupby([date_col, "customer"])["value"].transform("sum")
            long_df["value_norm"] = long_df["value"] / sums.where(sums > 0)
            value_col = "value_norm"
        elif normalize_scope == "per_day":
            # For each date, normalize across all (waste_type, customer)
            sums = long_df.groupby([date_col])["value"].transform("sum")
            long_df["value_norm"] = long_df["value"] / sums.where(sums > 0)
            value_col = "value_norm"
        elif normalize_scope == "none":
            # no-op
            pass
        else:
            raise ValueError("normalize_scope must be one of: 'per_customer', 'per_day', 'none'")

        # ---------------------------
        # Build wide with combined labels
        # ---------------------------
        # Combined label: "<waste_type>{sep}<customer>"
        long_df["combined"] = long_df["waste_type"].astype(str) + combine_label_sep + long_df["customer"].astype(str)

        wide_combined_values = long_df.pivot_table(
            index=date_col,
            columns="combined",
            values=value_col,
            aggfunc="mean",  # if duplicates exist per (date, combined), average them
        ).sort_index()

        # If no normalization selected, some rows may not sum to 1. We leave them as-is (by design).
        # For 'per_customer' scope, each customer's sub-vector sums to 1, but the combined columns per day will not necessarily sum to 1.
        # For 'per_day' scope, the entire row will sum to ~1 (ignoring NaNs).

        # Join passthrough (assumed day-level)
        if passthrough_cols:
            passthrough_daily = (
                df[[date_col] + list(passthrough_cols)].drop_duplicates(subset=[date_col]).set_index(date_col)
            )
            wide_combined = wide_combined_values.join(passthrough_daily, how="left").reset_index()
        else:
            wide_combined = wide_combined_values.reset_index()

        return wide_combined, long_df

    def build_lag_tensor(df, waste_cols, L=10, date_col="Date"):
        df = df.sort_values(date_col).reset_index(drop=True).copy()

        X = np.zeros((len(df), L + 1, len(waste_cols)))

        for lag in range(L + 1):
            shifted = df[waste_cols].shift(lag)
            X[:, lag, :] = shifted.to_numpy()

        # Drop rows with NaNs (first L days)
        valid = ~np.isnan(X).any(axis=(1, 2))
        df = df.loc[valid].reset_index(drop=True)
        X = X[valid]

        return df, X

    def fit_model(df, X_lag, waste_cols, L=10, energy_col="Net CV", tons_col="Net Infeed"):

        y = df[energy_col].to_numpy()

        # Standardize target
        y_mean, y_std = y.mean(), y.std()
        y_z = (y - y_mean) / y_std

        # Use K-1 waste types (avoid collinearity)
        waste_cols_used = waste_cols[:-1]
        X = X_lag[:, :, : len(waste_cols_used)]
        tons = df[tons_col].to_numpy() if tons_col in df else None

        with pm.Model() as model:

            # Lag weights76
            w = pm.Dirichlet("w", a=np.ones(X.shape[1]))

            # decay = pm.Exponential("decay", 1.0)
            # lags = np.arange(X.shape[1])
            # w_raw = pm.math.exp(-decay * lags)
            # w = w_raw / pm.math.sum(w_raw)

            # Effective burned mix
            x_eff = pm.math.sum(X * w[None, :, None], axis=1)

            # Waste effects
            beta = pm.Normal("beta", 0, 1, shape=x_eff.shape[1])

            # Intercept
            alpha = pm.Normal("alpha", 0, 1)

            mu = alpha + pm.math.dot(x_eff, beta)

            # Noise (optionally scaled by tons burned)
            sigma0 = pm.HalfNormal("sigma0", 1)

            if tons is not None:
                sigma = sigma0 / pm.math.sqrt(pm.math.maximum(tons, 1e-6))
            else:
                sigma = sigma0

            pm.Normal("y_obs", mu, sigma, observed=y_z)

            idata = sample_numpyro_nuts(draws=1500, tune=1500, chains=4, target_accept=0.90, chain_method="parallel")

        return model, idata, waste_cols_used, (y_mean, y_std)

    def summarize_effects(idata, waste_cols_used):
        # beta dims typically: (chain, draw, K)
        beta = idata.posterior["beta"].values
        beta = beta.reshape(-1, beta.shape[-1])  # (samples, K)

        rows = []
        for j, name in enumerate(waste_cols_used):
            s = beta[:, j]
            rows.append(
                {
                    "waste_type": name,
                    "P(beta>0)": float((s > 0).mean()),
                    "P(beta<0)": float((s < 0).mean()),
                    "beta_mean": float(s.mean()),
                    "beta_p10": float(np.quantile(s, 0.10)),
                    "beta_p50": float(np.quantile(s, 0.50)),
                    "beta_p90": float(np.quantile(s, 0.90)),
                }
            )

        df = pd.DataFrame(rows)
        df["abs_beta_p50"] = df["beta_p50"].abs()
        df = df.sort_values(["abs_beta_p50"], ascending=[False]).drop(columns="abs_beta_p50")

        return df

    file_b64 = data["excel_file_b64"]
    file_bytes = base64.b64decode(file_b64)

    # 2) Convert to file-like object
    file_obj = BytesIO(file_bytes)

    df_custPlusCategory = pd.read_excel(file_obj)

    # dropping columns that are almost always 0
    zero_ratio = (df_custPlusCategory == 0).mean()
    df_custPlusCategory = df_custPlusCategory.loc[:, zero_ratio < 0.95]

    # dropping rows that are almost all zero
    df_custPlusCategory = df_custPlusCategory.loc[(df_custPlusCategory != 0).sum(axis=1) > 1]

    # removing 0 cv
    df_custPlusCategory = df_custPlusCategory[df_custPlusCategory["Net CV"] > 0.0]

    # drop L1 and L2
    df_custPlusCategory = df_custPlusCategory.drop(
        columns=["Net Infeed", "CV Line 1", "CV Line 2", "Infeed Line 2", "Infeed Line 1"]
    )

    wide_df, long_df = wide_to_daily_mix_combined(
        df_custPlusCategory,
        date_col="Date",
        passthrough_cols=("Net CV"),
        normalize_scope="per_day",
        combine_label_sep=" | ",
    )

    daily_mix_df = wide_df.fillna(0)

    waste_cols = daily_mix_df.drop(columns=["Net CV", "Date"]).columns.to_list()

    daily2, X_lag = build_lag_tensor(daily_mix_df, waste_cols=waste_cols, L=10)  # how many carryover days to allow

    model, idata, waste_cols_used, scaler = fit_model(daily2, X_lag, waste_cols)

    effects = summarize_effects(idata, waste_cols_used)

    return {"model_df": effects}
