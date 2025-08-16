"""Function signature and argument handling utilities for cache key generation.

This module provides utilities for normalizing function arguments and signatures
to create consistent cache keys. It handles argument binding, default values,
and **kwargs sorting to ensure that functionally equivalent function calls
produce identical cache keys regardless of argument order or style.
"""

from __future__ import annotations

import inspect


def sort_args(sig: inspect.Signature, bound_args: dict):
    """Sort any variadic kwargs (**kwargs) with signature parameters first.

    Signature parameters are already bound in signature order with defaults), then any
    remaining **kwargs unpacked alphabetically.

    Args:
        sig: Function signature to use for ordering.
        bound_args: Dict of bound named arguments (which may contain unsorted **kwargs).

    Returns:
        Named args dict in signature order followed by alphabetically sorted **kwargs.

    Example:
        Here we have two signature args "b" and "a" (in that order). We first sort
        the signature parameters to ("a", "b") then the **kwargs to ("c", "d").

        >>> def f(a, b, **kw): pass
        >>> sig = inspect.signature(f)
        >>> args = tuple()
        >>> kwargs = dict(b=2, a=1, d=4, c=3)
        >>> bound = sig.bind(*args, **kwargs)
        >>> bound_args = bound.arguments  # {'a': 1, 'b': 2, 'kw': {'d': 4, 'c': 3}}
        >>> sort_args(sig, bound_args)
        {'a': 1, 'b': 2, 'c': 3, 'd': 4}
    """

    def not_var_keyword(param_name: str):
        """Check if parameter is not **kwargs (already sorted)."""
        return sig.parameters[param_name].kind != inspect.Parameter.VAR_KEYWORD

    # Ordered names of parameters in the signature
    bound_sig_params = list(filter(not_var_keyword, sig.parameters))
    # There can be only one variadic **kwargs parameter
    var_kw_params = set(bound_args) - set(bound_sig_params)
    # Unpacked **kwargs dict of key: value that are not bound in the function signature
    unpacked_kwargs = bound_args[var_kw_params.pop()] if any(var_kw_params) else {}
    # Flatten out the **kwargs into the same dict as the bound signature params
    return {
        **{k: bound_args[k] for k in bound_sig_params},
        **{k: unpacked_kwargs[k] for k in sorted(unpacked_kwargs)},
    }


def normalise_args(func, args, kwargs, sort: bool = True):
    """Normalise all parameters to signature order, **kwargs sorted and unpacked last.

    If `sort` is passed as True, sort the **kwargs (to avoid the same **kwargs in
    different order causing a cache miss, as the order of **kwargs rarely matters).
    """
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()  # Add missing defaults
    bound_args = bound.arguments  # k:v dict of all signature params
    return sort_args(sig, bound_args) if sort else bound_args
