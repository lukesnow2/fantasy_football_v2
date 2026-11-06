"""The Pythagorean exponent fit behind power rankings' expected wins and luck."""
import random

from src.edw_schema.edw_etl_processor import (
    DEFAULT_PYTHAGOREAN_EXPONENT, fit_pythagorean_exponent)


def synthetic(exponent, n=200, games=14, seed=7):
    """Team-seasons whose wins follow PF^x / (PF^x + PA^x) exactly."""
    rng = random.Random(seed)
    rows = []
    for _ in range(n):
        pf, pa = rng.uniform(1300, 1900), rng.uniform(1300, 1900)
        share = pf ** exponent / (pf ** exponent + pa ** exponent)
        rows.append((games, games * share, pf, pa))
    return rows


def test_recovers_the_exponent_that_generated_the_wins():
    assert fit_pythagorean_exponent(synthetic(6.3)) == 6.3
    assert fit_pythagorean_exponent(synthetic(2.4)) == 2.4


def test_too_little_history_keeps_the_default():
    assert fit_pythagorean_exponent(synthetic(6.3, n=10)) == DEFAULT_PYTHAGOREAN_EXPONENT


def test_rows_without_points_are_ignored():
    rows = synthetic(6.3) + [(14, 7.0, 0.0, 1500.0), (0, 0.0, 0.0, 0.0)]
    assert fit_pythagorean_exponent(rows) == 6.3
