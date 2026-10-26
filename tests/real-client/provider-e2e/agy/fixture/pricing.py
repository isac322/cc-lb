"""Tiny order-pricing helpers for the cc-lb agy QA fixture."""


def parse_quantity(text):
    """Parse a quantity such as "3" or " 12 " into an int; reject negatives."""
    value = int(text)
    if value < 0:
        raise ValueError("quantity must be non-negative")
    return value


def line_total(unit_price_cents, quantity):
    """Return the total for one order line in whole cents."""
    return unit_price_cents * quantity


def apply_discount(total_cents, percent):
    """Return the customer total after a percent discount.

    The result is an int rounded down to whole cents. percent must be
    between 0 and 100 inclusive, otherwise ValueError is raised.
    """
    return total_cents - total_cents * percent / 100
