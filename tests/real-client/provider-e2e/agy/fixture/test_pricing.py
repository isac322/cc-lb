import unittest

from pricing import apply_discount, line_total, parse_quantity


class PricingTest(unittest.TestCase):
    def test_parse_quantity_strips_whitespace(self):
        self.assertEqual(parse_quantity(" 12 "), 12)

    def test_parse_quantity_rejects_negative(self):
        with self.assertRaises(ValueError):
            parse_quantity("-1")

    def test_line_total(self):
        self.assertEqual(line_total(250, 4), 1000)

    def test_discount_rounds_down_to_int(self):
        result = apply_discount(999, 15)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 849)

    def test_discount_rejects_out_of_range(self):
        with self.assertRaises(ValueError):
            apply_discount(100, 101)
        with self.assertRaises(ValueError):
            apply_discount(100, -1)


if __name__ == "__main__":
    unittest.main()
