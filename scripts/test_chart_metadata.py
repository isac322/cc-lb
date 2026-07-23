from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import chart_metadata


class ChartMetadataTests(unittest.TestCase):
    def test_stamp_and_verify_chart_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            chart = Path(temp) / "Chart.yaml"
            chart.write_text(
                "apiVersion: v2\nname: cc-lb\nversion: 0.1.1\n"
                'appVersion: "0.1.1"\nannotations:\n'
                "  cc-lb.io/source-revision: __SOURCE_REVISION__\n",
                encoding="utf-8",
            )

            chart_metadata.stamp_chart(chart, "deadbeef")
            chart_metadata.verify_chart(chart, "0.1.1", "deadbeef")

    def test_verify_rejects_wrong_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            chart = Path(temp) / "Chart.yaml"
            chart.write_text(
                "apiVersion: v2\nname: cc-lb\nversion: 0.1.1\n"
                'appVersion: "0.1.0"\nannotations:\n'
                "  cc-lb.io/source-revision: deadbeef\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(chart_metadata.ChartMetadataError, "metadata mismatch"):
                chart_metadata.verify_chart(chart, "0.1.1", "deadbeef")

    def test_stamp_quotes_numeric_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            chart = Path(temp) / "Chart.yaml"
            chart.write_text(
                "apiVersion: v2\nname: cc-lb\nversion: 0.1.1\n"
                'appVersion: "0.1.1"\nannotations:\n'
                "  cc-lb.io/source-revision: __SOURCE_REVISION__\n",
                encoding="utf-8",
            )
            revision = "0" * 40

            chart_metadata.stamp_chart(chart, revision)

            self.assertIn(
                f'cc-lb.io/source-revision: "{revision}"',
                chart.read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
