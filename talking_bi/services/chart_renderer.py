"""
Chart Rendering Service — Phase 5

Renders actual chart images from prepared data.
Uses matplotlib for deterministic, backend-safe rendering.
"""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import base64
from io import BytesIO


class ChartRenderer:
    """Renders charts as base64-encoded PNG images."""

    def render_line(self, data, x_key, y_key, kpi_name=""):
        """
        Render a line chart from timeseries data.

        Args:
            data: List of dicts with x_key and y_key fields
            x_key: Field name for x-axis (usually time/segment column)
            y_key: Field name for y-axis values (usually "value")
            kpi_name: Name of the KPI for y-axis label

        Returns:
            Base64-encoded PNG string, or None if rendering fails
        """
        if not data or len(data) < 2:
            return None

        try:
            x = [d[x_key] for d in data]
            y = [d[y_key] for d in data]

            plt.figure(figsize=(6, 3))
            plt.plot(x, y, linewidth=2, color="#2563eb")
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            plt.xlabel(x_key)
            plt.ylabel(kpi_name or y_key)
            plt.tight_layout()

            buffer = BytesIO()
            plt.savefig(buffer, format="png", bbox_inches="tight", dpi=100)
            plt.close()

            buffer.seek(0)
            encoded = base64.b64encode(buffer.read()).decode("utf-8")

            # Prevent oversized payloads
            if len(encoded) > 200000:
                return None

            return encoded

        except Exception:
            return None

    def render_bar(self, data, x_key, y_key, kpi_name=""):
        """
        Render a bar chart from categorical data.

        Args:
            data: List of dicts with x_key and y_key fields
            x_key: Field name for x-axis (categorical column)
            y_key: Field name for y-axis values (usually "value")
            kpi_name: Name of the KPI for y-axis label

        Returns:
            Base64-encoded PNG string, or None if rendering fails
        """
        if not data or len(data) < 1:
            return None

        try:
            x = [d[x_key] for d in data]
            y = [d[y_key] for d in data]

            plt.figure(figsize=(6, 3))
            plt.bar(x, y, color="#2563eb")
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, axis="y")
            plt.xlabel(x_key)
            plt.ylabel(kpi_name or y_key)
            plt.tight_layout()

            buffer = BytesIO()
            plt.savefig(buffer, format="png", bbox_inches="tight", dpi=100)
            plt.close()

            buffer.seek(0)
            encoded = base64.b64encode(buffer.read()).decode("utf-8")

            if len(encoded) > 200000:
                return None

            return encoded

        except Exception:
            return None

    def render_metric(self, value):
        """
        Render a metric card (no image needed, just return value).

        Args:
            value: The scalar metric value

        Returns:
            The value unchanged
        """
        return value
