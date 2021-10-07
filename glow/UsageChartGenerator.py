from typing import List
import os

MAIN_PATH = '/Users/tomevers/projects/airglow'
USAGE_CHART_TEMPLATE = 'templates/usage_chart.md'

class UsageChartGenerator():
    usage_chart_template = None
    event_name = None
    title = None
    x_series = None
    y_series = None

    def __init__(self, event_name) -> None:
        self.usage_chart_template = self._get_usage_chart_template()
        self.event_name = event_name
        self.title = "weekly usage for " + event_name

    def _get_usage_chart_template(self) -> str:
        file_path = os.path.join(MAIN_PATH, USAGE_CHART_TEMPLATE)
        with open(file_path, 'r') as file:
            return file.read()

    def add_data(self, x_series: List, y_series: List) -> None:
        self.x_series = x_series
        self.y_series = y_series

    def generate_chart(self) -> str:
        new_chart = self.usage_chart_template
        new_chart = new_chart.replace("{{labels}}", ', '.join(['"{}"'.format(x_value) for x_value in self.x_series]))
        new_chart = new_chart.replace("{{title}}", self.title)
        new_chart = new_chart.replace("{{data}}", ', '.join([str(int(y_value)) for y_value in self.y_series]))

        return new_chart