import os
import json
from datetime import datetime

class ReportDelivery:
    def __init__(self, output_dir: str = "outputs", log_dir: str = "logs"):
        self.output_dir = output_dir
        self.log_dir = log_dir
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)

    def export_metrics_to_json(self, metrics, filename: str):
        payload = {
            "generated_at": datetime.utcnow().isoformat(),
            "metrics": [
                {"name": m.name, "value": m.value, "metadata": getattr(m, "metadata", None)}
                for m in metrics
            ],
        }
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "w") as f:
            json.dump(payload, f, indent=2)
        return filepath

    def export_full_report_to_json(self, metrics, insights: dict, filename: str):
        payload = {
            "generated_at": datetime.utcnow().isoformat(),
            "metrics": [
                {"name": m.name, "value": m.value, "metadata": getattr(m, "metadata", None)}
                for m in metrics
            ],
            "insights": insights,
        }
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "w") as f:
            json.dump(payload, f, indent=2)
        return filepath

    def log_delivery_event(self, report_path: str):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "report_path": report_path,
            "status": "DELIVERED",
        }
        log_file = os.path.join(self.log_dir, "delivery_log.jsonl")
        with open(log_file, "a") as f:
            f.write(json.dumps(log_entry) + "\n")
        return log_file

def export_full_report_to_json(self, metrics, insights: dict, filename: str):
    payload = {
        "generated_at": datetime.utcnow().isoformat(),
        "metrics": [
            {"name": m.name, "value": m.value, "metadata": getattr(m, "metadata", None)}
            for m in metrics
        ],
        "insights": insights,
    }
    filepath = os.path.join(self.output_dir, filename)
    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2)
    return filepath


