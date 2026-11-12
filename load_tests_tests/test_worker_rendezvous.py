"""Exercise the actual Locust test-start listener against disposable Redis."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.skipif(not os.getenv("CAMPAIGN_TEST_REDIS_URL"), reason="disposable Redis not configured")
@pytest.mark.parametrize("scenario", ["init-publication", "late-redis-reply", "worker-before-running", "duration", "delayed", "stale", "extra", "stop", "lease", "failure", "timeout"])
def test_application_readiness_before_traffic(manifest_data, tmp_path, scenario):
    manifest_data["coordination"] = {"redis_url_env": "TEST_REDIS", "dedicated": True,
        "expected_workers": 2, "rendezvous_seconds": 5}
    path = tmp_path / "input.json"
    path.write_text(json.dumps(manifest_data))
    script = Path(__file__).parent / "fixtures" / "rendezvous_process.py"
    result = subprocess.run([sys.executable, str(script), str(path), scenario],
        env={**os.environ, "TEST_REDIS": os.environ["CAMPAIGN_TEST_REDIS_URL"],
             "CWM_CAMPAIGN_ROLE": "coordinated-local", "PYTHONPATH": str(Path(__file__).parents[1])},
        capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
