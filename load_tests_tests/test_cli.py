import json
import os
import subprocess
import sys


MODULE = "cwm_minio_api.load_tests.campaign"


def test_cli_help_and_offline_validate_dont_create_state(manifest_data, tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest_data))
    for command in ([], ["prepare"], ["stage"], ["worker"], ["cleanup"], ["export"]):
        result = subprocess.run([sys.executable, "-m", MODULE, *command, "--help"], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    result = subprocess.run([sys.executable, "-m", MODULE, "validate", str(path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["valid"] is True
    assert not __import__("pathlib").Path(manifest_data["state_dir"]).exists()


def test_cli_errors_dont_echo_invalid_inline_secret(manifest_data, tmp_path):
    manifest_data["target"]["secret_key"] = "DO-NOT-PRINT-THIS"
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(manifest_data))
    result = subprocess.run([sys.executable, "-m", MODULE, "validate", str(path)], capture_output=True, text=True)
    assert result.returncode == 2
    assert "DO-NOT-PRINT-THIS" not in result.stdout + result.stderr
