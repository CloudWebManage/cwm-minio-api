"""R4a: real process death at publication, supported recovery, and no-clobber."""
import ctypes
import errno
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from cwm_minio_api.load_tests.campaign import stakeholder
from cwm_minio_api.load_tests.campaign.config import CampaignError
from cwm_minio_api.load_tests.campaign.report import archive
from cwm_minio_api.load_tests.campaign.state import State
from test_stakeholder import synthetic, criteria
from test_stakeholder_review import checker_cli


def die_after_publication(action, destination):
    """Kill after the actual filesystem operation, before Python cleanup can run."""
    pid = os.fork()
    if pid == 0:
        def hook(original):
            def publish(src, dst, *args, **kwargs):
                result = original(src, dst, *args, **kwargs)
                if Path(dst) == destination:
                    os._exit(77)
                return result
            return publish
        # Cover the original link implementation and the safe rename replacement.
        for module, name in ((os, "link"), (os, "replace"), (stakeholder, "_rename_noreplace")):
            if hasattr(module, name):
                setattr(module, name, hook(getattr(module, name)))
        action()
        os._exit(99)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 77


def cli(*args):
    return subprocess.run([sys.executable, "-m", "cwm_minio_api.load_tests.campaign", *map(str, args)],
                          capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize("kind", ["report", "compare"])
def test_first_explicit_export_death_before_xlsx_recovers_with_overwrite(manifest_data, tmp_path, kind):
    m, state = synthetic(manifest_data)
    state.close()
    out = tmp_path / "first.xlsx"
    if kind == "report":
        export = lambda: stakeholder.report_workbook(m, out, system_label="SYNTHETIC initial export")
        flags = []
    else:
        export = lambda: stakeholder.comparison_workbook([m], out, criteria())
        flags = ["--max-p99-ms", "100", "--max-error-rate", ".01"]
    die_after_publication(export, out.with_suffix(".json"))
    assert not out.exists() and out.with_suffix(".json").is_file()
    links_at_death = out.with_suffix(".json").stat().st_nlink
    assert checker_cli(out).returncode == 2
    recovered = cli(kind, m.path / "manifest.json", "--output", out, "--overwrite", *flags)
    assert recovered.returncode == 0, recovered.stderr
    assert checker_cli(out).returncode == 0
    assert links_at_death == 1  # No transient staged alias can survive process death.
    assert out.stat().st_nlink == out.with_suffix(".json").stat().st_nlink == 1
    assert out.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize("existing", [False, True])
def test_managed_archive_generation_death_before_xlsx_recovers(manifest_data, existing):
    m, state = synthetic(manifest_data)
    out = m.path / "stakeholder.xlsx"
    if existing:
        stakeholder.report_workbook(m, system_label="SYNTHETIC previous generation")
    state.close()
    def generate():
        child_state = State(m, create=False)
        archive(m, child_state)
    die_after_publication(generate, out.with_suffix(".json"))
    assert out.exists() is existing
    assert checker_cli(out).returncode == 2
    resumed = State(m, create=False)
    assert resumed.get("archive") is None
    resumed.close()
    recovered = cli("archive", m.path / "manifest.json")
    assert recovered.returncode == 0, recovered.stderr
    assert checker_cli(out).returncode == 0
    with tarfile.open(json.loads(recovered.stdout)["archive"]) as bundle:
        assert {"stakeholder.xlsx", "stakeholder.json"} <= set(bundle.getnames())
        assert not any(Path(name).name.startswith(".stakeholder") for name in bundle.getnames())
        assert bundle.extractfile("stakeholder.xlsx").read() == out.read_bytes()
        assert bundle.extractfile("stakeholder.json").read() == out.with_suffix(".json").read_bytes()


@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("suffix", [".xlsx", ".json"])
def test_foreign_hardlinks_remain_refused_even_with_private_staging_name(manifest_data, tmp_path, managed, suffix):
    m, state = synthetic(manifest_data)
    state.close()
    out = m.path / "stakeholder.xlsx" if managed else tmp_path / "foreign.xlsx"
    stakeholder.report_workbook(m, out)
    target = out.with_suffix(suffix)
    foreign_alias = target.with_name("." + target.name + ".abcdefgh")
    os.link(target, foreign_alias)
    before = (out.read_bytes(), out.with_suffix(".json").read_bytes(), foreign_alias.read_bytes())
    result = cli("archive", m.path / "manifest.json") if managed else cli(
        "report", m.path / "manifest.json", "--output", out, "--overwrite")
    assert result.returncode == 2
    assert "hard-linked" in result.stderr
    assert (out.read_bytes(), out.with_suffix(".json").read_bytes(), foreign_alias.read_bytes()) == before
    assert target.stat().st_nlink == 2


@pytest.mark.parametrize("damage", ["invalid", "exposed", "hardlinked", "foreign-manifest"])
def test_managed_json_without_matching_private_staged_workbook_is_not_adopted(manifest_data, tmp_path, damage):
    m, state = synthetic(manifest_data)
    state.close()
    elsewhere = tmp_path / "valid.xlsx"
    source_manifest = m
    if damage == "foreign-manifest":
        source_manifest, other_state = synthetic(manifest_data, suffix="other")
        other_state.close()
    stakeholder.report_workbook(source_manifest, elsewhere)
    # A JSON marker alone does not authorize replacing an arbitrary default file.
    companion = m.path / "stakeholder.json"
    companion.write_bytes(elsewhere.with_suffix(".json").read_bytes())
    staged = m.path / ".stakeholder.xlsx.abcdefgh"
    staged.write_bytes(b"unrelated private operator file" if damage == "invalid" else elsewhere.read_bytes())
    if damage == "exposed":
        staged.chmod(0o644)
    if damage == "hardlinked":
        os.link(staged, tmp_path / "foreign-alias.xlsx")
    before = companion.read_bytes(), staged.read_bytes()
    with pytest.raises(CampaignError, match="managed"):
        stakeholder.report_workbook(m)
    assert (companion.read_bytes(), staged.read_bytes()) == before


@pytest.mark.parametrize("capability", ["missing-libc-symbol", errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP, errno.EPERM])
def test_no_clobber_capability_unavailable_fails_without_fallback(manifest_data, tmp_path, monkeypatch, capability):
    m, state = synthetic(manifest_data)
    state.close()
    def unsupported(*args):
        ctypes.set_errno(capability)
        return -1
    library = SimpleNamespace() if capability == "missing-libc-symbol" else SimpleNamespace(renameat2=unsupported)
    monkeypatch.setattr(ctypes, "CDLL", lambda *a, **kw: library)
    monkeypatch.setattr(os, "link", lambda *a, **kw: pytest.fail("unsafe hardlink fallback"))
    monkeypatch.setattr(os, "replace", lambda *a, **kw: pytest.fail("unsafe overwrite fallback"))
    out = tmp_path / "unavailable.xlsx"
    with pytest.raises(CampaignError, match="renameat2|no-clobber"):
        stakeholder.report_workbook(m, out)
    assert not out.exists() and not out.with_suffix(".json").exists()
    assert not list(tmp_path.glob(".unavailable*"))


@pytest.mark.parametrize("suffix", [".json", ".xlsx"])
def test_no_clobber_refuses_destination_created_after_preflight(manifest_data, tmp_path, monkeypatch, suffix):
    m, state = synthetic(manifest_data)
    state.close()
    out = tmp_path / "raced.xlsx"
    name = "_rename_noreplace" if hasattr(stakeholder, "_rename_noreplace") else "link"
    module = stakeholder if name == "_rename_noreplace" else os
    original = getattr(module, name)
    def racing_writer(src, dst, *args, **kwargs):
        if Path(dst).suffix == suffix:
            Path(dst).write_bytes(b"foreign writer won the race")
        return original(src, dst, *args, **kwargs)
    monkeypatch.setattr(module, name, racing_writer)
    with pytest.raises((CampaignError, FileExistsError)):
        stakeholder.report_workbook(m, out)
    assert out.with_suffix(suffix).read_bytes() == b"foreign writer won the race"
    assert not out.with_suffix(".xlsx" if suffix == ".json" else ".json").exists()
