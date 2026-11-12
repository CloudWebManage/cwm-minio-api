"""Offline consistency checking for an optional JSON companion to a standalone XLSX.

Checksums bind the generated document and workbook bytes; they are not signatures
or proof of the truth of the underlying measurements. No workbook code is executed.
"""
import fcntl
import hashlib
import json
import os
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree

from .config import CampaignError, check_path
from .provenance import fingerprint


PRODUCER = "cwm-campaign-stakeholder-v1"
PUBLICATION_CONTRACT = "standalone-xlsx; per-file-atomic"
MAX_JSON_BYTES = 128 * 1024 * 1024
MAX_XLSX_BYTES = 256 * 1024 * 1024
METADATA = {"producer", "report_id", "workbook_sha256"}


def document_id(document):
    return fingerprint({key: value for key, value in document.items() if key not in METADATA})


def file_sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("nonfinite JSON")


def check_pair(xlsx, companion):
    """Validate two stable files. Caller holds the output-directory lock."""
    try:
        for path, limit in ((xlsx, MAX_XLSX_BYTES), (companion, MAX_JSON_BYTES)):
            check_path(path)
            if not path.is_file() or path.stat().st_size > limit:
                raise ValueError()
        with companion.open("rb") as stream:
            document = json.loads(stream.read(MAX_JSON_BYTES + 1), object_pairs_hook=_unique, parse_constant=_invalid_constant)
        if (not isinstance(document, dict) or document.get("producer") != PRODUCER
                or type(document.get("integrity_schema")) is not int or document["integrity_schema"] != 1
                or document.get("publication_contract") != PUBLICATION_CONTRACT):
            raise ValueError()
        identifier, expected_hash = document.get("report_id"), document.get("workbook_sha256")
        if (not isinstance(identifier, str) or not re.fullmatch("[0-9a-f]{64}", identifier)
                or not isinstance(expected_hash, str) or not re.fullmatch("[0-9a-f]{64}", expected_hash)
                or document_id(document) != identifier):
            raise ValueError()
        with xlsx.open("rb") as stream:
            actual_hash = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual_hash != expected_hash:
                raise ValueError()
            stream.seek(0)
            with zipfile.ZipFile(stream) as package:
                members = package.infolist()
                if (len(members) > 2048 or len({m.filename for m in members}) != len(members)
                        or sum(m.file_size for m in members) > MAX_XLSX_BYTES
                        or "xl/workbook.xml" not in package.namelist()
                        or package.getinfo("docProps/custom.xml").file_size > 65536
                        or package.testzip() is not None):
                    raise ValueError()
                raw = package.read("docProps/custom.xml")
                if b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
                    raise ValueError()
                properties = ElementTree.fromstring(raw)
                ids = [p for p in properties if p.attrib.get("name") == "Report ID"]
                if len(ids) != 1 or len(ids[0]) != 1 or ids[0][0].text != identifier:
                    raise ValueError()
        return {"valid": True, "report_id": identifier, "workbook_sha256": actual_hash, "publication_contract": PUBLICATION_CONTRACT}
    except (OSError, ValueError, KeyError, TypeError, OverflowError, RecursionError,
            RuntimeError, NotImplementedError, zipfile.BadZipFile, ElementTree.ParseError):
        raise CampaignError("report pair missing, malformed, unsupported or inconsistent; regenerate and check-report before paired sharing") from None


def check_report(xlsx, *, _output_locked=False):
    """The workbook is authoritative standalone; this checks paired sharing only."""
    xlsx = Path(xlsx).absolute()
    if ".." in xlsx.parts or xlsx.suffix != ".xlsx":
        raise CampaignError("check-report requires a non-escaping .xlsx path")
    if _output_locked:
        return check_pair(xlsx, xlsx.with_suffix(".json"))
    fd = None
    try:
        check_path(xlsx)
        fd = os.open(xlsx.parent, os.O_DIRECTORY)
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        return check_pair(xlsx, xlsx.with_suffix(".json"))
    except (OSError, ValueError):
        raise CampaignError("report files unavailable, unsafe or locked by an active export") from None
    finally:
        if fd is not None:
            os.close(fd)
