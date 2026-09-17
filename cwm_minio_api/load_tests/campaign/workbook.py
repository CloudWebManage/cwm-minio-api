"""Presentation of already sanitized evidence. No analytics or raw journal access."""
import json

import xlsxwriter
from .config import CampaignError


def write_workbook(document, path, *, max_rows=10000):
    with xlsxwriter.Workbook(path, {"strings_to_formulas": False, "strings_to_urls": False}) as book:
        book.set_properties({"title": "Object-store stakeholder evidence", "author": "CWM campaign harness",
                             "comments": "Derived evidence; operator criteria are not production SLOs."})
        book.set_custom_property("Report ID", document["report_id"])
        title = book.add_format({"bold": True, "font_size": 20, "font_color": "#17365D"})
        header = book.add_format({"bold": True, "bg_color": "#17365D", "font_color": "white", "text_wrap": True, "valign": "vcenter"})
        text = book.add_format({"text_wrap": True, "valign": "top"})
        numeric = book.add_format({"num_format": "0.000", "valign": "top"})
        integer = book.add_format({"num_format": "#,##0", "valign": "top"})
        percent = book.add_format({"num_format": "0.00%", "valign": "top"})
        good = book.add_format({"bg_color": "#E2F0D9", "font_color": "#375623"})
        bad = book.add_format({"bg_color": "#FCE4D6", "font_color": "#9C0006"})
        unknown = book.add_format({"bg_color": "#FFF2CC", "font_color": "#7F6000"})

        def value(cell):
            if cell is None:
                return "unknown"
            if isinstance(cell, (dict, list)):
                return json.dumps(cell, sort_keys=True)
            return cell

        def table(name, columns, rows):
            if len(rows) > max_rows:
                raise CampaignError("stakeholder workbook row limit exceeded; narrow the comparison")
            sheet = book.add_worksheet(name)
            sheet.hide_gridlines(2)
            sheet.freeze_panes(1, 1)
            sheet.set_row(0, 42)
            for col, (key, label, width) in enumerate(columns):
                sheet.write(0, col, label, header)
                sheet.set_column(col, col, width)
            for index, row in enumerate(rows, 1):
                for col, (key, _, _) in enumerate(columns):
                    cell = value(row.get(key))
                    fmt = percent if "error_rate" in key else integer if type(cell) is int else numeric if type(cell) is float else text
                    sheet.write(index, col, cell, fmt)
            sheet.autofilter(0, 0, max(1, len(rows)), len(columns) - 1)
            for col, (key, _, _) in enumerate(columns):
                if "status" in key or key in ("qualification", "validation"):
                    for term, fmt in (("passed", good), ("validated", good), ("failed", bad), ("aborted", bad),
                                      ("unknown", unknown), ("inconclusive", unknown), ("not-executed", unknown), ("not-validated", unknown), ("running", unknown)):
                        # Text rules, with explicit literal values, not user expressions.
                        sheet.conditional_format(1, col, max(1, len(rows)), col,
                            {"type": "cell", "criteria": "==", "value": '"' + term + '"', "format": fmt})
            sheet.set_landscape()
            sheet.fit_to_pages(1, 0)
            sheet.repeat_rows(0)
            return sheet

        runs = document["runs"]
        points = document.get("points", [p for run in runs for p in run["points"]])
        summary = book.add_worksheet("Executive Summary")
        summary.hide_gridlines(2)
        summary.set_column("A:A", 35)
        summary.set_column("B:B", 100)
        summary.set_column("D:N", 12)
        summary.freeze_panes(2, 0)
        summary.write(0, 0, "Object-store tested evidence", title)
        summary.write(2, 0, "System label", header)
        summary.write(2, 1, document["system_label"] or "Unlabelled campaign evidence", text)
        summary.set_row(2, 45)
        rows = [("Generated (UTC)", document["generated_utc"]),
                ("Report ID (document SHA256)", document["report_id"]),
                ("Sharing contract", "This self-contained XLSX is authoritative standalone. Files are published atomically individually. Run check-report XLSX before sharing its optional JSON companion."),
                ("Report interpretation", document.get("summary", "Tested lower bounds under explicit operator criteria; physical limits unknown")),
                ("Snapshot", "PROVISIONAL — active or unresolved command" if any(r["provisional"] for r in runs) else "Completed local evidence snapshot"),
                ("Criteria", document.get("criteria") or "None; descriptive report only — capacity not established"),
                ("Load model", "Closed-loop; requested RPS is a ceiling. HTTP requests, logical sequences and verified GET MiB/s are separate metrics."),
                ("Missing system context", "Server version, topology, hardware, background load and infrastructure telemetry not recorded."),
                ("Charts", "First 24 measured points / operation groups for legibility. Full bounded data is in the tables; no empty-data charts."),
                ("Data completeness", "No aggregate rows truncated. Unexecuted, failed, aborted and unknown cases remain in Stage Results / Load Points.")]
        for run in runs:
            rows += [("Run ID", run["run_id"]), ("Manifest SHA256", run["manifest_hash"]),
                     ("Source evidence revision", run["source_evidence_revision"]), ("Automated profile", run["profile_status"]),
                     ("Tested load", f"{sum(p['samples'] for p in run['points'] if p['phase'] == 'traffic')} traffic request samples; "
                                      f"{sum(p['phase'] == 'traffic' for p in run['points'])} recorded traffic attempts; capacity not established without comparison criteria."),
                     ("Cleanup evidence", run["cleanup"])]
        for group in document.get("groups", []):
            rate = group["highest_tested_passing_http_rps"]
            rows.append((group["stage"] + " / " + group["group"][:12],
                         f"Highest tested passing {rate:.3f} achieved HTTP RPS; {group['conclusion']}. Physical limit unknown."
                         if rate is not None else group["conclusion"]))
        for index, (label, cell) in enumerate(rows, 4):
            summary.write(index, 0, label, header)
            summary.write(index, 1, value(cell), text)
            summary.set_row(index, 48 if isinstance(cell, dict) else 34)

        def run_rows(key):
            return [{"run_id": run["run_id"], **row} for run in runs for row in run[key]]

        table("Stage Results", [("run_id", "Run ID", 26), ("stage", "Stage / scenario", 24), ("attempt", "Controller attempt", 14),
            ("status", "Outcome", 18), ("reason_code", "Bounded reason code", 26), ("started_utc", "Started (UTC)", 34), ("finished_utc", "Finished (UTC)", 34)], run_rows("stages"))
        table("Load Points", [("point", "Point (run/stage/index)", 44), ("group", "Comparison group SHA256", 68), ("phase", "Measurement scope", 16), ("stage_status", "Stage outcome", 16),
            ("controller_status", "Enclosing controller outcome", 20), ("controller_evidence", "Controller evidence resolution", 20),
            ("qualification", "Criterion qualification", 20), ("qualification_reasons", "Qualification reasons", 45),
            ("requested_rps", "Requested ceiling (HTTP RPS)", 19), ("samples", "Request samples", 16), ("timed_samples", "Source-timed samples", 18),
            ("duration_seconds", "Request interval (s)", 18), ("http_rps", "Achieved HTTP RPS", 18), ("logical_sequences_s", "Logical sequences/s", 19),
            ("verified_get_mib_s", "Verified GET MiB/s", 19), ("errors", "Unexpected errors", 18), ("error_rate", "Unexpected error fraction", 18),
            ("worst_operation_p99_ms", "Worst operation/size p99 (ms)", 20), ("admitted_sequences", "Admitted sequences", 18),
            ("completed_sequences", "Completed sequences", 18), ("incomplete_sequences", "Incomplete sequences", 18),
            ("sequence_duration_seconds", "Sequence interval (s)", 18), ("started_utc", "First request start (UTC)", 34),
            ("finished_utc", "Last request completion (UTC)", 34)], points)
        ops = run_rows("operations")
        table("Operations", [("point", "Point", 44), ("operation", "S3 operation", 38), ("size_bytes", "Request body size (bytes)", 20),
            ("samples", "Exact samples", 16), ("errors", "Unexpected errors", 18), ("error_rate", "Unexpected error fraction", 18),
            ("p50_ms", "p50 latency (ms)", 18), ("p95_ms", "p95 latency (ms)", 18), ("p99_ms", "p99 latency (ms)", 18),
            ("invalid_samples", "Invalid evidence samples", 20), ("verified_get_bytes", "Verified GET bytes", 20)], ops)
        lifecycle = run_rows("lifecycle") or [{"observation": "No lifecycle observations recorded", "status": "not-executed"}]
        table("Lifecycle", [("run_id", "Run ID", 26), ("cohort", "Cohort", 16), ("kind", "Evidence kind", 16),
            ("observation", "Recorded state / gate", 38), ("status", "Gate status", 18), ("observations", "Observation count", 18),
            ("first_utc", "First observation (UTC)", 34), ("last_utc", "Last observation (UTC)", 34)], lifecycle)
        conditions = [{"run_id": run["run_id"], "fact": key, "value": val} for run in runs for key, val in run["conditions"].items()]
        for run in runs:
            conditions += [{"run_id": run["run_id"], "fact": "request_budget", "value": run["request_budget"]},
                           {"run_id": run["run_id"], "fact": "byte_budget", "value": run["byte_budget"]}]
            for point in run["points"]:
                conditions += [{"run_id": run["run_id"], "fact": point["point"] + " / " + key, "value": point[key]}
                               for key in ("source_context", "worker_context_fingerprint", "inventory_fingerprint", "ticket_provenance", "remaining_objects_at_start", "next_size_bytes_at_start", "generator_mode", "context_consistent")]
        table("Test Conditions", [("run_id", "Run ID", 26), ("fact", "Condition / provenance fact", 54), ("value", "Recorded value", 110)], conditions)
        table("Evidence and Limitations", [("number", "#", 6), ("limitation", "Calculation definition / evidence limitation", 145)],
              [{"number": i, "limitation": v} for i, v in enumerate(document["limitations"], 1)])
        if document.get("groups"):
            table("Comparison", [("group", "Like-for-like group SHA256", 68), ("stage", "Workload", 24), ("repeat_count", "All tested points", 18),
                ("highest_tested_passing_http_rps", "Highest validated achieved HTTP RPS (repeat minimum)", 28),
                ("first_higher_nonpassing_requested_rps", "First higher nonpassing requested RPS", 25), ("conclusion", "Conclusion", 54), ("physical_limit", "Physical limit", 18)], document["groups"])
            table("Repeated Levels", [("group", "Group", 68), ("requested_rps", "Requested HTTP RPS", 20), ("repeat_count", "Repeat count", 15),
                ("validation", "Validation", 20), ("http_rps_min", "Achieved RPS min", 20), ("http_rps_max", "Achieved RPS max", 20),
                ("p99_ms_min", "Worst op p99 min (ms)", 20), ("p99_ms_max", "Worst op p99 max (ms)", 20),
                ("error_rate_min", "Error fraction min", 20), ("error_rate_max", "Error fraction max", 20), ("points", "All repeat point IDs", 60)],
                [{"group": group["group"], **level} for group in document["groups"] for level in group["levels"]])

        measured = [p for p in points if p["samples"] and p["phase"] == "traffic"][:24]
        latency = [o for o in ops if o["samples"] and o["p99_ms"] is not None][:24]
        charts = []
        rates = [p for p in measured if p["http_rps"] is not None]
        if rates:
            charts += [("Achieved HTTP request throughput", "HTTP requests/s", rates, "point", "http_rps"),
                       ("Checksum-verified GET throughput", "MiB/s", rates, "point", "verified_get_mib_s")]
        if latency:
            charts.append(("Latency by operation and size", "p99 latency (ms)",
                           [{**o, "label": f"{o['point']} / {o['operation']} / {o['size_bytes']} B"} for o in latency], "label", "p99_ms"))
        if measured:
            charts.append(("Unexpected request errors", "Error fraction", measured, "point", "error_rate"))
        if charts:
            data = book.add_worksheet("Chart Data")
            data.freeze_panes(1, 0)
            data.set_column("A:A", 75)
            data.set_column("B:B", 22)
            row = 0
            for index, (name, unit, values, label_key, value_key) in enumerate(charts):
                data.write_row(row, 0, [name, unit], header)
                first = row + 1
                for item in values:
                    row += 1
                    data.write(row, 0, item[label_key], text)
                    data.write(row, 1, item[value_key], percent if value_key == "error_rate" else numeric)
                chart = book.add_chart({"type": "bar"})
                chart.add_series({"name": name, "categories": ["Chart Data", first, 0, row, 0], "values": ["Chart Data", first, 1, row, 1],
                                  "fill": {"color": "#4472C4"}, "border": {"none": True}})
                chart.set_title({"name": name + " (first 24 groups)"})
                chart.set_x_axis({"name": unit, "min": 0})
                chart.set_y_axis({"num_font": {"size": 8}, "reverse": True})
                chart.set_legend({"none": True})
                chart.set_size({"width": 1000, "height": max(340, len(values) * 30)})
                summary.insert_chart(2 + sum(max(18, len(c[2]) * 2) + 2 for c in charts[:index]), 3, chart)
                row += 3
