"""An admitted scenario must finish every step; a request count is not coverage."""


def aggregate(outcomes, allow_idle=False):
    totals = {name: sum(row.get(name, 0) for row in outcomes) for name in
              ("requests", "admitted_sequences", "completed_sequences", "incomplete_sequences")}
    statuses = [row.get("status", "inconclusive") for row in outcomes]
    status = next((s for s in ("failed", "aborted", "inconclusive") if s in statuses), "passed")
    if status == "passed" and (totals["incomplete_sequences"] or totals["admitted_sequences"] != totals["completed_sequences"]
                               or (not allow_idle and totals["completed_sequences"] == 0)):
        status = "inconclusive"
    return {**totals, "status": status, "reason": "all admitted sequences completed" if status == "passed" else "incomplete or failed scenario coverage"}
