# Bounded GET and interrupted mixed recovery

User authorizes point-1 continuation, unchanged budgets/deadline/access. Preserve pinned predecessors.

1. Test then implement safe GET retries (5 attempts / 120 seconds), charge and measure every attempt, validate each fresh body; keep auth/integrity failures fatal. Separate physical failed attempts from logical scenario failure and make quit idempotent.
2. Test then implement checkpoint recovery of interrupted mixed sequences using acknowledged mutation identities. Never replay acknowledged writes/markers or completed sequences; preserve original interruption receipts. Check complete ticket coverage before allowing the stage to pass.
3. Test an audited successor helper requiring drained boundaries, exact journal/Redis counters and original scope. Install only public corrected runtime, bind existing chat approval to successor, preflight and resume. Preserve/recover companion ownership without reprovisioning.
4. Fresh whole-change review, relevant regression suites, fresh running status and concise private handoff.

Review focus: recovered physical errors still visible; no budget reset or ambiguous write replay; provenance remains historical; original deadlines and manifests unchanged.
