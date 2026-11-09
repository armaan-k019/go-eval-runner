"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { submitRun } from "@/lib/api";

// Launches a synthetic suite on the mock executor, useful for demos and load tests.
export function SubmitForm() {
  const router = useRouter();
  const [cases, setCases] = useState(2000);
  const [unique, setUnique] = useState(1500);
  const [latency, setLatency] = useState(100);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const distinct = Math.max(1, Math.min(unique, cases));
      const res = await submitRun({
        suite: `mock-${cases}`,
        job_type: "mock",
        config: { latency_median_ms: latency },
        cases: Array.from({ length: cases }, (_, i) => ({
          case_id: `case-${i}`,
          payload: { item: i % distinct },
        })),
      });
      router.push(`/runs/${res.run_id}`);
    } catch (err) {
      setError((err as Error).message);
      setBusy(false);
    }
  }

  return (
    <form className="panel" onSubmit={onSubmit}>
      <label>
        Cases
        <input type="number" min={1} max={100000} value={cases} onChange={(e) => setCases(+e.target.value)} />
      </label>
      <label>
        Distinct payloads
        <input type="number" min={1} value={unique} onChange={(e) => setUnique(+e.target.value)} />
      </label>
      <label>
        Median latency (ms)
        <input type="number" min={0} value={latency} onChange={(e) => setLatency(+e.target.value)} />
      </label>
      <button disabled={busy}>{busy ? "Submitting" : "Launch mock suite"}</button>
      {error && <span className="err">{error}</span>}
    </form>
  );
}
