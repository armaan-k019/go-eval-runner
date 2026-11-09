"use client";

import { useEffect, useState } from "react";
import { fmt, wsUrl, type RunStats } from "@/lib/api";
import { ProgressBar } from "./progress-bar";

type Conn = "connecting" | "live" | "finished" | "error";

// Subscribes to the run's WebSocket stream and renders each update.
export function LiveRun({ runId }: { runId: string }) {
  const [stats, setStats] = useState<RunStats | null>(null);
  const [conn, setConn] = useState<Conn>("connecting");

  useEffect(() => {
    const ws = new WebSocket(wsUrl(runId));
    ws.onopen = () => setConn("live");
    ws.onmessage = (e) => {
      const s = JSON.parse(e.data as string) as RunStats;
      setStats(s);
      if (s.done) setConn("finished");
    };
    ws.onerror = () => setConn("error");
    ws.onclose = (e) => setConn(e.code === 1000 ? "finished" : "error");
    return () => ws.close();
  }, [runId]);

  if (!stats) {
    return <p>{conn === "error" ? <span className="err">Could not open the run stream.</span> : "Connecting"}</p>;
  }
  const finished = stats.succeeded + stats.cached + stats.failed;
  const items: [string, string][] = [
    ["Finished", `${fmt(finished)} / ${fmt(stats.total)}`],
    ["Throughput", `${fmt(stats.jobs_per_minute)} jobs/min`],
    ["Wall time", `${fmt(stats.wall_seconds, 1)} s`],
    ["Executor calls", fmt(stats.executor_calls)],
    ["Cache saved", `${fmt(100 * stats.cache_saved, 1)}%`],
    ["Retries", fmt(stats.retries)],
    ["Overhead p50 / p95", `${fmt(stats.overhead_p50_ms, 1)} / ${fmt(stats.overhead_p95_ms, 1)} ms`],
    ["Exec p50 / p95", `${fmt(stats.exec_p50_ms)} / ${fmt(stats.exec_p95_ms)} ms`],
    ["Pass rate", `${fmt(100 * stats.pass_rate, 1)}%`],
    ["Mean score", fmt(stats.mean_score, 3)],
  ];
  return (
    <>
      <p>
        <span className="badge">{conn}</span> <span className="mono">{stats.run_id}</span> {stats.suite}
      </p>
      <div className="panel">
        <ProgressBar total={stats.total} succeeded={stats.succeeded} cached={stats.cached} failed={stats.failed} />
        <p className="mono" style={{ color: "var(--muted)" }}>
          queued {fmt(stats.queued)} &middot; leased {fmt(stats.leased)} &middot; waiting {fmt(stats.waiting)} &middot; ok {fmt(stats.succeeded)} &middot; cached{" "}
          {fmt(stats.cached)} &middot; failed {fmt(stats.failed)}
        </p>
      </div>
      <h2>Stats</h2>
      <div className="grid">
        {items.map(([k, v]) => (
          <div className="panel stat" key={k}>
            <div className="k">{k}</div>
            <div className="v">{v}</div>
          </div>
        ))}
      </div>
    </>
  );
}
