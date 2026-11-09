type Props = { total: number; succeeded: number; cached: number; failed: number };

// Stacked bar of finished jobs: executed, served from cache, failed.
export function ProgressBar({ total, succeeded, cached, failed }: Props) {
  const pct = (n: number) => (total > 0 ? `${(100 * n) / total}%` : "0%");
  return (
    <div className="bar" title={`${succeeded} ok, ${cached} cached, ${failed} failed of ${total}`}>
      <span className="ok" style={{ width: pct(succeeded) }} />
      <span className="cached" style={{ width: pct(cached) }} />
      <span className="fail" style={{ width: pct(failed) }} />
    </div>
  );
}
