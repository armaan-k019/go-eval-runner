import { connection } from "next/server";
import { RunTable } from "@/components/run-table";
import { SubmitForm } from "@/components/submit-form";
import { listRuns, serverApiUrl, type RunSummary } from "@/lib/api";

export default async function Home() {
  await connection(); // render per request: the run list is live data
  let runs: RunSummary[] = [];
  let error: string | null = null;
  try {
    runs = await listRuns(serverApiUrl);
  } catch (e) {
    error = (e as Error).message;
  }
  return (
    <>
      <h2>New run</h2>
      <SubmitForm />
      <h2>Recent runs</h2>
      {error && <p className="err">API unreachable from the server: {error}</p>}
      <RunTable initial={runs} />
    </>
  );
}
