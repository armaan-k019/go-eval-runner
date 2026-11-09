import { LiveRun } from "@/components/live-run";

export default async function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <LiveRun runId={id} />;
}
