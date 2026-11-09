import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "Eval Runner",
  description: "Live dashboard for the distributed eval runner",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <main>
          <h1>
            <Link href="/">Eval Runner</Link>
          </h1>
          {children}
        </main>
      </body>
    </html>
  );
}
