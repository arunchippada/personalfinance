import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Fidelity Portfolio Builder",
  description: "Local-only deterministic portfolio construction app for Fidelity funds data."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
