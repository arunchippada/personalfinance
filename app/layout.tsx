import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "FundWise — Fund research UX",
  description: "Explore, compare and improve your fund portfolio."
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
