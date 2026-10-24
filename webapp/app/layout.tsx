import type { Metadata } from 'next';
import { Suspense } from 'react';
import './globals.css';
import Navbar from '@/components/Navbar';

export const metadata: Metadata = {
  title: 'FM Analytics | Opta & FBref Player Scouting Warehouse',
  description:
    'Comprehensive Football Manager player scouting reports, Opta pizza charts, FBref percentile rankings, and vector similarity comparisons powered by SQL Server Data Warehouse.',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body>
        <div className="app-container">
          <Suspense fallback={<div className="navbar" style={{ height: '60px' }} />}>
            <Navbar />
          </Suspense>
          <main className="main-content" id="main-content-area">
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}

