'use client';

import React from 'react';
import { RadarSlice } from './PizzaRadarChart';

interface PercentileTableProps {
  slices: RadarSlice[];
}

export default function PercentileTable({ slices }: PercentileTableProps) {
  const getBarColorClass = (pct: number) => {
    if (pct >= 80) return 'pct-elite';
    if (pct >= 60) return 'pct-great';
    if (pct >= 40) return 'pct-good';
    if (pct >= 20) return 'pct-avg';
    return 'pct-poor';
  };

  const getCategoryBadge = (category: string) => {
    switch (category) {
      case 'attacking':
        return <span style={{ color: 'var(--accent-attacking)', fontWeight: 600, fontSize: '0.7rem' }}>ATTACK</span>;
      case 'passing':
        return <span style={{ color: 'var(--accent-passing)', fontWeight: 600, fontSize: '0.7rem' }}>PASS</span>;
      case 'possession':
        return <span style={{ color: 'var(--accent-possession)', fontWeight: 600, fontSize: '0.7rem' }}>POSS</span>;
      case 'defending':
        return <span style={{ color: 'var(--accent-defending)', fontWeight: 600, fontSize: '0.7rem' }}>DEF</span>;
      default:
        return null;
    }
  };

  return (
    <div className="fbref-table-container">
      <table className="fbref-table">
        <thead>
          <tr>
            <th style={{ width: '80px' }}>Type</th>
            <th>Statistic</th>
            <th style={{ textAlign: 'right', width: '90px' }}>Per 90</th>
            <th style={{ width: '170px' }}>Percentile vs Peers</th>
          </tr>
        </thead>
        <tbody>
          {slices.map((slice, idx) => (
            <tr key={idx}>
              <td>{getCategoryBadge(slice.category)}</td>
              <td style={{ fontWeight: 500, color: '#f1f5f9' }}>
                <span>{slice.label}</span>
                {slice.label === 'Possession Lost' && (
                  <span style={{ fontSize: '0.68rem', color: 'var(--text-dim)', marginLeft: '6px', fontWeight: 400 }}>
                    (fewer is better)
                  </span>
                )}
              </td>
              <td style={{ textAlign: 'right' }} className="mono-number">
                {slice.value}
              </td>
              <td>
                <div className="percentile-cell">
                  <span
                    className="mono-number"
                    style={{
                      width: '28px',
                      textAlign: 'right',
                      fontSize: '0.8rem',
                      fontWeight: 700,
                      color:
                        slice.percentile >= 80
                          ? '#34d399'
                          : slice.percentile >= 60
                          ? '#38bdf8'
                          : slice.percentile >= 40
                          ? '#60a5fa'
                          : slice.percentile >= 20
                          ? '#fbbf24'
                          : '#fb7185',
                    }}
                  >
                    {Math.round(slice.percentile)}
                  </span>
                  <div className="percentile-bar-bg">
                    <div
                      className={`percentile-bar-fill ${getBarColorClass(slice.percentile)}`}
                      style={{ width: `${Math.max(4, Math.min(100, slice.percentile))}%` }}
                    />
                  </div>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
