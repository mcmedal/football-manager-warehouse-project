'use client';

import React, { useState } from 'react';

export interface RadarSlice {
  label: string;
  category: 'attacking' | 'passing' | 'possession' | 'defending';
  value: number; // per 90 value
  unit?: string;
  percentile: number; // 0 to 100
}

interface PizzaRadarChartProps {
  slices: RadarSlice[];
  playerName: string;
  position: string;
  comparisonSlices?: RadarSlice[];
  comparisonName?: string;
  ghostSlices?: RadarSlice[];
  ghostLabel?: string;
}

const CATEGORY_COLORS: Record<string, { fill: string; stroke: string; glow: string; text: string }> = {
  attacking: {
    fill: 'rgba(245, 158, 11, 0.45)',
    stroke: '#f59e0b',
    glow: 'rgba(245, 158, 11, 0.25)',
    text: '#fbbf24',
  },
  passing: {
    fill: 'rgba(6, 182, 212, 0.45)',
    stroke: '#06b6d4',
    glow: 'rgba(6, 182, 212, 0.25)',
    text: '#38bdf8',
  },
  possession: {
    fill: 'rgba(139, 92, 246, 0.45)',
    stroke: '#8b5cf6',
    glow: 'rgba(139, 92, 246, 0.25)',
    text: '#a78bfa',
  },
  defending: {
    fill: 'rgba(244, 63, 94, 0.45)',
    stroke: '#f43f5e',
    glow: 'rgba(244, 63, 94, 0.25)',
    text: '#fb7185',
  },
};

export default function PizzaRadarChart({
  slices,
  playerName,
  position,
  comparisonSlices,
  comparisonName,
  ghostSlices,
  ghostLabel,
}: PizzaRadarChartProps) {
  const isComparison = Boolean(comparisonSlices && comparisonSlices.length > 0);
  const [chartMode, setChartMode] = useState<'pizza' | 'radar'>('pizza');
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);

  const size = 520;
  const center = size / 2;
  const maxRadius = 180;
  const total = slices.length;

  if (total === 0) {
    return <div style={{ color: 'var(--text-dim)', textAlign: 'center', padding: '2rem' }}>No percentile data available</div>;
  }

  const angleStep = (2 * Math.PI) / total;
  const rings = [0.25, 0.5, 0.75, 1.0];

  // Full wedge path for pizza mode
  const getFullSlicePath = (index: number, pct: number) => {
    const gap = angleStep * 0.02;
    const startAngle = index * angleStep - Math.PI / 2 + gap;
    const endAngle = (index + 1) * angleStep - Math.PI / 2 - gap;
    const r = Math.max(12, (Math.min(100, Math.max(0, pct)) / 100) * maxRadius);

    const x1 = center + r * Math.cos(startAngle);
    const y1 = center + r * Math.sin(startAngle);
    const x2 = center + r * Math.cos(endAngle);
    const y2 = center + r * Math.sin(endAngle);

    return `M ${center} ${center} L ${x1} ${y1} A ${r} ${r} 0 0 1 ${x2} ${y2} Z`;
  };

  // Outer arc only for ghost raw outline in pizza mode
  const getFullSliceArc = (index: number, pct: number) => {
    const gap = angleStep * 0.02;
    const startAngle = index * angleStep - Math.PI / 2 + gap;
    const endAngle = (index + 1) * angleStep - Math.PI / 2 - gap;
    const r = Math.max(12, (Math.min(100, Math.max(0, pct)) / 100) * maxRadius);

    const x1 = center + r * Math.cos(startAngle);
    const y1 = center + r * Math.sin(startAngle);
    const x2 = center + r * Math.cos(endAngle);
    const y2 = center + r * Math.sin(endAngle);

    return `M ${x1} ${y1} A ${r} ${r} 0 0 1 ${x2} ${y2}`;
  };

  // Helper for polygon points in radar mode
  const getRadarPolygon = (items: RadarSlice[]) => {
    return items
      .map((item, idx) => {
        const angle = idx * angleStep + angleStep / 2 - Math.PI / 2;
        const r = Math.max(8, (Math.min(100, Math.max(0, item.percentile)) / 100) * maxRadius);
        const x = center + r * Math.cos(angle);
        const y = center + r * Math.sin(angle);
        return `${x},${y}`;
      })
      .join(' ');
  };

  const hoveredSlice = hoveredIdx !== null ? slices[hoveredIdx] : null;
  const hoveredCompSlice = (hoveredIdx !== null && comparisonSlices) ? comparisonSlices[hoveredIdx] : null;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', position: 'relative' }}>
      {/* Chart Controls & Legend */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          width: '100%',
          marginBottom: '1rem',
          flexWrap: 'wrap',
          gap: '0.75rem',
        }}
      >
        {/* Player / Category Legend */}
        {isComparison ? (
          <div style={{ display: 'flex', gap: '0.6rem', flexWrap: 'wrap', alignItems: 'center' }}>
            <span
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.4rem',
                fontSize: '0.75rem',
                fontWeight: 700,
                color: '#38bdf8',
                background: 'rgba(56, 189, 248, 0.12)',
                padding: '0.2rem 0.55rem',
                borderRadius: '5px',
                border: '1px solid rgba(56, 189, 248, 0.35)',
              }}
            >
              <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#38bdf8' }} />
              {playerName}
            </span>
            <span
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.4rem',
                fontSize: '0.75rem',
                fontWeight: 700,
                color: '#f43f5e',
                background: 'rgba(244, 63, 94, 0.12)',
                padding: '0.2rem 0.55rem',
                borderRadius: '5px',
                border: '1px solid rgba(244, 63, 94, 0.35)',
              }}
            >
              <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#f43f5e' }} />
              {comparisonName || 'Player 2'}
            </span>
          </div>
        ) : (
          <div style={{ display: 'flex', gap: '0.6rem', flexWrap: 'wrap' }}>
            {Object.entries(CATEGORY_COLORS).map(([cat, colors]) => (
              <span
                key={cat}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '0.35rem',
                  fontSize: '0.75rem',
                  fontWeight: 600,
                  color: colors.text,
                  background: 'rgba(255, 255, 255, 0.04)',
                  padding: '0.2rem 0.5rem',
                  borderRadius: '4px',
                  border: `1px solid ${colors.glow}`,
                }}
              >
                <span
                  style={{
                    width: '8px',
                    height: '8px',
                    borderRadius: '50%',
                    background: colors.stroke,
                  }}
                />
                {cat.toUpperCase()}
              </span>
            ))}
            {ghostSlices && (
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                  fontSize: '0.72rem',
                  fontWeight: 600,
                  color: '#94a3b8',
                  background: 'rgba(148, 163, 184, 0.12)',
                  padding: '0.2rem 0.5rem',
                  borderRadius: '4px',
                  border: '1px dashed #94a3b8',
                }}
              >
                <span style={{ width: '12px', height: '2px', borderTop: '2px dashed #94a3b8' }} />
                {ghostLabel || 'Raw Domestic Baseline'}
              </span>
            )}
          </div>
        )}

        {/* Mode Toggle Buttons */}
        <div style={{ display: 'flex', gap: '0.35rem', background: 'rgba(255, 255, 255, 0.05)', padding: '2px', borderRadius: '6px' }}>
          <button
            onClick={() => setChartMode('pizza')}
            style={{
              padding: '0.3rem 0.65rem',
              fontSize: '0.75rem',
              fontWeight: 600,
              borderRadius: '4px',
              color: chartMode === 'pizza' ? '#fff' : 'var(--text-dim)',
              background: chartMode === 'pizza' ? '#0284c7' : 'transparent',
              border: 'none',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
            }}
            title={isComparison ? 'Concentric overlaid full-width pizza wedges' : 'Standard Opta percentile pizza'}
          >
            Opta Pizza
          </button>
          <button
            onClick={() => setChartMode('radar')}
            style={{
              padding: '0.3rem 0.65rem',
              fontSize: '0.75rem',
              fontWeight: 600,
              borderRadius: '4px',
              color: chartMode === 'radar' ? '#fff' : 'var(--text-dim)',
              background: chartMode === 'radar' ? '#0284c7' : 'transparent',
              border: 'none',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
            }}
            title="Classic polygon web radar overlay"
          >
            Radar Polygon
          </button>
        </div>
      </div>

      {/* SVG Container */}
      <div style={{ position: 'relative', width: '100%', maxWidth: '520px', aspectRatio: '1/1' }}>
        <svg
          viewBox={`0 0 ${size} ${size}`}
          style={{ width: '100%', height: '100%', overflow: 'visible' }}
        >
          <defs>
            <radialGradient id="centerGlow" cx="50%" cy="50%" r="50%">
              <stop offset="0%" stopColor="rgba(255,255,255,0.08)" />
              <stop offset="100%" stopColor="transparent" />
            </radialGradient>
          </defs>

          {/* Background concentric rings */}
          {rings.map((ring) => (
            <circle
              key={ring}
              cx={center}
              cy={center}
              r={maxRadius * ring}
              fill="none"
              stroke="rgba(255, 255, 255, 0.09)"
              strokeWidth="1"
              strokeDasharray={ring === 1.0 ? 'none' : '3 3'}
            />
          ))}

          {/* Radial grid dividers */}
          {slices.map((_, idx) => {
            const angle = idx * angleStep - Math.PI / 2;
            const x = center + maxRadius * Math.cos(angle);
            const y = center + maxRadius * Math.sin(angle);
            return (
              <line
                key={`line-${idx}`}
                x1={center}
                y1={center}
                x2={x}
                y2={y}
                stroke="rgba(255, 255, 255, 0.12)"
                strokeWidth="1"
              />
            );
          })}

          {/* 1. SINGLE PLAYER OPTA PIZZA */}
          {!isComparison && chartMode === 'pizza' && (
            slices.map((slice, idx) => {
              const colors = CATEGORY_COLORS[slice.category] || CATEGORY_COLORS.attacking;
              const isHovered = hoveredIdx === idx;

              return (
                <path
                  key={`single-slice-${idx}`}
                  d={getFullSlicePath(idx, slice.percentile)}
                  fill={isHovered ? colors.stroke : colors.fill}
                  stroke={colors.stroke}
                  strokeWidth={isHovered ? '2' : '1.2'}
                  style={{
                    transition: 'all 0.25s cubic-bezier(0.16, 1, 0.3, 1)',
                    cursor: 'pointer',
                    filter: isHovered ? `drop-shadow(0 0 8px ${colors.stroke})` : 'none',
                  }}
                  onMouseEnter={() => setHoveredIdx(idx)}
                  onMouseLeave={() => setHoveredIdx(null)}
                />
              );
            })
          )}

          {/* 2. DUAL COMPARISON: OVERLAID OPTA PIZZA (Concentric Layered Wedges) */}
          {isComparison && chartMode === 'pizza' && (
            slices.map((slice, idx) => {
              const compSlice = comparisonSlices ? comparisonSlices[idx] : null;
              const isHovered = hoveredIdx === idx;
              const p1 = slice.percentile;
              const p2 = compSlice ? compSlice.percentile : 0;

              // To prevent hiding the smaller wedge, draw the larger one first, smaller one on top
              const p1Larger = p1 >= p2;

              return (
                <g key={`overlay-group-${idx}`} onMouseEnter={() => setHoveredIdx(idx)} onMouseLeave={() => setHoveredIdx(null)} style={{ cursor: 'pointer' }}>
                  {p1Larger ? (
                    <>
                      {/* Player 1 (Larger) Base Arc */}
                      <path
                        d={getFullSlicePath(idx, p1)}
                        fill={isHovered ? 'rgba(56, 189, 248, 0.45)' : 'rgba(56, 189, 248, 0.25)'}
                        stroke="#38bdf8"
                        strokeWidth="2"
                        style={{ transition: 'all 0.2s ease', filter: isHovered ? 'drop-shadow(0 0 8px #38bdf8)' : 'none' }}
                      />
                      {/* Player 2 (Smaller) Foreground Arc */}
                      {compSlice && (
                        <path
                          d={getFullSlicePath(idx, p2)}
                          fill={isHovered ? 'rgba(244, 63, 94, 0.85)' : 'rgba(244, 63, 94, 0.58)'}
                          stroke="#f43f5e"
                          strokeWidth="2"
                          strokeDasharray="4 2"
                          style={{ transition: 'all 0.2s ease', filter: isHovered ? 'drop-shadow(0 0 8px #f43f5e)' : 'none' }}
                        />
                      )}
                    </>
                  ) : (
                    <>
                      {/* Player 2 (Larger) Base Arc */}
                      {compSlice && (
                        <path
                          d={getFullSlicePath(idx, p2)}
                          fill={isHovered ? 'rgba(244, 63, 94, 0.45)' : 'rgba(244, 63, 94, 0.25)'}
                          stroke="#f43f5e"
                          strokeWidth="2"
                          strokeDasharray="4 2"
                          style={{ transition: 'all 0.2s ease', filter: isHovered ? 'drop-shadow(0 0 8px #f43f5e)' : 'none' }}
                        />
                      )}
                      {/* Player 1 (Smaller) Foreground Arc */}
                      <path
                        d={getFullSlicePath(idx, p1)}
                        fill={isHovered ? 'rgba(56, 189, 248, 0.85)' : 'rgba(56, 189, 248, 0.58)'}
                        stroke="#38bdf8"
                        strokeWidth="2"
                        style={{ transition: 'all 0.2s ease', filter: isHovered ? 'drop-shadow(0 0 8px #38bdf8)' : 'none' }}
                      />
                    </>
                  )}

                  {/* Ghost arc outline (raw baseline before adjustment) in pizza mode */}
                  {ghostSlices && ghostSlices[idx] && Math.abs(ghostSlices[idx].percentile - slice.percentile) >= 2 && (
                    <path
                      d={getFullSliceArc(idx, ghostSlices[idx].percentile)}
                      fill="none"
                      stroke="#cbd5e1"
                      strokeWidth="2.5"
                      strokeDasharray="3 3"
                      opacity="0.85"
                    />
                  )}
                </g>
              );
            })
          )}

          {/* 4. RADAR POLYGON MODE */}
          {chartMode === 'radar' && (
            <g>
              {/* Ghost outline: raw domestic baseline polygon */}
              {ghostSlices && (
                <polygon
                  points={getRadarPolygon(ghostSlices)}
                  fill="none"
                  stroke="#94a3b8"
                  strokeWidth="2"
                  strokeDasharray="4 4"
                  opacity="0.75"
                />
              )}
              <polygon
                points={getRadarPolygon(slices)}
                fill="rgba(56, 189, 248, 0.35)"
                stroke="#38bdf8"
                strokeWidth="2.5"
                style={{ filter: 'drop-shadow(0 0 10px rgba(56, 189, 248, 0.3))' }}
              />
              {comparisonSlices && (
                <polygon
                  points={getRadarPolygon(comparisonSlices)}
                  fill="rgba(244, 63, 94, 0.3)"
                  stroke="#f43f5e"
                  strokeWidth="2.5"
                  strokeDasharray="4 4"
                  style={{ filter: 'drop-shadow(0 0 10px rgba(244, 63, 94, 0.3))' }}
                />
              )}
            </g>
          )}

          {/* Central Core Overlay */}
          <circle cx={center} cy={center} r={18} fill="#0d111a" stroke="rgba(255,255,255,0.2)" strokeWidth="1.5" />

          {/* Perimeter Labels */}
          {slices.map((slice, idx) => {
            const angle = idx * angleStep + angleStep / 2 - Math.PI / 2;
            const labelRadius = maxRadius + 24;
            const x = center + labelRadius * Math.cos(angle);
            const y = center + labelRadius * Math.sin(angle);
            const isHovered = hoveredIdx === idx;
            const colors = CATEGORY_COLORS[slice.category] || CATEGORY_COLORS.attacking;

            // Determine text anchor based on angle
            const cos = Math.cos(angle);
            const anchor = Math.abs(cos) < 0.2 ? 'middle' : cos > 0 ? 'start' : 'end';

            return (
              <text
                key={`label-${idx}`}
                x={x}
                y={y}
                textAnchor={anchor}
                dominantBaseline="central"
                fill={isHovered ? '#fff' : colors.text}
                fontSize={isHovered ? '11px' : '9.5px'}
                fontWeight={isHovered ? 700 : 500}
                style={{
                  transition: 'all 0.2s ease',
                  cursor: 'pointer',
                  userSelect: 'none',
                }}
                onMouseEnter={() => setHoveredIdx(idx)}
                onMouseLeave={() => setHoveredIdx(null)}
              >
                {slice.label}
              </text>
            );
          })}
        </svg>

        {/* Hover Tooltip Overlay */}
        {hoveredSlice && (
          <div
            style={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              background: 'rgba(13, 17, 26, 0.96)',
              border: `1px solid ${CATEGORY_COLORS[hoveredSlice.category]?.stroke || '#38bdf8'}`,
              borderRadius: '10px',
              padding: '0.75rem 1rem',
              pointerEvents: 'none',
              boxShadow: '0 8px 30px rgba(0,0,0,0.7)',
              textAlign: 'center',
              minWidth: isComparison ? '230px' : '150px',
              backdropFilter: 'blur(10px)',
              zIndex: 10,
            }}
          >
            <div style={{ fontSize: '0.72rem', color: CATEGORY_COLORS[hoveredSlice.category]?.text || 'var(--text-dim)', textTransform: 'uppercase', fontWeight: 700, letterSpacing: '0.05em' }}>
              {hoveredSlice.category}
            </div>
            <div style={{ fontSize: '0.95rem', fontWeight: 800, color: '#fff', margin: '3px 0 6px 0' }}>
              {hoveredSlice.label}
            </div>
            {hoveredSlice.label === 'Possession Lost' && (
              <div style={{ fontSize: '0.68rem', color: 'var(--text-dim)', marginTop: '-3px', marginBottom: '6px' }}>
                (Fewer ball losses = higher %tile)
              </div>
            )}

            {isComparison && hoveredCompSlice ? (
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.85rem', borderTop: '1px solid rgba(255,255,255,0.08)', paddingTop: '6px' }}>
                <div style={{ textAlign: 'left' }}>
                  <div style={{ fontSize: '0.72rem', color: '#38bdf8', fontWeight: 700, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {playerName.split(' ').pop()}
                  </div>
                  <div style={{ fontSize: '1.25rem', fontWeight: 800, color: '#38bdf8' }}>
                    {Math.round(hoveredSlice.percentile)}
                    <span style={{ fontSize: '0.68rem', fontWeight: 500, color: 'var(--text-muted)' }}>%tile</span>
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }} className="mono-number">
                    {hoveredSlice.value} {hoveredSlice.unit || '/90'}
                  </div>
                </div>

                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: '0.72rem', color: '#f43f5e', fontWeight: 700, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {(comparisonName || 'Player 2').split(' ').pop()}
                  </div>
                  <div style={{ fontSize: '1.25rem', fontWeight: 800, color: '#f43f5e' }}>
                    {Math.round(hoveredCompSlice.percentile)}
                    <span style={{ fontSize: '0.68rem', fontWeight: 500, color: 'var(--text-muted)' }}>%tile</span>
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }} className="mono-number">
                    {hoveredCompSlice.value} {hoveredCompSlice.unit || '/90'}
                  </div>
                </div>
              </div>
            ) : (
              <div>
                <div style={{ fontSize: '1.3rem', fontWeight: 800, color: CATEGORY_COLORS[hoveredSlice.category]?.text || '#38bdf8' }}>
                  {Math.round(hoveredSlice.percentile)}
                  <span style={{ fontSize: '0.72rem', fontWeight: 500, color: 'var(--text-muted)', marginLeft: '2px' }}>
                    th %tile
                  </span>
                </div>
                <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: '2px' }} className="mono-number">
                  {hoveredSlice.value} {hoveredSlice.unit || '/90'}
                </div>
                {ghostSlices && hoveredIdx !== null && ghostSlices[hoveredIdx] && Math.abs(ghostSlices[hoveredIdx].percentile - hoveredSlice.percentile) >= 1 && (
                  <div style={{ fontSize: '0.72rem', color: '#94a3b8', marginTop: '6px', borderTop: '1px dashed rgba(255,255,255,0.12)', paddingTop: '4px' }}>
                    Raw Domestic: <span style={{ color: '#fff', fontWeight: 600 }}>{ghostSlices[hoveredIdx].value} {hoveredSlice.unit || '/90'}</span> ({Math.round(ghostSlices[hoveredIdx].percentile)}%tile)
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Subtext info */}
      <div style={{ marginTop: '0.75rem', fontSize: '0.75rem', color: 'var(--text-dim)', textAlign: 'center' }}>
        Ranked against {position}s with ≥ 450 minutes in database.
      </div>
    </div>
  );
}
