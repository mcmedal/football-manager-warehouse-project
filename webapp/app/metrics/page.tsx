'use client';

import React, { useState, useMemo } from 'react';
import Link from 'next/link';
import {
  BarChart3,
  BookOpen,
  Search,
  Sparkles,
  Layers,
  ArrowRight,
  TrendingUp,
  Shield,
  Target,
  Zap,
  SlidersHorizontal,
  ScatterChart,
  Users,
  Compass,
  Coins,
  Activity,
  Calculator,
  Info,
  CheckCircle2,
  ChevronRight,
  HelpCircle,
  Eye,
  Sliders,
  Filter,
  Copy,
  Check
} from 'lucide-react';

import {
  METRIC_DEFINITIONS,
  GRAPH_DEFINITIONS,
  MetricDefinition,
  GraphDefinition,
  MetricCategory,
} from './metricsData';

export default function MetricsPage() {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<string>('all');
  const [selectedGraphSlice, setSelectedGraphSlice] = useState<number>(0);
  const [pizzaMode, setPizzaMode] = useState<'pizza' | 'radar'>('pizza');
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const graphs = GRAPH_DEFINITIONS;
  const metrics = METRIC_DEFINITIONS;

  const handleCopyKey = (key: string) => {
    navigator.clipboard?.writeText(key);
    setCopiedKey(key);
    setTimeout(() => {
      setCopiedKey(null);
    }, 2000);
  };

  // Dynamic Category Configuration matching the webapp's domain structure
  const categories = useMemo(() => {
    return [
      { id: 'all', label: 'All Knowledge', icon: Layers, count: metrics.length + graphs.length },
      { id: 'graphs', label: 'Visual Graphs & Charts', icon: BarChart3, count: graphs.length },
      { id: 'general', label: 'General & Pipeline', icon: Info, count: metrics.filter((m) => m.category === 'general').length },
      { id: 'attacking', label: 'Attacking & Shooting', icon: Target, count: metrics.filter((m) => m.category === 'attacking').length },
      { id: 'passing', label: 'Passing & Creation', icon: Zap, count: metrics.filter((m) => m.category === 'passing').length },
      { id: 'defending', label: 'Defending & Duels', icon: Shield, count: metrics.filter((m) => m.category === 'defending').length },
      { id: 'goalkeeper', label: 'Goalkeeping', icon: Activity, count: metrics.filter((m) => m.category === 'goalkeeper').length },
      { id: 'valuation', label: 'Moneyball & Valuation', icon: Coins, count: metrics.filter((m) => m.category === 'valuation').length },
      { id: 'math', label: 'Similarity & Modeling', icon: Calculator, count: metrics.filter((m) => m.category === 'math').length },
    ];
  }, [metrics, graphs]);

  // Filtered lists with complete cross-app field reference indexing
  const filteredMetrics = useMemo(() => {
    return metrics.filter((m) => {
      const matchesCategory = selectedCategory === 'all' || m.category === selectedCategory;
      if (!matchesCategory) return false;
      const q = searchQuery.toLowerCase().trim();
      if (!q) return true;
      const matchesSearch =
        m.name.toLowerCase().includes(q) ||
        m.code.toLowerCase().includes(q) ||
        m.id.toLowerCase().includes(q) ||
        (m.fieldKey && m.fieldKey.toLowerCase().includes(q)) ||
        (m.aliases && m.aliases.some((a) => a.toLowerCase().includes(q))) ||
        (m.usedIn && m.usedIn.some((u) => u.toLowerCase().includes(q))) ||
        m.shortDefinition.toLowerCase().includes(q) ||
        m.tacticalMeaning.toLowerCase().includes(q) ||
        m.formula.toLowerCase().includes(q) ||
        m.tags.some((t) => t.toLowerCase().includes(q));
      return matchesSearch;
    });
  }, [metrics, selectedCategory, searchQuery]);

  const filteredGraphs = useMemo(() => {
    if (selectedCategory !== 'all' && selectedCategory !== 'graphs') return [];
    const q = searchQuery.toLowerCase().trim();
    if (!q) return graphs;
    return graphs.filter(
      (g) =>
        g.title.toLowerCase().includes(q) ||
        g.subtitle.toLowerCase().includes(q) ||
        g.description.toLowerCase().includes(q) ||
        g.keyFeatures.some((f) => f.toLowerCase().includes(q))
    );
  }, [graphs, selectedCategory, searchQuery]);

  // Demo Pizza Slices for Interactive Preview
  const demoSlices = [
    { label: 'NPxG/90', category: 'attacking', pct: 92, color: 'var(--accent-attacking)' },
    { label: 'xG/Shot', category: 'attacking', pct: 86, color: 'var(--accent-attacking)' },
    { label: 'Shots/90', category: 'attacking', pct: 78, color: 'var(--accent-attacking)' },
    { label: 'xA/90', category: 'passing', pct: 84, color: 'var(--accent-passing)' },
    { label: 'OP-KP/90', category: 'passing', pct: 91, color: 'var(--accent-passing)' },
    { label: 'Pr Passes', category: 'passing', pct: 72, color: 'var(--accent-passing)' },
    { label: 'Dribbles/90', category: 'possession', pct: 88, color: 'var(--accent-possession)' },
    { label: 'Retain %', category: 'possession', pct: 64, color: 'var(--accent-possession)' },
    { label: 'Turnover %', category: 'possession', pct: 75, color: 'var(--accent-possession)' },
    { label: 'Def Act/90', category: 'defending', pct: 45, color: 'var(--accent-defending)' },
    { label: 'Pressures', category: 'defending', pct: 82, color: 'var(--accent-defending)' },
    { label: 'Aerial Won', category: 'defending', pct: 68, color: 'var(--accent-defending)' },
  ];

  return (
    <div style={{ maxWidth: '1400px', margin: '0 auto', paddingBottom: '4rem' }}>
      {/* Hero Header */}
      <div
        style={{
          background: 'linear-gradient(180deg, rgba(14, 165, 233, 0.12) 0%, rgba(10, 13, 20, 0) 100%)',
          borderRadius: '24px',
          padding: '3rem 2.5rem',
          border: '1px solid var(--border-subtle)',
          marginBottom: '2.5rem',
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        <div style={{ position: 'relative', zIndex: 1, maxWidth: '900px' }}>
          <div
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.4rem 0.85rem',
              borderRadius: '20px',
              background: 'rgba(14, 165, 233, 0.15)',
              border: '1px solid rgba(14, 165, 233, 0.3)',
              color: '#38bdf8',
              fontSize: '0.82rem',
              fontWeight: 600,
              marginBottom: '1rem',
            }}
          >
            <BookOpen size={14} />
            <span>Opta & FBref Analytics Dictionary</span>
          </div>

          <h1
            style={{
              fontSize: '2.75rem',
              fontWeight: 800,
              lineHeight: 1.15,
              color: 'var(--text-main)',
              marginBottom: '1rem',
              letterSpacing: '-0.02em',
            }}
          >
            Metrics, Mathematics & Visual Graph Guide
          </h1>

          <p
            style={{
              fontSize: '1.1rem',
              color: 'var(--text-muted)',
              lineHeight: 1.6,
              marginBottom: '2rem',
            }}
          >
            A unified reference cataloging every metric in the entirety of the web application. Every statistical column,
            filter, scatter axis, and visual graph can be searched and referenced here with its mathematical formula,
            tactical interpretation, and performance benchmarks.
          </p>

          {/* Quick Search */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              background: 'var(--bg-card)',
              border: '1px solid var(--border-subtle)',
              borderRadius: '14px',
              padding: '0.6rem 1rem',
              maxWidth: '650px',
              boxShadow: '0 8px 24px rgba(0,0,0,0.25)',
            }}
          >
            <Search size={20} color="var(--text-dim)" style={{ marginRight: '0.75rem' }} />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search any metric, column name, or formula (e.g., 'heading_accuracy', 'data_freshness', 'xGI')..."
              style={{
                background: 'transparent',
                border: 'none',
                outline: 'none',
                color: 'var(--text-main)',
                fontSize: '1rem',
                width: '100%',
              }}
              id="metrics-search-input"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery('')}
                style={{
                  background: 'transparent',
                  border: 'none',
                  color: 'var(--text-dim)',
                  cursor: 'pointer',
                  fontSize: '0.85rem',
                  padding: '4px 8px',
                }}
              >
                Clear
              </button>
            )}
          </div>
        </div>

        {/* Decorative Grid Accent */}
        <div
          style={{
            position: 'absolute',
            right: '-10%',
            top: '-20%',
            width: '600px',
            height: '600px',
            background: 'radial-gradient(circle, rgba(14, 165, 233, 0.15) 0%, rgba(0,0,0,0) 70%)',
            pointerEvents: 'none',
          }}
        />
      </div>

      {/* Category Tabs */}
      <div
        style={{
          display: 'flex',
          gap: '0.5rem',
          flexWrap: 'wrap',
          marginBottom: '2.5rem',
          borderBottom: '1px solid var(--border-subtle)',
          paddingBottom: '1rem',
        }}
      >
        {categories.map((cat) => {
          const Icon = cat.icon;
          const isActive = selectedCategory === cat.id;
          return (
            <button
              key={cat.id}
              onClick={() => setSelectedCategory(cat.id)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.6rem',
                padding: '0.65rem 1.15rem',
                borderRadius: '12px',
                background: isActive ? 'var(--accent-primary)' : 'var(--bg-card)',
                color: isActive ? '#fff' : 'var(--text-muted)',
                border: '1px solid',
                borderColor: isActive ? 'var(--accent-primary)' : 'var(--border-subtle)',
                fontWeight: isActive ? 600 : 500,
                fontSize: '0.9rem',
                cursor: 'pointer',
                transition: 'all 0.15s ease',
              }}
              id={`cat-btn-${cat.id}`}
            >
              <Icon size={16} />
              <span>{cat.label}</span>
              <span
                style={{
                  fontSize: '0.75rem',
                  padding: '1px 6px',
                  borderRadius: '10px',
                  background: isActive ? 'rgba(255,255,255,0.2)' : 'var(--bg-elevated)',
                  color: isActive ? '#fff' : 'var(--text-dim)',
                }}
              >
                {cat.count}
              </span>
            </button>
          );
        })}
      </div>

      {/* Interactive Visual Graph Preview Widget (Shown when viewing All or Graphs) */}
      {(selectedCategory === 'all' || selectedCategory === 'graphs') && !searchQuery && (
        <div
          style={{
            background: 'var(--bg-card)',
            border: '1px solid var(--border-subtle)',
            borderRadius: '20px',
            padding: '2rem',
            marginBottom: '3rem',
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem', flexWrap: 'wrap', gap: '1rem' }}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: '#38bdf8', fontSize: '0.82rem', fontWeight: 600, marginBottom: '0.25rem' }}>
                <Sparkles size={14} />
                <span>INTERACTIVE VISUAL FRAMEWORK</span>
              </div>
              <h2 style={{ fontSize: '1.5rem', fontWeight: 700, color: 'var(--text-main)' }}>
                Understanding the Opta / StatsBomb Pizza Radar Chart
              </h2>
            </div>

            <div style={{ display: 'flex', gap: '0.5rem', background: 'var(--bg-elevated)', padding: '4px', borderRadius: '10px' }}>
              <button
                onClick={() => setPizzaMode('pizza')}
                style={{
                  padding: '0.4rem 0.85rem',
                  borderRadius: '7px',
                  border: 'none',
                  background: pizzaMode === 'pizza' ? 'var(--accent-primary)' : 'transparent',
                  color: pizzaMode === 'pizza' ? '#fff' : 'var(--text-muted)',
                  fontSize: '0.82rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Pizza Slices
              </button>
              <button
                onClick={() => setPizzaMode('radar')}
                style={{
                  padding: '0.4rem 0.85rem',
                  borderRadius: '7px',
                  border: 'none',
                  background: pizzaMode === 'radar' ? 'var(--accent-primary)' : 'transparent',
                  color: pizzaMode === 'radar' ? '#fff' : 'var(--text-muted)',
                  fontSize: '0.82rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Classic Polygon
              </button>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2.5rem', alignItems: 'center' }}>
            {/* SVG Radial Chart */}
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '340px' }}>
              <svg width="320" height="320" viewBox="0 0 320 320" style={{ overflow: 'visible' }}>
                <defs>
                  <radialGradient id="radar-bg-radial" cx="50%" cy="50%" r="50%">
                    <stop offset="0%" stopColor="#0e1726" stopOpacity="0.8" />
                    <stop offset="100%" stopColor="#060911" stopOpacity="0.95" />
                  </radialGradient>
                </defs>

                {/* Base Circles */}
                <circle cx="160" cy="160" r="140" fill="url(#radar-bg-radial)" stroke="var(--border-subtle)" strokeWidth="1" />
                <circle cx="160" cy="160" r="105" fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth="1" strokeDasharray="3 3" />
                <circle cx="160" cy="160" r="70" fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth="1" strokeDasharray="3 3" />
                <circle cx="160" cy="160" r="35" fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth="1" strokeDasharray="3 3" />

                {/* Percentile Legend Circles */}
                <text x="164" y="27" fill="var(--text-dim)" fontSize="9" fontFamily="monospace">99th</text>
                <text x="164" y="62" fill="var(--text-dim)" fontSize="9" fontFamily="monospace">75th</text>
                <text x="164" y="97" fill="var(--text-dim)" fontSize="9" fontFamily="monospace">50th</text>
                <text x="164" y="132" fill="var(--text-dim)" fontSize="9" fontFamily="monospace">25th</text>

                {/* Slices or Polygon */}
                {pizzaMode === 'pizza' ? (
                  demoSlices.map((slice, i) => {
                    const total = demoSlices.length;
                    const angleStep = (2 * Math.PI) / total;
                    const startAngle = i * angleStep - Math.PI / 2;
                    const endAngle = (i + 1) * angleStep - Math.PI / 2;
                    const radius = (slice.pct / 100) * 135;

                    const x1 = 160 + radius * Math.cos(startAngle);
                    const y1 = 160 + radius * Math.sin(startAngle);
                    const x2 = 160 + radius * Math.cos(endAngle);
                    const y2 = 160 + radius * Math.sin(endAngle);

                    const isSelected = selectedGraphSlice === i;

                    return (
                      <g key={slice.label} onClick={() => setSelectedGraphSlice(i)} style={{ cursor: 'pointer' }}>
                        <path
                          d={`M 160 160 L ${x1} ${y1} A ${radius} ${radius} 0 0 1 ${x2} ${y2} Z`}
                          fill={slice.color}
                          fillOpacity={isSelected ? 0.75 : 0.35}
                          stroke={slice.color}
                          strokeWidth={isSelected ? 2 : 1}
                          style={{ transition: 'all 0.2s ease' }}
                        />
                        {/* Spoke Divider Line */}
                        <line
                          x1="160"
                          y1="160"
                          x2={160 + 140 * Math.cos(startAngle)}
                          y2={160 + 140 * Math.sin(startAngle)}
                          stroke="rgba(255,255,255,0.08)"
                          strokeWidth="1"
                        />
                      </g>
                    );
                  })
                ) : (
                  <>
                    {/* Classic Polygon */}
                    {(() => {
                      const points = demoSlices
                        .map((slice, i) => {
                          const total = demoSlices.length;
                          const angle = i * ((2 * Math.PI) / total) - Math.PI / 2;
                          const r = (slice.pct / 100) * 135;
                          const x = 160 + r * Math.cos(angle);
                          const y = 160 + r * Math.sin(angle);
                          return `${x},${y}`;
                        })
                        .join(' ');
                      return (
                        <polygon
                          points={points}
                          fill="rgba(56, 189, 248, 0.35)"
                          stroke="#38bdf8"
                          strokeWidth="2.5"
                        />
                      );
                    })()}
                  </>
                )}
              </svg>
            </div>

            {/* Explanation of Selected Slice */}
            <div>
              <div
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                  fontSize: '0.78rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  color: demoSlices[selectedGraphSlice].color,
                  marginBottom: '0.5rem',
                }}
              >
                <span>Quadrant: {demoSlices[selectedGraphSlice].category}</span>
              </div>

              <h3 style={{ fontSize: '1.75rem', fontWeight: 800, color: 'var(--text-main)', marginBottom: '0.5rem' }}>
                {demoSlices[selectedGraphSlice].label}
              </h3>

              <div
                style={{
                  display: 'flex',
                  alignItems: 'baseline',
                  gap: '0.5rem',
                  marginBottom: '1rem',
                }}
              >
                <span style={{ fontSize: '2.5rem', fontWeight: 900, color: demoSlices[selectedGraphSlice].color }}>
                  {demoSlices[selectedGraphSlice].pct}th
                </span>
                <span style={{ color: 'var(--text-muted)', fontSize: '0.95rem' }}>
                  Percentile vs Positional Peers (Min. 900 mins)
                </span>
              </div>

              <p style={{ color: 'var(--text-muted)', fontSize: '0.95rem', lineHeight: 1.6, marginBottom: '1.25rem' }}>
                The distance of each slice from the central axis corresponds directly to relative ranking across 11,000+
                players in the database. Slices extending to the 99th ring denote world-class output within that tactical
                facet.
              </p>

              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(4, 1fr)',
                  gap: '0.5rem',
                  paddingTop: '1rem',
                  borderTop: '1px solid var(--border-subtle)',
                }}
              >
                <div style={{ textAlign: 'center' }}>
                  <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: 'var(--accent-attacking)', margin: '0 auto 4px' }} />
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>Attacking</div>
                </div>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: 'var(--accent-passing)', margin: '0 auto 4px' }} />
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>Passing</div>
                </div>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: 'var(--accent-possession)', margin: '0 auto 4px' }} />
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>Possession</div>
                </div>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: 'var(--accent-defending)', margin: '0 auto 4px' }} />
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>Defending</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* SECTION 1: VISUAL GRAPHS & DASHBOARD ARTIFACTS */}
      {filteredGraphs.length > 0 && (
        <div style={{ marginBottom: '3.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
              <BarChart3 size={24} color="#38bdf8" />
              <h2 style={{ fontSize: '1.8rem', fontWeight: 700, color: 'var(--text-main)' }}>
                Visual Graph & Analytical Model Guide
              </h2>
            </div>
            <div style={{ fontSize: '0.88rem', color: 'var(--text-dim)' }}>
              Showing <strong>{filteredGraphs.length}</strong> core visualization frameworks
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(420px, 1fr))', gap: '1.5rem' }}>
            {filteredGraphs.map((g) => (
              <div
                key={g.id}
                style={{
                  background: 'var(--bg-card)',
                  border: '1px solid var(--border-subtle)',
                  borderRadius: '16px',
                  padding: '1.75rem',
                  display: 'flex',
                  flexDirection: 'column',
                  justifyContent: 'space-between',
                }}
                id={`graph-${g.id}`}
              >
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                    <span
                      style={{
                        fontSize: '0.72rem',
                        fontWeight: 700,
                        textTransform: 'uppercase',
                        letterSpacing: '0.05em',
                        color: '#38bdf8',
                        background: 'rgba(56, 189, 248, 0.1)',
                        padding: '2px 8px',
                        borderRadius: '6px',
                      }}
                    >
                      Visualization Engine
                    </span>
                  </div>

                  <h3 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.25rem' }}>
                    {g.title}
                  </h3>
                  <div style={{ fontSize: '0.85rem', color: 'var(--text-dim)', marginBottom: '1rem' }}>
                    {g.subtitle}
                  </div>

                  <p style={{ fontSize: '0.9rem', color: 'var(--text-muted)', lineHeight: 1.55, marginBottom: '1.25rem' }}>
                    {g.description}
                  </p>

                  {/* Key Features */}
                  <div style={{ marginBottom: '1.25rem' }}>
                    <div style={{ fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-dim)', marginBottom: '0.5rem' }}>
                      Key Capabilities:
                    </div>
                    <ul style={{ paddingLeft: '1.2rem', margin: 0, fontSize: '0.82rem', color: 'var(--text-muted)', display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                      {g.keyFeatures.map((f, i) => (
                        <li key={i}>{f}</li>
                      ))}
                    </ul>
                  </div>

                  {/* How to Interpret */}
                  <div
                    style={{
                      background: 'var(--bg-elevated)',
                      padding: '0.85rem 1rem',
                      borderRadius: '10px',
                      marginBottom: '1rem',
                      fontSize: '0.82rem',
                    }}
                  >
                    <div style={{ fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.25rem' }}>
                      💡 How to Interpret:
                    </div>
                    <div style={{ color: 'var(--text-muted)', lineHeight: 1.5 }}>{g.howToInterpret}</div>
                  </div>
                </div>

                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    paddingTop: '1rem',
                    borderTop: '1px solid var(--border-subtle)',
                    fontSize: '0.78rem',
                    color: 'var(--text-dim)',
                  }}
                >
                  <div>
                    Used in: <strong style={{ color: 'var(--text-muted)' }}>{g.usedIn.join(', ')}</strong>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* SECTION 2: METRIC CATALOG */}
      {filteredMetrics.length > 0 && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem', flexWrap: 'wrap', gap: '1rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
              <Calculator size={24} color="#10b981" />
              <h2 style={{ fontSize: '1.8rem', fontWeight: 700, color: 'var(--text-main)' }}>
                Detailed Metric Catalog & Formulas
              </h2>
            </div>
            <div style={{ fontSize: '0.88rem', color: 'var(--text-dim)' }}>
              Showing <strong>{filteredMetrics.length}</strong> defined metrics across the application
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(440px, 1fr))', gap: '1.5rem' }}>
            {filteredMetrics.map((m) => {
              const categoryColor =
                m.category === 'general'
                  ? '#38bdf8'
                  : m.category === 'attacking'
                  ? 'var(--accent-attacking)'
                  : m.category === 'passing'
                  ? 'var(--accent-passing)'
                  : m.category === 'defending'
                  ? 'var(--accent-defending)'
                  : m.category === 'goalkeeper'
                  ? '#0ea5e9'
                  : m.category === 'valuation'
                  ? 'var(--accent-emerald)'
                  : '#a855f7';

              const isKeyCopied = copiedKey === (m.fieldKey || m.id);

              return (
                <div
                  key={m.id}
                  style={{
                    background: 'var(--bg-card)',
                    border: '1px solid var(--border-subtle)',
                    borderRadius: '16px',
                    padding: '1.75rem',
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'space-between',
                  }}
                  id={`metric-${m.fieldKey || m.id}`}
                >
                  <div>
                    {/* Header: Code badge, category, and canonical fieldKey badge */}
                    <div style={{ marginBottom: '0.75rem' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap', marginBottom: '0.5rem' }}>
                        <span
                          style={{
                            fontFamily: 'JetBrains Mono, monospace',
                            fontSize: '0.85rem',
                            fontWeight: 700,
                            color: categoryColor,
                            background: 'rgba(255,255,255,0.06)',
                            padding: '2px 8px',
                            borderRadius: '6px',
                          }}
                        >
                          {m.code}
                        </span>

                        <span
                          style={{
                            fontSize: '0.72rem',
                            fontWeight: 600,
                            textTransform: 'uppercase',
                            letterSpacing: '0.04em',
                            padding: '2px 8px',
                            borderRadius: '10px',
                            background: 'var(--bg-elevated)',
                            color: 'var(--text-dim)',
                          }}
                        >
                          {m.category}
                        </span>

                        {/* Canonical Database & App Field Key Badge */}
                        <button
                          onClick={() => handleCopyKey(m.fieldKey || m.id)}
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '0.35rem',
                            fontFamily: 'JetBrains Mono, monospace',
                            fontSize: '0.72rem',
                            color: '#38bdf8',
                            background: 'rgba(56, 189, 248, 0.1)',
                            border: '1px solid rgba(56, 189, 248, 0.25)',
                            padding: '2px 7px',
                            borderRadius: '6px',
                            cursor: 'pointer',
                            transition: 'all 0.15s ease',
                          }}
                          title="Click to copy exact field key used in Screener, Scatter & Dossier"
                        >
                          <span>field: {m.fieldKey || m.id}</span>
                          {isKeyCopied ? <Check size={11} color="#10b981" /> : <Copy size={11} color="#38bdf8" />}
                        </button>
                      </div>

                      <h3 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.35rem' }}>
                        {m.name}
                      </h3>

                      {/* Aliases in App */}
                      {m.aliases && m.aliases.length > 1 && (
                        <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', display: 'flex', gap: '0.35rem', alignItems: 'center', flexWrap: 'wrap', marginBottom: '0.5rem' }}>
                          <span style={{ color: 'var(--text-muted)' }}>Aliases:</span>
                          {m.aliases.slice(0, 4).map((alias) => (
                            <span
                              key={alias}
                              style={{
                                fontFamily: 'monospace',
                                fontSize: '0.7rem',
                                background: 'rgba(255,255,255,0.04)',
                                padding: '1px 5px',
                                borderRadius: '4px',
                                border: '1px solid rgba(255,255,255,0.06)',
                              }}
                            >
                              {alias}
                            </span>
                          ))}
                        </div>
                      )}

                      {/* Where used across the web app */}
                      {m.usedIn && m.usedIn.length > 0 && (
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', flexWrap: 'wrap', marginBottom: '0.75rem' }}>
                          <span style={{ fontSize: '0.72rem', color: 'var(--text-dim)' }}>Used in:</span>
                          {m.usedIn.map((loc) => (
                            <span
                              key={loc}
                              style={{
                                fontSize: '0.68rem',
                                padding: '1px 6px',
                                borderRadius: '4px',
                                background: 'rgba(255,255,255,0.05)',
                                border: '1px solid rgba(255,255,255,0.08)',
                                color: 'var(--text-muted)',
                              }}
                            >
                              {loc}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>

                    {/* Short Def */}
                    <p style={{ fontSize: '0.9rem', color: 'var(--text-muted)', lineHeight: 1.5, marginBottom: '1rem' }}>
                      {m.shortDefinition}
                    </p>

                    {/* Formula Box */}
                    <div
                      style={{
                        background: 'var(--bg-primary)',
                        border: '1px solid var(--border-subtle)',
                        borderRadius: '10px',
                        padding: '0.85rem 1.15rem',
                        marginBottom: '1rem',
                        fontFamily: 'JetBrains Mono, monospace',
                        fontSize: '0.82rem',
                        color: '#38bdf8',
                        overflowX: 'auto',
                      }}
                    >
                      {m.formula}
                    </div>

                    {/* Tactical Meaning */}
                    <div style={{ marginBottom: '1rem' }}>
                      <div style={{ fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-dim)', marginBottom: '0.25rem' }}>
                        Tactical Interpretation:
                      </div>
                      <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', lineHeight: 1.55 }}>
                        {m.tacticalMeaning}
                      </p>
                    </div>

                    {/* Moneyball Significance */}
                    <div style={{ marginBottom: '1.25rem' }}>
                      <div style={{ fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--accent-emerald)', marginBottom: '0.25rem' }}>
                        Moneyball Value:
                      </div>
                      <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', lineHeight: 1.55 }}>
                        {m.moneyballSignificance}
                      </p>
                    </div>

                    {/* Benchmarks Grid */}
                    <div
                      style={{
                        background: 'var(--bg-elevated)',
                        borderRadius: '10px',
                        padding: '0.85rem 1rem',
                        marginBottom: '1rem',
                        fontSize: '0.8rem',
                      }}
                    >
                      <div style={{ fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.4rem' }}>
                        Performance Benchmarks:
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem', color: 'var(--text-muted)' }}>
                        {m.benchmarks.elite && <div>🟢 <strong style={{ color: 'var(--accent-emerald)' }}>Elite:</strong> {m.benchmarks.elite}</div>}
                        {m.benchmarks.good && <div>🔵 <strong style={{ color: '#38bdf8' }}>Good:</strong> {m.benchmarks.good}</div>}
                        {m.benchmarks.average && <div>🟡 <strong style={{ color: '#fbbf24' }}>Average:</strong> {m.benchmarks.average}</div>}
                        {m.benchmarks.poor && <div>🔴 <strong style={{ color: '#f43f5e' }}>Below Avg:</strong> {m.benchmarks.poor}</div>}
                      </div>
                    </div>
                  </div>

                  {/* FM Source Footer */}
                  <div
                    style={{
                      borderTop: '1px solid var(--border-subtle)',
                      paddingTop: '0.85rem',
                      fontSize: '0.75rem',
                      color: 'var(--text-dim)',
                    }}
                  >
                    🎮 <strong>FM24 Source:</strong> {m.fmSource}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Empty State */}
      {filteredMetrics.length === 0 && filteredGraphs.length === 0 && (
        <div
          style={{
            textAlign: 'center',
            padding: '4rem 2rem',
            background: 'var(--bg-card)',
            borderRadius: '20px',
            border: '1px solid var(--border-subtle)',
          }}
        >
          <HelpCircle size={48} color="var(--text-dim)" style={{ marginBottom: '1rem' }} />
          <h3 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.5rem' }}>
            No metrics or graphs match &quot;{searchQuery}&quot;
          </h3>
          <p style={{ color: 'var(--text-muted)', marginBottom: '1.5rem' }}>
            Try searching for terms like &quot;heading_accuracy&quot;, &quot;data_freshness&quot;, &quot;cost_per_xgi&quot;, &quot;Hold Rate&quot;, or click a category tab above.
          </p>
          <button
            onClick={() => {
              setSearchQuery('');
              setSelectedCategory('all');
            }}
            style={{
              padding: '0.65rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--accent-primary)',
              color: '#fff',
              fontWeight: 600,
              cursor: 'pointer',
            }}
          >
            Reset Filters
          </button>
        </div>
      )}

      {/* Floating Action Links to Webapp Tools */}
      <div
        style={{
          marginTop: '4rem',
          background: 'linear-gradient(135deg, rgba(59, 130, 246, 0.1), rgba(139, 92, 246, 0.1))',
          borderRadius: '20px',
          border: '1px solid var(--border-highlight)',
          padding: '2.5rem',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: '1.5rem',
        }}
      >
        <div>
          <h3 style={{ fontSize: '1.5rem', fontWeight: 800, color: 'var(--text-main)', marginBottom: '0.5rem' }}>
            Ready to Put the Metrics into Action?
          </h3>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.95rem' }}>
            Test out these metrics across our live database of 11,350+ players and goalkeepers.
          </p>
        </div>

        <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
          <Link
            href="/screener"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.75rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--accent-primary)',
              color: '#fff',
              fontWeight: 600,
              fontSize: '0.9rem',
            }}
          >
            <SlidersHorizontal size={16} />
            <span>Launch Screener</span>
          </Link>
          <Link
            href="/scatter"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.75rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--bg-elevated)',
              border: '1px solid var(--border-highlight)',
              color: 'var(--text-main)',
              fontWeight: 600,
              fontSize: '0.9rem',
            }}
          >
            <ScatterChart size={16} />
            <span>Open Scatter Explorer</span>
          </Link>
          <Link
            href="/"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.75rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--bg-elevated)',
              border: '1px solid var(--border-highlight)',
              color: 'var(--text-main)',
              fontWeight: 600,
              fontSize: '0.9rem',
            }}
          >
            <BarChart3 size={16} />
            <span>Scouting Hub</span>
          </Link>
        </div>
      </div>
    </div>
  );
}
