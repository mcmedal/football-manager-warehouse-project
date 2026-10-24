'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import { 
  ShieldAlert, 
  CheckCircle2, 
  AlertTriangle, 
  ChevronRight, 
  TrendingUp, 
  Users, 
  ArrowRight, 
  Search, 
  Activity,
  Layers,
  Sparkles
} from 'lucide-react';

interface MetricComparison {
  name: string;
  value: number;
  leagueAvg: number;
  unit: string;
  diff: number;
}

interface DepartmentData {
  id: string;
  name: string;
  icon: string;
  count: number;
  avgAge: number;
  status: 'healthy' | 'warning' | 'critical';
  starter: string;
  starterAge: number;
  starterMinutes: number;
  primaryMetric: MetricComparison;
  secondaryMetric: MetricComparison;
  alerts: string[];
  players: any[];
  solutionAction?: string;
  solutionQuery: string;
}

interface DiagnosisData {
  clubName: string;
  league: string;
  totalSquadSize: number;
  averageSquadAge: number;
  squadHealthScore: number;
  departments: DepartmentData[];
}

export default function SquadDiagnosisPage() {
  const [data, setData] = useState<DiagnosisData | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandedDept, setExpandedDept] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/squad-diagnosis')
      .then((res) => res.json())
      .then((d: DiagnosisData) => {
        setData(d);
        setLoading(false);
      })
      .catch((err) => {
        console.error('Diagnosis fetch error:', err);
        setLoading(false);
      });
  }, []);

  const getStatusBadge = (status: 'healthy' | 'warning' | 'critical') => {
    if (status === 'critical') {
      return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem', padding: '3px 10px', borderRadius: '12px', background: 'rgba(239, 68, 68, 0.15)', color: '#f87171', fontSize: '0.75rem', fontWeight: 700, border: '1px solid rgba(239, 68, 68, 0.3)' }}>
          <ShieldAlert size={13} />
          <span>ACTION REQUIRED</span>
        </span>
      );
    }
    if (status === 'warning') {
      return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem', padding: '3px 10px', borderRadius: '12px', background: 'rgba(245, 158, 11, 0.15)', color: '#fbbf24', fontSize: '0.75rem', fontWeight: 700, border: '1px solid rgba(245, 158, 11, 0.3)' }}>
          <AlertTriangle size={13} />
          <span>MONITOR / SUCCESSION</span>
        </span>
      );
    }
    return (
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem', padding: '3px 10px', borderRadius: '12px', background: 'rgba(16, 185, 129, 0.15)', color: '#34d399', fontSize: '0.75rem', fontWeight: 700, border: '1px solid rgba(16, 185, 129, 0.3)' }}>
        <CheckCircle2 size={13} />
        <span>HEALTHY / STABLE</span>
      </span>
    );
  };

  return (
    <div className="container" style={{ paddingBottom: '5rem' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '2rem', flexWrap: 'wrap', gap: '1rem' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.5rem' }}>
            <div style={{ padding: '0.5rem', borderRadius: '10px', background: 'rgba(239, 68, 68, 0.1)', color: '#f87171' }}>
              <Activity size={24} />
            </div>
            <div>
              <h1 style={{ fontSize: '1.75rem', fontWeight: 700, margin: 0, letterSpacing: '-0.02em' }}>
                Squad Tactical Gap &amp; Depth Diagnosis
              </h1>
              <p style={{ margin: 0, fontSize: '0.875rem', color: 'var(--text-dim)' }}>
                Automated 5-department audit for Macará vs LIGA MX benchmarks with direct solution pathways
              </p>
            </div>
          </div>
        </div>

        {data && (
          <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
            <div className="card" style={{ padding: '0.75rem 1.25rem', textAlign: 'center', minWidth: '120px' }}>
              <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>Squad Health</div>
              <div style={{ fontSize: '1.5rem', fontWeight: 800, color: data.squadHealthScore >= 75 ? '#34d399' : data.squadHealthScore >= 60 ? '#fbbf24' : '#f87171' }}>
                {data.squadHealthScore}<span style={{ fontSize: '0.9rem', color: 'var(--text-dim)' }}>/100</span>
              </div>
            </div>
            <div className="card" style={{ padding: '0.75rem 1.25rem', textAlign: 'center', minWidth: '110px' }}>
              <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>Avg Age</div>
              <div style={{ fontSize: '1.5rem', fontWeight: 800, color: '#38bdf8' }}>
                {data.averageSquadAge} <span style={{ fontSize: '0.85rem', color: 'var(--text-dim)' }}>yrs</span>
              </div>
            </div>
          </div>
        )}
      </div>

      {loading ? (
        <div className="card" style={{ padding: '4rem', textAlign: 'center', color: 'var(--text-dim)' }}>
          <div style={{ width: '32px', height: '32px', border: '3px solid rgba(56, 189, 248, 0.2)', borderTopColor: '#38bdf8', borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto 1rem auto' }} />
          Auditing Macará departments and comparing against LIGA MX medians...
        </div>
      ) : data ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          {data.departments.map((dept) => {
            const isExpanded = expandedDept === dept.id;

            return (
              <div
                key={dept.id}
                className="card"
                style={{
                  padding: '1.5rem',
                  borderLeft: `4px solid ${
                    dept.status === 'critical' ? '#ef4444' : dept.status === 'warning' ? '#f59e0b' : '#10b981'
                  }`,
                  background: 'rgba(19, 23, 34, 0.7)',
                }}
              >
                {/* Department Header */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: '1rem', marginBottom: '1.25rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                    <span style={{ fontSize: '1.75rem' }}>{dept.icon}</span>
                    <div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                        <h2 style={{ fontSize: '1.3rem', fontWeight: 700, margin: 0, color: '#fff' }}>
                          {dept.name}
                        </h2>
                        {getStatusBadge(dept.status)}
                      </div>
                      <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)', marginTop: '3px' }}>
                        {dept.count} Players in Roster • Average Department Age: {dept.avgAge} yrs
                      </div>
                    </div>
                  </div>

                  {/* Primary Starter Readout */}
                  <div style={{ textAlign: 'right' }}>
                    <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase', display: 'block' }}>
                      Primary Starter(s)
                    </span>
                    <strong style={{ fontSize: '0.95rem', color: '#fff' }}>
                      {dept.starter}
                    </strong>
                    <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                      {dept.starterMinutes.toLocaleString()} combined mins played
                    </div>
                  </div>
                </div>

                {/* Metrics Benchmark Grid */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '1rem', marginBottom: '1.25rem' }}>
                  {/* Primary Metric Card */}
                  <div style={{ background: 'rgba(0,0,0,0.3)', borderRadius: '8px', padding: '1rem', border: '1px solid rgba(255,255,255,0.05)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>
                        {dept.primaryMetric.name}
                      </span>
                      <span style={{ fontSize: '0.75rem', fontWeight: 700, color: dept.primaryMetric.diff >= 0 ? '#34d399' : '#f87171' }}>
                        {dept.primaryMetric.diff >= 0 ? `+${dept.primaryMetric.diff}` : dept.primaryMetric.diff} vs Lg Avg
                      </span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.75rem' }}>
                      <span style={{ fontSize: '1.4rem', fontWeight: 800, color: '#fff' }}>
                        {dept.primaryMetric.value} {dept.primaryMetric.unit}
                      </span>
                      <span style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
                        League Avg: {dept.primaryMetric.leagueAvg} {dept.primaryMetric.unit}
                      </span>
                    </div>
                  </div>

                  {/* Secondary Metric Card */}
                  <div style={{ background: 'rgba(0,0,0,0.3)', borderRadius: '8px', padding: '1rem', border: '1px solid rgba(255,255,255,0.05)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>
                        {dept.secondaryMetric.name}
                      </span>
                      <span style={{ fontSize: '0.75rem', fontWeight: 700, color: dept.secondaryMetric.diff >= 0 ? '#34d399' : '#f87171' }}>
                        {dept.secondaryMetric.diff >= 0 ? `+${dept.secondaryMetric.diff}` : dept.secondaryMetric.diff} vs Lg Avg
                      </span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.75rem' }}>
                      <span style={{ fontSize: '1.4rem', fontWeight: 800, color: '#fff' }}>
                        {dept.secondaryMetric.value} {dept.secondaryMetric.unit}
                      </span>
                      <span style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
                        League Avg: {dept.secondaryMetric.leagueAvg} {dept.secondaryMetric.unit}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Tactical Alerts List */}
                <div style={{ marginBottom: '1.25rem' }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                    {dept.alerts.map((alert, i) => (
                      <div
                        key={i}
                        style={{
                          padding: '0.65rem 0.85rem',
                          borderRadius: '6px',
                          background: alert.startsWith('🔴') ? 'rgba(239, 68, 68, 0.08)' : alert.startsWith('🟡') ? 'rgba(245, 158, 11, 0.08)' : 'rgba(16, 185, 129, 0.08)',
                          border: `1px solid ${alert.startsWith('🔴') ? '#ef444430' : alert.startsWith('🟡') ? '#f59e0b30' : '#10b98130'}`,
                          fontSize: '0.825rem',
                          color: '#e2e8f0',
                        }}
                      >
                        {alert}
                      </div>
                    ))}
                  </div>
                </div>

                {/* Footer Action Bar: Find Solutions & Expand Roster */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', paddingTop: '1rem', borderTop: '1px solid rgba(255,255,255,0.06)', flexWrap: 'wrap', gap: '0.75rem' }}>
                  <button
                    onClick={() => setExpandedDept(isExpanded ? null : dept.id)}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: 'var(--text-dim)',
                      fontSize: '0.8rem',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.35rem',
                      padding: 0,
                    }}
                  >
                    <span>{isExpanded ? 'Hide Department Roster' : `View All ${dept.count} Players`}</span>
                    <ChevronRight size={14} style={{ transform: isExpanded ? 'rotate(90deg)' : 'none', transition: 'transform 0.2s' }} />
                  </button>

                  <Link
                    href={dept.solutionQuery}
                    className="btn-primary"
                    style={{
                      padding: '0.55rem 1.15rem',
                      fontSize: '0.85rem',
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      textDecoration: 'none',
                      boxShadow: dept.status === 'critical' ? '0 0 16px rgba(239, 68, 68, 0.25)' : '0 0 16px rgba(56, 189, 248, 0.2)',
                    }}
                  >
                    <Sparkles size={14} />
                    <span>{dept.solutionAction || 'Scout Department Solutions'}</span>
                    <ArrowRight size={14} />
                  </Link>
                </div>

                {/* Expanded Roster Drawer */}
                {isExpanded && (
                  <div style={{ marginTop: '1.25rem', paddingTop: '1rem', borderTop: '1px solid rgba(255,255,255,0.08)' }}>
                    <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-dim)', textTransform: 'uppercase', marginBottom: '0.75rem' }}>
                      Current Department Depth Chart
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: '0.75rem' }}>
                      {dept.players.map((p) => (
                        <Link
                          key={p.player_key}
                          href={`/player/${p.player_key}`}
                          style={{
                            padding: '0.75rem',
                            borderRadius: '8px',
                            background: 'rgba(0,0,0,0.3)',
                            border: '1px solid rgba(255,255,255,0.05)',
                            textDecoration: 'none',
                            display: 'block',
                            transition: 'border-color 0.15s ease',
                          }}
                        >
                          <div style={{ fontWeight: 600, fontSize: '0.85rem', color: '#fff' }}>
                            {p.player_name}
                          </div>
                          <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                            {p.position} • Age {p.age}
                          </div>
                          <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '0.5rem', fontSize: '0.7rem' }}>
                            <span style={{ color: '#38bdf8' }}>{p.minutes_played} mins</span>
                            <span style={{ color: '#34d399' }}>{p.transfer_value_m ? `£${p.transfer_value_m}M` : 'Free'}</span>
                          </div>
                        </Link>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
