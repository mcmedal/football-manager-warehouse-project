'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import { 
  Users, 
  TrendingUp, 
  Sparkles, 
  Clock, 
  DollarSign, 
  ShieldAlert, 
  ArrowRight, 
  Star, 
  SlidersHorizontal,
  ChevronRight,
  Award,
  Crosshair,
  Shield,
  Target
} from 'lucide-react';
import { useShortlist } from '@/lib/shortlist';

interface SquadOption {
  player_key: number;
  player_name: string;
  position: string;
  age: number;
  minutes_played: number;
  transfer_value_m: number | null;
  wage_per_week_k: number | null;
  is_gk: number | boolean;
}

interface IncumbentData {
  player_key: number;
  player_name: string;
  position: string;
  club_name: string;
  league: string;
  age: number;
  minutes_played: number;
  transfer_value_m: number | null;
  wage_per_week_k: number | null;
  value_gap: number | null;
  metrics: Record<string, number>;
}

interface SuccessionCandidate {
  player_key: number;
  player_name: string;
  position: string;
  club_name: string;
  league: string;
  age: number;
  minutes_played: number;
  transfer_value_m: number | null;
  wage_per_week_k: number | null;
  value_gap: number | null;
  is_own_squad: boolean;
  similarity_pct: number;
  metrics: Record<string, number>;
  deltas: {
    delta_primary_stat: number;
    season_goal_delta: number;
    primary_label: string;
    season_label: string;
    delta_stat_2: number;
    label_stat_2: string;
    delta_stat_3: number;
    label_stat_3: string;
  };
  financial: {
    wage_savings_annual_k: number;
    value_diff_m: number;
  };
}

export default function SuccessionPage() {
  const [squadOptions, setSquadOptions] = useState<SquadOption[]>([]);
  const [selectedIncumbentKey, setSelectedIncumbentKey] = useState<number | null>(null);
  const [positionMode, setPositionMode] = useState<'exact' | 'compatible' | 'any'>('exact');
  const [maxAge, setMaxAge] = useState<number>(24);
  const [maxVal, setMaxVal] = useState<string>('');
  const [maxWage, setMaxWage] = useState<string>('');
  const [minMinutes, setMinMinutes] = useState<number>(450);

  const [loading, setLoading] = useState<boolean>(true);
  const [incumbent, setIncumbent] = useState<IncumbentData | null>(null);
  const [candidates, setCandidates] = useState<SuccessionCandidate[]>([]);
  const [inHouseOptions, setInHouseOptions] = useState<SuccessionCandidate[]>([]);
  const [isGk, setIsGk] = useState<boolean>(false);

  const { toggle: toggleShortlist, isShortlisted } = useShortlist();

  // Load squad options on mount
  useEffect(() => {
    fetch('/api/succession?action=squad_options')
      .then((res) => res.json())
      .then((data) => {
        if (data.squad && data.squad.length > 0) {
          setSquadOptions(data.squad);
          setSelectedIncumbentKey(data.squad[0].player_key);
        }
      })
      .catch((err) => console.error('Failed to load squad options:', err));
  }, []);

  // Fetch succession candidates whenever incumbent or filters change
  useEffect(() => {
    if (!selectedIncumbentKey) return;
    setLoading(true);

    const params = new URLSearchParams({
      incumbentKey: selectedIncumbentKey.toString(),
      positionMode,
      maxAge: maxAge.toString(),
      minMinutes: minMinutes.toString(),
      limit: '20',
    });

    if (maxVal) params.append('maxVal', maxVal);
    if (maxWage) params.append('maxWage', maxWage);

    fetch(`/api/succession?${params.toString()}`)
      .then((res) => res.json())
      .then((data) => {
        setIncumbent(data.incumbent || null);
        setCandidates(data.candidates || []);
        setInHouseOptions(data.inHouseOptions || []);
        setIsGk(!!data.isGk);
        setLoading(false);
      })
      .catch((err) => {
        console.error('Succession fetch error:', err);
        setLoading(false);
      });
  }, [selectedIncumbentKey, positionMode, maxAge, maxVal, maxWage, minMinutes]);

  const renderCandidateCard = (c: SuccessionCandidate, index: number, isInternal: boolean = false) => {
    const isPositiveSeason = c.deltas.season_goal_delta >= 0;
    const isShort = isShortlisted(c.player_key);

    return (
      <div
        key={c.player_key}
        className="card"
        style={{
          padding: '1.25rem 1.5rem',
          background: isInternal
            ? 'linear-gradient(135deg, rgba(56, 189, 248, 0.08), rgba(19, 23, 34, 0.85))'
            : index === 0
            ? 'linear-gradient(135deg, rgba(16, 185, 129, 0.06), rgba(19, 23, 34, 0.8))'
            : 'rgba(19, 23, 34, 0.6)',
          border: isInternal
            ? '1px solid rgba(56, 189, 248, 0.4)'
            : index === 0
            ? '1px solid rgba(16, 185, 129, 0.4)'
            : '1px solid rgba(255,255,255,0.06)',
          display: 'grid',
          gridTemplateColumns: 'minmax(220px, 1.2fr) minmax(200px, 1.3fr) minmax(180px, 1fr) auto',
          gap: '1.5rem',
          alignItems: 'center',
        }}
      >
        {/* Candidate Identity */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.4rem', flexWrap: 'wrap' }}>
            {isInternal ? (
              <span style={{ fontSize: '0.7rem', fontWeight: 800, padding: '2px 8px', borderRadius: '12px', background: 'rgba(56, 189, 248, 0.2)', color: '#38bdf8', border: '1px solid rgba(56, 189, 248, 0.3)' }}>
                🛡️ MACARÁ SQUAD OPTION
              </span>
            ) : (
              <span style={{ fontSize: '0.75rem', fontWeight: 700, padding: '2px 8px', borderRadius: '12px', background: 'rgba(255,255,255,0.06)', color: '#38bdf8' }}>
                #{index + 1} Target
              </span>
            )}

            {/* Position badge */}
            <span
              style={{
                fontSize: '0.7rem',
                fontWeight: 700,
                padding: '2px 8px',
                borderRadius: '12px',
                background: c.position === incumbent?.position ? 'rgba(16, 185, 129, 0.15)' : 'rgba(255,255,255,0.08)',
                color: c.position === incumbent?.position ? '#34d399' : '#fff',
                border: c.position === incumbent?.position ? '1px solid rgba(16, 185, 129, 0.3)' : 'none',
              }}
            >
              {c.position === incumbent?.position ? `🎯 ${c.position}` : c.position}
            </span>
          </div>

          <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>
            {c.player_name}
          </div>
          <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)', marginTop: '2px' }}>
            {c.position} • {c.club_name} ({c.league})
          </div>
          <div style={{ display: 'flex', gap: '0.75rem', marginTop: '0.5rem', fontSize: '0.75rem' }}>
            <span style={{ color: '#34d399', fontWeight: 600 }}>Age {c.age}</span>
            <span style={{ color: 'var(--text-dim)' }}>•</span>
            <span style={{ color: '#38bdf8' }}>{c.transfer_value_m ? `£${c.transfer_value_m}M` : 'Free'}</span>
            <span style={{ color: 'var(--text-dim)' }}>•</span>
            <span style={{ color: 'var(--text-dim)' }}>{c.wage_per_week_k ? `£${c.wage_per_week_k}k/wk` : '-'}</span>
          </div>
        </div>

        {/* Season Upgrade Impact */}
        <div style={{ background: 'rgba(0,0,0,0.3)', padding: '0.85rem 1rem', borderRadius: '10px', border: '1px solid rgba(255,255,255,0.05)' }}>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase', marginBottom: '0.25rem' }}>
            {c.deltas.season_label}
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.5rem' }}>
            <span style={{ fontSize: '1.6rem', fontWeight: 800, color: isPositiveSeason ? '#34d399' : '#f87171' }}>
              {isPositiveSeason ? `+${c.deltas.season_goal_delta}` : c.deltas.season_goal_delta}
            </span>
            <span style={{ fontSize: '0.75rem', color: isPositiveSeason ? '#34d399' : '#f87171', fontWeight: 600 }}>
              {isGk ? 'goals saved / season' : 'goal involvement / season'}
            </span>
          </div>

          {/* Sub-deltas */}
          <div style={{ display: 'flex', gap: '1rem', marginTop: '0.5rem', paddingTop: '0.5rem', borderTop: '1px solid rgba(255,255,255,0.06)', fontSize: '0.75rem' }}>
            <div>
              <span style={{ color: 'var(--text-dim)', display: 'block', fontSize: '0.65rem' }}>{c.deltas.label_stat_2}</span>
              <strong style={{ color: c.deltas.delta_stat_2 >= 0 ? '#34d399' : '#f87171' }}>
                {c.deltas.delta_stat_2 >= 0 ? `+${c.deltas.delta_stat_2}` : c.deltas.delta_stat_2}
              </strong>
            </div>
            <div>
              <span style={{ color: 'var(--text-dim)', display: 'block', fontSize: '0.65rem' }}>{c.deltas.label_stat_3}</span>
              <strong style={{ color: c.deltas.delta_stat_3 >= 0 ? '#34d399' : '#f87171' }}>
                {c.deltas.delta_stat_3 >= 0 ? `+${c.deltas.delta_stat_3}` : c.deltas.delta_stat_3}
              </strong>
            </div>
          </div>
        </div>

        {/* Financial & Similarity Matrix */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.35rem' }}>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>Profile Match</span>
            <strong style={{ fontSize: '0.85rem', color: '#38bdf8' }}>{c.similarity_pct}%</strong>
          </div>
          <div style={{ width: '100%', height: '6px', background: 'rgba(255,255,255,0.1)', borderRadius: '3px', overflow: 'hidden', marginBottom: '0.75rem' }}>
            <div style={{ width: `${c.similarity_pct}%`, height: '100%', background: 'linear-gradient(90deg, #38bdf8, #34d399)', borderRadius: '3px' }} />
          </div>

          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem' }}>
            <span style={{ color: 'var(--text-dim)' }}>Annual Wage Diff:</span>
            <strong style={{ color: c.financial.wage_savings_annual_k >= 0 ? '#34d399' : '#f87171' }}>
              {c.financial.wage_savings_annual_k >= 0 ? `Save £${c.financial.wage_savings_annual_k}k/yr` : `+£${Math.abs(c.financial.wage_savings_annual_k)}k/yr`}
            </strong>
          </div>

          {c.value_gap !== null && (
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem', marginTop: '0.25rem' }}>
              <span style={{ color: 'var(--text-dim)' }}>Value Gap:</span>
              <strong style={{ color: c.value_gap >= 0 ? '#34d399' : '#f87171' }}>
                {c.value_gap >= 0 ? `+${c.value_gap.toFixed(1)}` : c.value_gap.toFixed(1)}
              </strong>
            </div>
          )}
        </div>

        {/* Action Buttons */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem', minWidth: '130px' }}>
          <button
            onClick={() => {
              toggleShortlist({
                player_key: c.player_key,
                player_name: c.player_name,
                position: c.position,
                club_name: c.club_name,
                league: c.league,
                age: c.age,
                minutes_played: c.minutes_played,
                transfer_value_m: c.transfer_value_m ?? undefined,
                wage_per_week_k: c.wage_per_week_k ?? undefined,
                primary_stat: `${c.deltas.season_label}: ${c.deltas.season_goal_delta}`,
                added_at: new Date().toISOString(),
                notes: `Successor for ${incumbent?.player_name} (${c.position}, ${c.similarity_pct}% match)`,
              });
            }}
            style={{
              padding: '0.45rem 0.65rem',
              borderRadius: '6px',
              border: isShort ? '1px solid #f59e0b' : '1px solid rgba(255,255,255,0.15)',
              background: isShort ? 'rgba(245, 158, 11, 0.15)' : 'rgba(255,255,255,0.04)',
              color: isShort ? '#f59e0b' : '#fff',
              fontSize: '0.75rem',
              fontWeight: 600,
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '0.35rem',
            }}
          >
            <Star size={14} fill={isShort ? '#f59e0b' : 'none'} />
            <span>{isShort ? 'Shortlisted' : 'Shortlist'}</span>
          </button>

          {incumbent && (
            <Link
              href={`/compare?p1=${incumbent.player_key}&p2=${c.player_key}`}
              style={{
                padding: '0.45rem 0.65rem',
                borderRadius: '6px',
                border: '1px solid rgba(56, 189, 248, 0.3)',
                background: 'rgba(56, 189, 248, 0.08)',
                color: '#38bdf8',
                fontSize: '0.75rem',
                fontWeight: 600,
                textAlign: 'center',
                textDecoration: 'none',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: '0.35rem',
              }}
            >
              <Crosshair size={14} />
              <span>Compare</span>
            </Link>
          )}

          <Link
            href={`/player/${c.player_key}`}
            style={{
              padding: '0.45rem 0.65rem',
              borderRadius: '6px',
              background: 'rgba(255,255,255,0.06)',
              color: 'var(--text-dim)',
              fontSize: '0.75rem',
              textAlign: 'center',
              textDecoration: 'none',
            }}
          >
            Profile
          </Link>
        </div>
      </div>
    );
  };

  return (
    <div className="container" style={{ paddingBottom: '5rem' }}>
      {/* Header */}
      <div style={{ marginBottom: '2rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.5rem' }}>
          <div style={{ padding: '0.5rem', borderRadius: '10px', background: 'rgba(16, 185, 129, 0.1)', color: '#34d399' }}>
            <Users size={24} />
          </div>
          <div>
            <h1 style={{ fontSize: '1.75rem', fontWeight: 700, margin: 0, letterSpacing: '-0.02em' }}>
              Squad Succession &amp; Replacement Engine
            </h1>
            <p style={{ margin: 0, fontSize: '0.875rem', color: 'var(--text-dim)' }}>
              Identify internal and external replacements that play the same position with 38-game season upgrade deltas
            </p>
          </div>
        </div>
      </div>

      {/* Top Section: Incumbent Selection & Filter Panel */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '1.5rem', marginBottom: '2rem' }}>
        {/* Incumbent Player Selector Card */}
        <div className="card" style={{ padding: '1.5rem', background: 'rgba(19, 23, 34, 0.7)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <label style={{ fontSize: '0.8rem', fontWeight: 700, color: '#38bdf8', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              1. Select Player to Replace
            </label>
            <span style={{ fontSize: '0.75rem', color: '#10b981', fontWeight: 600 }}>
              Macará Squad
            </span>
          </div>

          <select
            value={selectedIncumbentKey || ''}
            onChange={(e) => setSelectedIncumbentKey(parseInt(e.target.value, 10))}
            style={{
              width: '100%',
              padding: '0.65rem 0.85rem',
              borderRadius: '8px',
              background: 'rgba(0,0,0,0.5)',
              border: '1px solid rgba(56, 189, 248, 0.3)',
              color: '#fff',
              fontSize: '0.9rem',
              fontWeight: 600,
              marginBottom: '1.25rem',
            }}
          >
            {squadOptions.map((p) => (
              <option key={p.player_key} value={p.player_key}>
                {p.player_name} ({p.position}, Age {p.age}) — {p.minutes_played} mins {p.age >= 29 ? '⚠️ AGING CORE' : ''}
              </option>
            ))}
          </select>

          {/* Current Incumbent Bio */}
          {incumbent && (
            <div style={{ background: 'rgba(0,0,0,0.3)', borderRadius: '10px', padding: '1rem', border: '1px solid rgba(255,255,255,0.06)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.75rem' }}>
                <div>
                  <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>
                    {incumbent.player_name}
                  </div>
                  <div style={{ fontSize: '0.8rem', color: '#38bdf8', marginTop: '2px', fontWeight: 600 }}>
                    🎯 Primary Position: {incumbent.position}
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                    {incumbent.club_name} ({incumbent.league})
                  </div>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: '0.85rem', fontWeight: 700, color: incumbent.age >= 30 ? '#f87171' : '#f59e0b' }}>
                    Age {incumbent.age}
                  </div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>
                    {incumbent.minutes_played} mins
                  </div>
                </div>
              </div>

              {/* Financial & Output stats */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem', textAlign: 'center', paddingTop: '0.75rem', borderTop: '1px solid rgba(255,255,255,0.08)' }}>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>Market Value</span>
                  <strong style={{ fontSize: '0.85rem', color: '#38bdf8' }}>
                    {incumbent.transfer_value_m ? `£${incumbent.transfer_value_m}M` : 'Free'}
                  </strong>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>Wage</span>
                  <strong style={{ fontSize: '0.85rem', color: '#fff' }}>
                    {incumbent.wage_per_week_k ? `£${incumbent.wage_per_week_k}k/wk` : '-'}
                  </strong>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>
                    {isGk ? 'PSxG Net/90' : 'NP-xGI/90'}
                  </span>
                  <strong style={{ fontSize: '0.85rem', color: '#34d399' }}>
                    {isGk 
                      ? (incumbent.metrics.psxg_net_per90?.toFixed(2) || '0.00') 
                      : (incumbent.metrics.npxg_plus_xa_per90?.toFixed(2) || '0.00')}
                  </strong>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Succession Parameters & Filters */}
        <div className="card" style={{ padding: '1.5rem', background: 'rgba(19, 23, 34, 0.7)' }}>
          <div style={{ fontSize: '0.8rem', fontWeight: 700, color: '#34d399', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <SlidersHorizontal size={14} />
            <span>2. Succession Target Criteria</span>
          </div>

          {/* Position Matching Mode Selector */}
          <div style={{ marginBottom: '1.25rem' }}>
            <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600, marginBottom: '0.4rem' }}>
              POSITION MATCHING SCOPE
            </label>
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              <button
                onClick={() => setPositionMode('exact')}
                style={{
                  padding: '0.45rem 0.85rem',
                  borderRadius: '20px',
                  fontSize: '0.8rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  border: positionMode === 'exact' ? '1px solid #10b981' : '1px solid rgba(255,255,255,0.08)',
                  background: positionMode === 'exact' ? 'rgba(16, 185, 129, 0.2)' : 'rgba(255,255,255,0.02)',
                  color: positionMode === 'exact' ? '#34d399' : 'var(--text-dim)',
                  transition: 'all 0.15s ease',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                }}
              >
                <span>🎯 Same Position Only: {incumbent?.position || ''}</span>
              </button>

              <button
                onClick={() => setPositionMode('compatible')}
                style={{
                  padding: '0.45rem 0.85rem',
                  borderRadius: '20px',
                  fontSize: '0.8rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  border: positionMode === 'compatible' ? '1px solid #38bdf8' : '1px solid rgba(255,255,255,0.08)',
                  background: positionMode === 'compatible' ? 'rgba(56, 189, 248, 0.2)' : 'rgba(255,255,255,0.02)',
                  color: positionMode === 'compatible' ? '#38bdf8' : 'var(--text-dim)',
                  transition: 'all 0.15s ease',
                }}
              >
                <span>🌐 Compatible Roles</span>
              </button>

              <button
                onClick={() => setPositionMode('any')}
                style={{
                  padding: '0.45rem 0.85rem',
                  borderRadius: '20px',
                  fontSize: '0.8rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  border: positionMode === 'any' ? '1px solid #a855f7' : '1px solid rgba(255,255,255,0.08)',
                  background: positionMode === 'any' ? 'rgba(168, 85, 247, 0.2)' : 'rgba(255,255,255,0.02)',
                  color: positionMode === 'any' ? '#c084fc' : 'var(--text-dim)',
                  transition: 'all 0.15s ease',
                }}
              >
                <span>All Roles</span>
              </button>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
            {/* Max Age Slider */}
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.35rem' }}>
                <label style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600 }}>MAX AGE</label>
                <span style={{ fontSize: '0.75rem', color: '#34d399', fontWeight: 700 }}>Under {maxAge} yrs</span>
              </div>
              <input
                type="range"
                min="18"
                max="28"
                step="1"
                value={maxAge}
                onChange={(e) => setMaxAge(parseInt(e.target.value, 10))}
                style={{ width: '100%', accentColor: '#34d399', cursor: 'pointer' }}
              />
              <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>
                {incumbent ? `${incumbent.age - maxAge} yrs younger than incumbent` : ''}
              </span>
            </div>

            {/* Min Minutes */}
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.35rem' }}>
                <label style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600 }}>MIN MINUTES</label>
                <span style={{ fontSize: '0.75rem', color: '#38bdf8', fontWeight: 700 }}>{minMinutes} mins</span>
              </div>
              <input
                type="range"
                min="180"
                max="1800"
                step="90"
                value={minMinutes}
                onChange={(e) => setMinMinutes(parseInt(e.target.value, 10))}
                style={{ width: '100%', accentColor: '#38bdf8', cursor: 'pointer' }}
              />
              <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>Reliable sample size</span>
            </div>

            {/* Max Transfer Value */}
            <div>
              <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
                BUDGET CEILING (£M)
              </label>
              <input
                type="number"
                placeholder="Unlimited"
                value={maxVal}
                onChange={(e) => setMaxVal(e.target.value)}
                style={{
                  width: '100%',
                  padding: '0.5rem 0.75rem',
                  borderRadius: '6px',
                  background: 'rgba(0,0,0,0.4)',
                  border: '1px solid rgba(255,255,255,0.1)',
                  color: '#fff',
                  fontSize: '0.85rem',
                }}
              />
            </div>

            {/* Max Wage */}
            <div>
              <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
                WAGE CEILING (£k/wk)
              </label>
              <input
                type="number"
                placeholder="Unlimited"
                value={maxWage}
                onChange={(e) => setMaxWage(e.target.value)}
                style={{
                  width: '100%',
                  padding: '0.5rem 0.75rem',
                  borderRadius: '6px',
                  background: 'rgba(0,0,0,0.4)',
                  border: '1px solid rgba(255,255,255,0.1)',
                  color: '#fff',
                  fontSize: '0.85rem',
                }}
              />
            </div>
          </div>
        </div>
      </div>

      {/* SECTION 1: IN-HOUSE SQUAD OPTIONS (SAME POSITION) */}
      {inHouseOptions.length > 0 && (
        <div style={{ marginBottom: '2.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', marginBottom: '1rem' }}>
            <Shield size={18} style={{ color: '#38bdf8' }} />
            <h2 style={{ fontSize: '1.25rem', fontWeight: 700, margin: 0, color: '#fff' }}>
              In-House Squad Options ({inHouseOptions.length}) — Same Position: {incumbent?.position}
            </h2>
            <span style={{ fontSize: '0.75rem', padding: '2px 8px', borderRadius: '12px', background: 'rgba(56, 189, 248, 0.15)', color: '#38bdf8', fontWeight: 600 }}>
              Macará Roster Depth
            </span>
          </div>
          <p style={{ margin: '0 0 1rem 0', fontSize: '0.85rem', color: 'var(--text-dim)' }}>
            Players already under contract at Macará who play as a {incumbent?.position}. Evaluate whether an external signing is necessary.
          </p>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {inHouseOptions.map((c, idx) => renderCandidateCard(c, idx, true))}
          </div>
        </div>
      )}

      {/* SECTION 2: EXTERNAL RECRUITMENT TARGETS (SAME POSITION) */}
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.25rem', flexWrap: 'wrap', gap: '0.75rem' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
              <Target size={18} style={{ color: '#34d399' }} />
              <h2 style={{ fontSize: '1.25rem', fontWeight: 700, margin: 0 }}>
                External Transfer Market Targets ({candidates.length})
              </h2>
            </div>
            <p style={{ margin: '3px 0 0 0', fontSize: '0.8rem', color: 'var(--text-dim)' }}>
              Showing targets playing in {positionMode === 'exact' ? `the same position (${incumbent?.position})` : positionMode === 'compatible' ? 'compatible tactical roles' : 'any role'} ranked by season performance upgrade
            </p>
          </div>

          <div style={{ display: 'flex', gap: '0.5rem', fontSize: '0.75rem', color: 'var(--text-dim)' }}>
            <span style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
              <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', background: '#10b981' }} />
              Net Positive Season Upgrade
            </span>
          </div>
        </div>

        {loading ? (
          <div className="card" style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-dim)' }}>
            <div style={{ width: '32px', height: '32px', border: '3px solid rgba(56, 189, 248, 0.2)', borderTopColor: '#38bdf8', borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto 1rem auto' }} />
            Filtering players who play in the same position ({incumbent?.position})...
          </div>
        ) : candidates.length === 0 ? (
          <div className="card" style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-dim)' }}>
            <p style={{ margin: '0 0 0.5rem 0', fontSize: '0.95rem', color: '#fff' }}>
              No external candidates playing as {incumbent?.position} matched the current age (&le; {maxAge}) or budget thresholds.
            </p>
            <p style={{ margin: 0, fontSize: '0.8rem', color: 'var(--text-dim)' }}>
              Try switching to &quot;Compatible Roles&quot; or increasing the Max Age slider.
            </p>
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {candidates.map((c, index) => renderCandidateCard(c, index, false))}
          </div>
        )}
      </div>
    </div>
  );
}
