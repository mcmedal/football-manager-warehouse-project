'use client';

import React, { useState, useEffect, useMemo, useRef } from 'react';
import Link from 'next/link';
import { 
  ScatterChart, 
  Sparkles, 
  Crosshair, 
  Filter, 
  Star, 
  Search, 
  ExternalLink, 
  ChevronRight, 
  Info, 
  TrendingUp,
  Layers,
  Shield,
  Award,
  RotateCcw,
  Globe,
  Coins,
  DollarSign
} from 'lucide-react';
import { useShortlist } from '@/lib/shortlist';

interface ScatterPlayer {
  player_key: number;
  player_name: string;
  position: string;
  club_name: string;
  league: string;
  league_difficulty_coef?: number;
  age: number;
  minutes_played: number;
  transfer_value_m: number | null;
  wage_per_week_k: number | null;
  value_gap: number | null;
  is_own_squad: number | boolean;
  x: number;
  y: number;
}

interface ScatterData {
  xField: string;
  yField: string;
  cohortSize: number;
  sampleSize?: number;
  xMedian: number;
  yMedian: number;
  xMean: number;
  yMean: number;
  xMin: number;
  xMax: number;
  yMin: number;
  yMax: number;
  players: ScatterPlayer[];
}

const OUTFIELD_METRIC_OPTIONS = [
  { group: 'Finishing & Goal Threat', options: [
    { value: 'goals_per90', label: 'Goals / 90' },
    { value: 'non_penalty_xGoals_per90', label: 'Non-Penalty xG / 90' },
    { value: 'goals_minus_xg_per90', label: 'Goals - xG / 90 (Finishing Delta)' },
    { value: 'finishing_overperf_shrunk', label: 'Shrunk Finishing Overperf' },
    { value: 'shots_per90', label: 'Shots / 90' },
    { value: 'shot_on_target_rate', label: 'Shot on Target %' },
    { value: 'conversion_rate', label: 'Shot Conversion %' },
    { value: 'npxg_per_shot', label: 'NP-xG / Shot (Shot Quality)' },
    { value: 'box_presence_proxy', label: 'Box Presence Proxy' },
  ]},
  { group: 'Creation & Passing', options: [
    { value: 'assists_per90', label: 'Assists / 90' },
    { value: 'xAssists_per90', label: 'Expected Assists (xA) / 90' },
    { value: 'npxg_plus_xa_per90', label: 'NP-xG + xA / 90' },
    { value: 'open_play_key_passes_per90', label: 'Open-Play Key Passes / 90' },
    { value: 'progressive_passes_per90', label: 'Progressive Passes / 90' },
    { value: 'progressive_actions_per90', label: 'Total Progressive Actions / 90' },
    { value: 'completed_passes_per90', label: 'Completed Passes / 90' },
    { value: 'pass_accuracy', label: 'Pass Accuracy %' },
    { value: 'completed_crosses_per90', label: 'Completed Crosses / 90' },
    { value: 'crosses_share_of_creation', label: 'Cross Share of Creation' },
    { value: 'assist_luck_shrunk', label: 'Shrunk Assist Luck' },
  ]},
  { group: 'Progression & Ball Carrying', options: [
    { value: 'dribbles_made_per90', label: 'Dribbles Completed / 90' },
    { value: 'turnover_rate', label: 'Turnover Rate %' },
    { value: 'net_ball_security', label: 'Net Ball Security' },
    { value: 'distance_covered_km_per90', label: 'Distance Covered (km/90)' },
    { value: 'sprints_per90', label: 'Sprints / 90' },
  ]},
  { group: 'Defending & Pressing', options: [
    { value: 'defensive_actions_per90', label: 'Total Defensive Actions / 90' },
    { value: 'non_clearance_def_actions_per90', label: 'Non-Clearance Def Actions / 90' },
    { value: 'tackles_attempted_per90', label: 'Tackles Attempted / 90' },
    { value: 'tackle_accuracy', label: 'Tackle Win %' },
    { value: 'interceptions_per90', label: 'Interceptions / 90' },
    { value: 'clearances_per90', label: 'Clearances / 90' },
    { value: 'aerial_duels_won_per90', label: 'Aerial Duels Won / 90' },
    { value: 'pressures_attempted_per90', label: 'Pressures Attempted / 90' },
    { value: 'pressure_efficiency', label: 'Pressure Efficiency' },
  ]},
  { group: 'Valuation & Moneyball', options: [
    { value: 'value_gap', label: 'Value Gap (Output %ile - Value %ile)' },
    { value: 'transfer_value_m', label: 'Transfer Value (£M)' },
    { value: 'wage_per_week_k', label: 'Wage (£k/wk)' },
    { value: 'league_difficulty_coef', label: 'League Difficulty Coef (x)' },
    { value: 'minutes_played', label: 'Minutes Played' },
    { value: 'age', label: 'Age' },
  ]}
];

const GK_METRIC_OPTIONS = [
  { group: 'Shot Stopping', options: [
    { value: 'save_pct', label: 'Save %' },
    { value: 'psxg_net_per90', label: 'Goals Prevented / 90' },
    { value: 'xgp_per_shot', label: 'Goals Prevented / Shot' },
    { value: 'xgot_faced_per90', label: 'xGOT Faced / 90 (Workload)' },
    { value: 'hold_rate_pct', label: 'Catch / Hold Rate %' },
    { value: 'hold_rate', label: 'Hold Rate' },
  ]},
  { group: 'Command of Area & Sweeping', options: [
    { value: 'sweeper_actions_per90', label: 'Sweeper Actions / 90' },
    { value: 'sweeper_share', label: 'Sweeper Share of Actions' },
    { value: 'clearances_per90', label: 'Clearances / 90' },
    { value: 'interceptions_per90', label: 'Interceptions / 90' },
  ]},
  { group: 'Distribution & Reliability', options: [
    { value: 'completed_passes_per90', label: 'Completed Passes / 90' },
    { value: 'pass_accuracy', label: 'Pass Accuracy %' },
    { value: 'distribution_value', label: 'Distribution Value Proxy' },
    { value: 'turnovers_per90', label: 'Turnovers / 90' },
    { value: 'errors_leading_to_goal_per90', label: 'Errors Leading to Goal / 90' },
  ]},
  { group: 'Valuation & Moneyball', options: [
    { value: 'value_gap', label: 'Value Gap (Output %ile - Value %ile)' },
    { value: 'transfer_value_m', label: 'Transfer Value (£M)' },
    { value: 'wage_per_week_k', label: 'Wage (£k/wk)' },
    { value: 'league_difficulty_coef', label: 'League Difficulty Coef (x)' },
    { value: 'minutes_played', label: 'Minutes Played' },
    { value: 'age', label: 'Age' },
  ]}
];

interface Preset {
  name: string;
  x: string;
  y: string;
  desc: string;
  mode: 'outfield' | 'gk';
}

const PRESETS: Preset[] = [
  { name: 'Finishing Overperformance', x: 'non_penalty_xGoals_per90', y: 'goals_per90', desc: 'Identifies elite finishers vs xG underperformers', mode: 'outfield' },
  { name: 'Creation vs Threat', x: 'open_play_key_passes_per90', y: 'npxg_plus_xa_per90', desc: 'Dual-threat offensive dynamos', mode: 'outfield' },
  { name: 'Progression vs Security', x: 'progressive_actions_per90', y: 'net_ball_security', desc: 'High ball carriers who protect possession', mode: 'outfield' },
  { name: 'Defensive Work vs Efficiency', x: 'tackles_attempted_per90', y: 'tackle_accuracy', desc: 'High volume ball-winners who win duels cleanly', mode: 'outfield' },
  { name: 'Moneyball Value Gap', x: 'transfer_value_m', y: 'value_gap', desc: 'Top output players available at bargain valuations', mode: 'outfield' },
  { name: 'Wage vs Threat', x: 'wage_per_week_k', y: 'npxg_plus_xa_per90', desc: 'Attacking production relative to weekly wage burden', mode: 'outfield' },
  { name: 'League Level vs Output', x: 'league_difficulty_coef', y: 'npxg_plus_xa_per90', desc: 'Performance normalized across league difficulty levels', mode: 'outfield' },
  // GK Presets
  { name: 'GK: Shot Stopping Mastery', x: 'psxg_net_per90', y: 'save_pct', desc: 'Goals prevented vs raw save percentage', mode: 'gk' },
  { name: 'GK: Sweeper-Keeper Index', x: 'sweeper_actions_per90', y: 'psxg_net_per90', desc: 'High sweeping presence combined with reliable shot stopping', mode: 'gk' },
  { name: 'GK: Handling & Catch Security', x: 'hold_rate_pct', y: 'psxg_net_per90', desc: 'Handling security vs shot-stopping impact', mode: 'gk' },
  { name: 'GK: Moneyball Value Gap', x: 'transfer_value_m', y: 'value_gap', desc: 'Goalkeepers with positive goals prevented at low valuations', mode: 'gk' },
  { name: 'GK: Wage vs Prevention', x: 'wage_per_week_k', y: 'psxg_net_per90', desc: 'Goals prevented relative to salary cost', mode: 'gk' },
];

export default function ScatterPage() {
  const [mode, setMode] = useState<'outfield' | 'gk'>('outfield');
  const [xMetric, setXMetric] = useState('non_penalty_xGoals_per90');
  const [yMetric, setYMetric] = useState('goals_per90');
  const [posGroup, setPosGroup] = useState('ALL');
  const [minMinutes, setMinMinutes] = useState(450);
  const [debouncedMinMinutes, setDebouncedMinMinutes] = useState(450);
  const [maxAge, setMaxAge] = useState<number | ''>('');
  const [minCoef, setMinCoef] = useState<string>('ALL');
  const [maxPrice, setMaxPrice] = useState<string>('ALL');
  const [maxWage, setMaxWage] = useState<string>('ALL');
  const [highlightOwnSquad, setHighlightOwnSquad] = useState(true);
  const [searchPlayer, setSearchPlayer] = useState('');

  const [loading, setLoading] = useState(true);
  const [scatterData, setScatterData] = useState<ScatterData | null>(null);
  const [hoveredPlayer, setHoveredPlayer] = useState<ScatterPlayer | null>(null);
  const [selectedPlayer, setSelectedPlayer] = useState<ScatterPlayer | null>(null);

  const { shortlist, toggle: toggleShortlist, isShortlisted } = useShortlist();

  // Debounce minMinutes so range slider interaction feels instant and doesn't overwhelm the backend
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedMinMinutes(minMinutes);
    }, 250);
    return () => clearTimeout(timer);
  }, [minMinutes]);

  const hasActiveFilters = 
    minCoef !== 'ALL' || 
    maxPrice !== 'ALL' || 
    maxWage !== 'ALL' || 
    maxAge !== '' || 
    posGroup !== 'ALL' || 
    minMinutes !== 450 || 
    searchPlayer !== '';

  const resetFilters = () => {
    setMinCoef('ALL');
    setMaxPrice('ALL');
    setMaxWage('ALL');
    setMaxAge('');
    setPosGroup('ALL');
    setMinMinutes(450);
    setSearchPlayer('');
  };

  // Reset metrics when switching modes
  const handleModeChange = (newMode: 'outfield' | 'gk') => {
    setMode(newMode);
    if (newMode === 'gk') {
      setXMetric('psxg_net_per90');
      setYMetric('save_pct');
    } else {
      setXMetric('non_penalty_xGoals_per90');
      setYMetric('goals_per90');
    }
  };

  const applyPreset = (p: Preset) => {
    if (p.mode !== mode) {
      setMode(p.mode);
    }
    setXMetric(p.x);
    setYMetric(p.y);
  };

  // Fetch scatter data
  useEffect(() => {
    let isMounted = true;
    setLoading(true);

    const params = new URLSearchParams({
      mode,
      x: xMetric,
      y: yMetric,
      posGroup,
      minMinutes: debouncedMinMinutes.toString(),
      limit: '500',
    });

    if (maxAge !== '') {
      params.append('maxAge', maxAge.toString());
    }
    if (minCoef !== 'ALL') {
      params.append('minCoef', minCoef);
    }
    if (maxPrice !== 'ALL') {
      params.append('maxPrice', maxPrice);
    }
    if (maxWage !== 'ALL') {
      params.append('maxWage', maxWage);
    }
    if (searchPlayer.trim().length >= 2) {
      params.append('search', searchPlayer.trim());
    }

    fetch(`/api/scatter?${params.toString()}`)
      .then((res) => res.json())
      .then((data: ScatterData) => {
        if (isMounted) {
          setScatterData(data);
          setLoading(false);
        }
      })
      .catch((err) => {
        console.error('Scatter fetch error:', err);
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [mode, xMetric, yMetric, posGroup, debouncedMinMinutes, maxAge, minCoef, maxPrice, maxWage, searchPlayer]);

  // Label lookups
  const metricOptions = mode === 'gk' ? GK_METRIC_OPTIONS : OUTFIELD_METRIC_OPTIONS;
  const getMetricLabel = (key: string) => {
    for (const group of metricOptions) {
      const found = group.options.find((o) => o.value === key);
      if (found) return found.label;
    }
    return key;
  };

  // SVG dimensions
  const svgWidth = 840;
  const svgHeight = 540;
  const padding = { top: 40, right: 40, bottom: 60, left: 70 };
  const plotWidth = svgWidth - padding.left - padding.right;
  const plotHeight = svgHeight - padding.top - padding.bottom;

  // Scale calculations
  const scales = useMemo(() => {
    if (!scatterData || scatterData.players.length === 0) {
      return null;
    }

    const { xMin, xMax, yMin, yMax, xMedian, yMedian } = scatterData;

    // Add 8% margin around min/max to avoid clipping dots at the boundary
    const xSpan = xMax - xMin || 1;
    const ySpan = yMax - yMin || 1;
    const xDomainMin = xMin - xSpan * 0.05;
    const xDomainMax = xMax + xSpan * 0.05;
    const yDomainMin = yMin - ySpan * 0.05;
    const yDomainMax = yMax + ySpan * 0.05;

    const scaleX = (val: number) => {
      return padding.left + ((val - xDomainMin) / (xDomainMax - xDomainMin)) * plotWidth;
    };

    const scaleY = (val: number) => {
      return padding.top + plotHeight - ((val - yDomainMin) / (yDomainMax - yDomainMin)) * plotHeight;
    };

    return {
      scaleX,
      scaleY,
      medianXCoord: scaleX(xMedian),
      medianYCoord: scaleY(yMedian),
      xDomainMin,
      xDomainMax,
      yDomainMin,
      yDomainMax,
    };
  }, [scatterData, plotWidth, plotHeight, padding.left, padding.top]);

  // Top Q1 & Value Gap players
  const { topQ1, topValueGaps } = useMemo(() => {
    if (!scatterData) return { topQ1: [], topValueGaps: [] };

    const q1 = scatterData.players
      .filter((p) => p.x >= scatterData.xMedian && p.y >= scatterData.yMedian)
      .sort((a, b) => b.y + b.x - (a.y + a.x))
      .slice(0, 6);

    const valGaps = [...scatterData.players]
      .filter((p) => p.value_gap !== null && p.value_gap !== undefined)
      .sort((a, b) => (b.value_gap || 0) - (a.value_gap || 0))
      .slice(0, 6);

    return { topQ1: q1, topValueGaps: valGaps };
  }, [scatterData]);

  const activePresets = PRESETS.filter((p) => p.mode === mode);

  return (
    <div className="container" style={{ paddingBottom: '5rem' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '1.5rem', flexWrap: 'wrap', gap: '1rem' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.5rem' }}>
            <div style={{ padding: '0.5rem', borderRadius: '10px', background: 'rgba(56, 189, 248, 0.1)', color: '#38bdf8' }}>
              <ScatterChart size={24} />
            </div>
            <div>
              <h1 style={{ fontSize: '1.75rem', fontWeight: 700, margin: 0, letterSpacing: '-0.02em' }}>
                Opta / FBref 4-Quadrant Explorer
              </h1>
              <p style={{ margin: 0, fontSize: '0.875rem', color: 'var(--text-dim)' }}>
                Dynamic 2D scatter matrix with statistical median crosshairs &amp; Moneyball value gap mapping
              </p>
            </div>
          </div>
        </div>

        {/* Outfield / GK Toggle */}
        <div style={{ display: 'flex', background: 'rgba(255,255,255,0.04)', borderRadius: '10px', padding: '4px', border: '1px solid rgba(255,255,255,0.08)' }}>
          <button
            onClick={() => handleModeChange('outfield')}
            style={{
              padding: '0.5rem 1.25rem',
              borderRadius: '8px',
              border: 'none',
              background: mode === 'outfield' ? 'linear-gradient(135deg, #0ea5e9, #2563eb)' : 'transparent',
              color: mode === 'outfield' ? '#fff' : 'var(--text-dim)',
              fontWeight: 600,
              fontSize: '0.85rem',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              transition: 'all 0.2s',
            }}
          >
            <span>🏃</span> Outfield Players
          </button>
          <button
            onClick={() => handleModeChange('gk')}
            style={{
              padding: '0.5rem 1.25rem',
              borderRadius: '8px',
              border: 'none',
              background: mode === 'gk' ? 'linear-gradient(135deg, #10b981, #059669)' : 'transparent',
              color: mode === 'gk' ? '#fff' : 'var(--text-dim)',
              fontWeight: 600,
              fontSize: '0.85rem',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              transition: 'all 0.2s',
            }}
          >
            <span>🧤</span> Goalkeepers
          </button>
        </div>
      </div>

      {/* Analytical Presets */}
      <div style={{ marginBottom: '1.25rem' }}>
        <div style={{ fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-dim)', marginBottom: '0.5rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <Sparkles size={14} style={{ color: '#f59e0b' }} />
          <span>Curated Scouting Presets</span>
        </div>
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          {activePresets.map((p) => {
            const isActive = xMetric === p.x && yMetric === p.y;
            return (
              <button
                key={p.name}
                onClick={() => applyPreset(p)}
                style={{
                  padding: '0.45rem 0.85rem',
                  borderRadius: '20px',
                  fontSize: '0.8rem',
                  fontWeight: 500,
                  cursor: 'pointer',
                  border: isActive ? '1px solid #38bdf8' : '1px solid rgba(255,255,255,0.08)',
                  background: isActive ? 'rgba(56, 189, 248, 0.15)' : 'rgba(255,255,255,0.02)',
                  color: isActive ? '#38bdf8' : 'var(--text-main)',
                  transition: 'all 0.15s ease',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                }}
                title={p.desc}
              >
                <span>{p.name}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* Axis & Filter Bar */}
      <div className="card" style={{ padding: '1.25rem', marginBottom: '1.5rem', background: 'rgba(19, 23, 34, 0.6)' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem', alignItems: 'flex-end' }}>
          {/* X Axis Selector */}
          <div>
            <label style={{ display: 'block', fontSize: '0.75rem', color: '#38bdf8', fontWeight: 600, marginBottom: '0.35rem' }}>
              X-AXIS METRIC (Horizontal)
            </label>
            <select
              value={xMetric}
              onChange={(e) => setXMetric(e.target.value)}
              style={{
                width: '100%',
                padding: '0.55rem 0.75rem',
                borderRadius: '8px',
                background: 'rgba(0,0,0,0.4)',
                border: '1px solid rgba(56, 189, 248, 0.3)',
                color: '#fff',
                fontSize: '0.85rem',
              }}
            >
              {metricOptions.map((group) => (
                <optgroup key={group.group} label={group.group}>
                  {group.options.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.label}
                    </option>
                  ))}
                </optgroup>
              ))}
            </select>
          </div>

          {/* Y Axis Selector */}
          <div>
            <label style={{ display: 'block', fontSize: '0.75rem', color: '#34d399', fontWeight: 600, marginBottom: '0.35rem' }}>
              Y-AXIS METRIC (Vertical)
            </label>
            <select
              value={yMetric}
              onChange={(e) => setYMetric(e.target.value)}
              style={{
                width: '100%',
                padding: '0.55rem 0.75rem',
                borderRadius: '8px',
                background: 'rgba(0,0,0,0.4)',
                border: '1px solid rgba(52, 211, 153, 0.3)',
                color: '#fff',
                fontSize: '0.85rem',
              }}
            >
              {metricOptions.map((group) => (
                <optgroup key={group.group} label={group.group}>
                  {group.options.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.label}
                    </option>
                  ))}
                </optgroup>
              ))}
            </select>
          </div>

          {/* Outfield Position Group */}
          {mode === 'outfield' && (
            <div>
              <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
                POSITION SECTOR
              </label>
              <select
                value={posGroup}
                onChange={(e) => setPosGroup(e.target.value)}
                style={{
                  width: '100%',
                  padding: '0.55rem 0.75rem',
                  borderRadius: '8px',
                  background: 'rgba(0,0,0,0.4)',
                  border: '1px solid rgba(255,255,255,0.1)',
                  color: '#fff',
                  fontSize: '0.85rem',
                }}
              >
                <option value="ALL">All Outfield Roles</option>
                <option value="DEF">Defenders (CB / FB / WB)</option>
                <option value="MID">Midfielders (DM / CM / AM)</option>
                <option value="ATT">Attackers (ST / Winger)</option>
              </select>
            </div>
          )}

          {/* Min Minutes */}
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.35rem' }}>
              <label style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600 }}>
                MIN MINUTES
              </label>
              <span style={{ fontSize: '0.75rem', color: '#38bdf8', fontWeight: 600 }}>
                {minMinutes} mins
              </span>
            </div>
            <input
              type="range"
              min="180"
              max="3000"
              step="90"
              value={minMinutes}
              onChange={(e) => setMinMinutes(parseInt(e.target.value, 10))}
              style={{ width: '100%', accentColor: '#38bdf8', cursor: 'pointer' }}
            />
          </div>

          {/* Search Player on Plot */}
          <div>
            <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
              HIGHLIGHT TARGET PLAYER
            </label>
            <div style={{ position: 'relative' }}>
              <Search size={14} style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-dim)' }} />
              <input
                type="text"
                placeholder="Find in plot..."
                value={searchPlayer}
                onChange={(e) => setSearchPlayer(e.target.value)}
                style={{
                  width: '100%',
                  padding: '0.55rem 0.75rem 0.55rem 2rem',
                  borderRadius: '8px',
                  background: 'rgba(0,0,0,0.4)',
                  border: searchPlayer ? '1px solid #38bdf8' : '1px solid rgba(255,255,255,0.1)',
                  color: '#fff',
                  fontSize: '0.85rem',
                }}
              />
            </div>
          </div>
        </div>

        {/* Secondary Filter Row: Financial & Competition Filters */}
        <div style={{ 
          marginTop: '1rem', 
          paddingTop: '1rem', 
          borderTop: '1px solid rgba(255,255,255,0.06)', 
          display: 'grid', 
          gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', 
          gap: '1rem', 
          alignItems: 'flex-end' 
        }}>
          {/* League Coefficient Filter */}
          <div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: minCoef !== 'ALL' ? '#38bdf8' : 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
              <Globe size={13} style={{ color: '#38bdf8' }} />
              MIN LEAGUE COEF
            </label>
            <select
              value={minCoef}
              onChange={(e) => setMinCoef(e.target.value)}
              style={{
                width: '100%',
                padding: '0.55rem 0.75rem',
                borderRadius: '8px',
                background: minCoef !== 'ALL' ? 'rgba(56, 189, 248, 0.12)' : 'rgba(0,0,0,0.4)',
                border: minCoef !== 'ALL' ? '1px solid #38bdf8' : '1px solid rgba(255,255,255,0.1)',
                color: minCoef !== 'ALL' ? '#38bdf8' : '#fff',
                fontSize: '0.85rem',
                fontWeight: minCoef !== 'ALL' ? 600 : 400,
              }}
            >
              <option value="ALL">All Leagues (Any Coef)</option>
              <option value="0.90">Tier 1 Elite (≥ 0.90x)</option>
              <option value="0.70">Top Flight &amp; Above (≥ 0.70x)</option>
              <option value="0.60">Competitive Tier (≥ 0.60x)</option>
              <option value="0.40">Developing Tier (≥ 0.40x)</option>
            </select>
          </div>

          {/* Max Price / Transfer Value */}
          <div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: maxPrice !== 'ALL' ? '#34d399' : 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
              <Coins size={13} style={{ color: '#10b981' }} />
              MAX PRICE / VALUE
            </label>
            <select
              value={maxPrice}
              onChange={(e) => setMaxPrice(e.target.value)}
              style={{
                width: '100%',
                padding: '0.55rem 0.75rem',
                borderRadius: '8px',
                background: maxPrice !== 'ALL' ? 'rgba(16, 185, 129, 0.12)' : 'rgba(0,0,0,0.4)',
                border: maxPrice !== 'ALL' ? '1px solid #10b981' : '1px solid rgba(255,255,255,0.1)',
                color: maxPrice !== 'ALL' ? '#34d399' : '#fff',
                fontSize: '0.85rem',
                fontWeight: maxPrice !== 'ALL' ? 600 : 400,
              }}
            >
              <option value="ALL">Any Transfer Value</option>
              <option value="1">≤ £1.0M (Bargain)</option>
              <option value="3">≤ £3.0M (Low Cost)</option>
              <option value="5">≤ £5.0M (Mid-Tier)</option>
              <option value="10">≤ £10.0M (First Team)</option>
              <option value="15">≤ £15.0M (Quality Target)</option>
              <option value="25">≤ £25.0M (Premium)</option>
              <option value="50">≤ £50.0M (Marquee)</option>
            </select>
          </div>

          {/* Max Wage Filter */}
          <div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: maxWage !== 'ALL' ? '#fbbf24' : 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
              <DollarSign size={13} style={{ color: '#f59e0b' }} />
              MAX WAGE / WEEK
            </label>
            <select
              value={maxWage}
              onChange={(e) => setMaxWage(e.target.value)}
              style={{
                width: '100%',
                padding: '0.55rem 0.75rem',
                borderRadius: '8px',
                background: maxWage !== 'ALL' ? 'rgba(245, 158, 11, 0.12)' : 'rgba(0,0,0,0.4)',
                border: maxWage !== 'ALL' ? '1px solid #f59e0b' : '1px solid rgba(255,255,255,0.1)',
                color: maxWage !== 'ALL' ? '#fbbf24' : '#fff',
                fontSize: '0.85rem',
                fontWeight: maxWage !== 'ALL' ? 600 : 400,
              }}
            >
              <option value="ALL">Any Wage</option>
              <option value="5">≤ £5k / wk (Squad Fringe)</option>
              <option value="10">≤ £10k / wk (Rotational)</option>
              <option value="20">≤ £20k / wk (Starter)</option>
              <option value="35">≤ £35k / wk (Key Player)</option>
              <option value="50">≤ £50k / wk (Star Player)</option>
              <option value="100">≤ £100k / wk (High Earner)</option>
            </select>
          </div>

          {/* Max Age Filter */}
          <div>
            <label style={{ display: 'block', fontSize: '0.75rem', color: maxAge !== '' ? '#c084fc' : 'var(--text-dim)', fontWeight: 600, marginBottom: '0.35rem' }}>
              MAX AGE
            </label>
            <select
              value={maxAge === '' ? 'ALL' : maxAge.toString()}
              onChange={(e) => setMaxAge(e.target.value === 'ALL' ? '' : parseInt(e.target.value, 10))}
              style={{
                width: '100%',
                padding: '0.55rem 0.75rem',
                borderRadius: '8px',
                background: maxAge !== '' ? 'rgba(168, 85, 247, 0.12)' : 'rgba(0,0,0,0.4)',
                border: maxAge !== '' ? '1px solid #a855f7' : '1px solid rgba(255,255,255,0.1)',
                color: maxAge !== '' ? '#c084fc' : '#fff',
                fontSize: '0.85rem',
                fontWeight: maxAge !== '' ? 600 : 400,
              }}
            >
              <option value="ALL">Any Age</option>
              <option value="21">U21 (≤ 21 yrs - Wonderkid)</option>
              <option value="23">U23 (≤ 23 yrs - High Upside)</option>
              <option value="25">U25 (≤ 25 yrs - Prime Upside)</option>
              <option value="28">≤ 28 yrs (Peak Performance)</option>
              <option value="30">≤ 30 yrs (Experienced)</option>
            </select>
          </div>

          {/* Reset Filters / Clear Button */}
          {hasActiveFilters && (
            <div>
              <button
                onClick={resetFilters}
                style={{
                  width: '100%',
                  padding: '0.55rem 0.75rem',
                  borderRadius: '8px',
                  background: 'rgba(239, 68, 68, 0.12)',
                  border: '1px solid rgba(239, 68, 68, 0.35)',
                  color: '#f87171',
                  fontSize: '0.825rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '0.4rem',
                  transition: 'all 0.15s ease',
                }}
              >
                <RotateCcw size={14} />
                Reset Filters
              </button>
            </div>
          )}
        </div>

        {/* Legend / Toggles */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '1rem', paddingTop: '0.75rem', borderTop: '1px solid rgba(255,255,255,0.06)', flexWrap: 'wrap', gap: '0.75rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem', fontSize: '0.75rem', color: 'var(--text-dim)' }}>
            <span style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}>
              <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '50%', background: '#10b981', boxShadow: '0 0 8px #10b981' }} />
              <strong style={{ color: '#fff' }}>Own Squad (Macará)</strong>
            </span>
            <span style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}>
              <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', background: '#38bdf8' }} />
              Scouted Targets
            </span>
            <span style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}>
              <span style={{ display: 'inline-block', width: '14px', height: '1px', borderTop: '1px dashed #e2e8f0' }} />
              Cohort Median Crosshair
            </span>
          </div>

          <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
            Showing <strong style={{ color: '#fff' }}>{scatterData?.players.length || 0}</strong> of <strong style={{ color: '#38bdf8' }}>{scatterData?.cohortSize ? scatterData.cohortSize.toLocaleString() : 0}</strong> qualified players (≥ {debouncedMinMinutes} mins)
          </div>
        </div>
      </div>

      {/* Main Scatter Visualizer */}
      <div style={{ display: 'grid', gridTemplateColumns: selectedPlayer ? '1fr 340px' : '1fr', gap: '1.5rem', marginBottom: '2rem' }}>
        {/* SVG Container Card */}
        <div className="card" style={{ padding: '1.5rem', position: 'relative', overflow: 'hidden' }}>
          {loading && (
            <div style={{ position: 'absolute', inset: 0, background: 'rgba(11, 14, 20, 0.75)', backdropFilter: 'blur(4px)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 10 }}>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.75rem' }}>
                <div style={{ width: '32px', height: '32px', border: '3px solid rgba(56, 189, 248, 0.2)', borderTopColor: '#38bdf8', borderRadius: '50%', animation: 'spin 0.8s linear infinite' }} />
                <span style={{ fontSize: '0.85rem', color: 'var(--text-dim)' }}>Plotting {mode === 'gk' ? 'goalkeepers' : 'outfield players'}...</span>
              </div>
            </div>
          )}

          <div style={{ width: '100%', overflowX: 'auto' }}>
            <svg
              viewBox={`0 0 ${svgWidth} ${svgHeight}`}
              style={{ width: '100%', height: 'auto', display: 'block', background: '#0a0d14', borderRadius: '12px' }}
            >
              <defs>
                <radialGradient id="quadrantEliteGlow" cx="100%" cy="0%" r="80%">
                  <stop offset="0%" stopColor="#10b981" stopOpacity="0.12" />
                  <stop offset="100%" stopColor="#10b981" stopOpacity="0.0" />
                </radialGradient>
                <radialGradient id="quadrantEfficiencyGlow" cx="0%" cy="0%" r="80%">
                  <stop offset="0%" stopColor="#38bdf8" stopOpacity="0.10" />
                  <stop offset="100%" stopColor="#38bdf8" stopOpacity="0.0" />
                </radialGradient>
                <radialGradient id="quadrantVolumeGlow" cx="100%" cy="100%" r="80%">
                  <stop offset="0%" stopColor="#f59e0b" stopOpacity="0.08" />
                  <stop offset="100%" stopColor="#f59e0b" stopOpacity="0.0" />
                </radialGradient>
              </defs>

              {scales && (
                <>
                  {/* Quadrant Tint Overlays */}
                  {/* Q1: Top Right (Elite Output) */}
                  <rect
                    x={scales.medianXCoord}
                    y={padding.top}
                    width={padding.left + plotWidth - scales.medianXCoord}
                    height={scales.medianYCoord - padding.top}
                    fill="url(#quadrantEliteGlow)"
                  />
                  {/* Q2: Top Left (High Y / Lower X) */}
                  <rect
                    x={padding.left}
                    y={padding.top}
                    width={scales.medianXCoord - padding.left}
                    height={scales.medianYCoord - padding.top}
                    fill="url(#quadrantEfficiencyGlow)"
                  />
                  {/* Q4: Bottom Right (High X / Lower Y) */}
                  <rect
                    x={scales.medianXCoord}
                    y={scales.medianYCoord}
                    width={padding.left + plotWidth - scales.medianXCoord}
                    height={padding.top + plotHeight - scales.medianYCoord}
                    fill="url(#quadrantVolumeGlow)"
                  />

                  {/* Quadrant Corner Badges */}
                  <g style={{ opacity: 0.65, fontSize: '11px', fontWeight: 600 }}>
                    {/* Top Right Q1 */}
                    <text x={svgWidth - padding.right - 10} y={padding.top + 20} textAnchor="end" fill="#34d399">
                      ★ Q1: HIGH OUTPUT &amp; ELITE
                    </text>
                    {/* Top Left Q2 */}
                    <text x={padding.left + 15} y={padding.top + 20} textAnchor="start" fill="#38bdf8">
                      ◆ Q2: HIGH {getMetricLabel(yMetric).toUpperCase().slice(0, 15)}
                    </text>
                    {/* Bottom Right Q4 */}
                    <text x={svgWidth - padding.right - 10} y={svgHeight - padding.bottom - 12} textAnchor="end" fill="#f59e0b">
                      ▲ Q4: HIGH {getMetricLabel(xMetric).toUpperCase().slice(0, 15)}
                    </text>
                    {/* Bottom Left Q3 */}
                    <text x={padding.left + 15} y={svgHeight - padding.bottom - 12} textAnchor="start" fill="#64748b">
                      ● Q3: BELOW MEDIAN
                    </text>
                  </g>

                  {/* Median Crosshair Lines */}
                  {/* X Median Line (Vertical) */}
                  <line
                    x1={scales.medianXCoord}
                    y1={padding.top}
                    x2={scales.medianXCoord}
                    y2={padding.top + plotHeight}
                    stroke="#475569"
                    strokeWidth="1.5"
                    strokeDasharray="4 4"
                  />
                  <text
                    x={scales.medianXCoord}
                    y={padding.top - 8}
                    textAnchor="middle"
                    fill="#94a3b8"
                    fontSize="10"
                    fontWeight="500"
                  >
                    Median: {scatterData?.xMedian.toFixed(2)}
                  </text>

                  {/* Y Median Line (Horizontal) */}
                  <line
                    x1={padding.left}
                    y1={scales.medianYCoord}
                    x2={padding.left + plotWidth}
                    y2={scales.medianYCoord}
                    stroke="#475569"
                    strokeWidth="1.5"
                    strokeDasharray="4 4"
                  />
                  <text
                    x={padding.left - 8}
                    y={scales.medianYCoord + 3}
                    textAnchor="end"
                    fill="#94a3b8"
                    fontSize="10"
                    fontWeight="500"
                  >
                    Median: {scatterData?.yMedian.toFixed(2)}
                  </text>

                  {/* Axis Borders */}
                  <line
                    x1={padding.left}
                    y1={padding.top + plotHeight}
                    x2={padding.left + plotWidth}
                    y2={padding.top + plotHeight}
                    stroke="rgba(255,255,255,0.15)"
                    strokeWidth="1"
                  />
                  <line
                    x1={padding.left}
                    y1={padding.top}
                    x2={padding.left}
                    y2={padding.top + plotHeight}
                    stroke="rgba(255,255,255,0.15)"
                    strokeWidth="1"
                  />

                  {/* X Axis Label */}
                  <text
                    x={padding.left + plotWidth / 2}
                    y={svgHeight - 15}
                    textAnchor="middle"
                    fill="#38bdf8"
                    fontSize="12"
                    fontWeight="600"
                  >
                    {getMetricLabel(xMetric)} →
                  </text>

                  {/* Y Axis Label */}
                  <text
                    x={-(padding.top + plotHeight / 2)}
                    y={20}
                    transform="rotate(-90)"
                    textAnchor="middle"
                    fill="#34d399"
                    fontSize="12"
                    fontWeight="600"
                  >
                    {getMetricLabel(yMetric)} →
                  </text>

                  {/* Render Data Dots */}
                  {scatterData?.players.map((p) => {
                    const cx = scales.scaleX(p.x);
                    const cy = scales.scaleY(p.y);
                    const isOwn = p.is_own_squad === 1 || p.is_own_squad === true;
                    const isMatchSearch = searchPlayer.trim().length > 1 && 
                      p.player_name.toLowerCase().includes(searchPlayer.toLowerCase().trim());
                    const isHovered = hoveredPlayer?.player_key === p.player_key;
                    const isSelected = selectedPlayer?.player_key === p.player_key;

                    // Color assignment
                    let dotColor = '#38bdf8';
                    if (isOwn) {
                      dotColor = '#10b981';
                    } else if (p.position?.includes('ST') || p.position?.includes('Forward') || p.position?.includes('Striker')) {
                      dotColor = '#f59e0b';
                    } else if (p.position?.includes('M') || p.position?.includes('Midfielder')) {
                      dotColor = '#818cf8';
                    } else if (p.position?.includes('D') || p.position?.includes('Back')) {
                      dotColor = '#38bdf8';
                    }

                    const radius = isOwn ? 7 : isSelected ? 8 : isMatchSearch ? 8 : isHovered ? 7 : 4.5;

                    return (
                      <g
                        key={p.player_key}
                        onClick={() => setSelectedPlayer(p)}
                        onMouseEnter={() => setHoveredPlayer(p)}
                        onMouseLeave={() => setHoveredPlayer(null)}
                        style={{ cursor: 'pointer' }}
                      >
                        {/* Search or Selected Pulse Glow */}
                        {(isMatchSearch || isSelected) && (
                          <circle
                            cx={cx}
                            cy={cy}
                            r={radius + 6}
                            fill="none"
                            stroke={isOwn ? '#10b981' : '#38bdf8'}
                            strokeWidth="2"
                            strokeOpacity="0.8"
                          >
                            <animate attributeName="r" values={`${radius + 2};${radius + 9};${radius + 2}`} dur="1.5s" repeatCount="indefinite" />
                            <animate attributeName="stroke-opacity" values="0.9;0.2;0.9" dur="1.5s" repeatCount="indefinite" />
                          </circle>
                        )}

                        {/* Own squad outer glow */}
                        {isOwn && (
                          <circle
                            cx={cx}
                            cy={cy}
                            r={radius + 3}
                            fill="none"
                            stroke="#10b981"
                            strokeWidth="1.5"
                            strokeOpacity="0.6"
                          />
                        )}

                        <circle
                          cx={cx}
                          cy={cy}
                          r={radius}
                          fill={dotColor}
                          fillOpacity={isHovered || isSelected || isMatchSearch || isOwn ? 1 : 0.75}
                          stroke={isSelected ? '#ffffff' : '#0a0d14'}
                          strokeWidth={isSelected ? 2 : 1}
                        />

                        {/* Text label for own squad or matched search */}
                        {(isOwn || isMatchSearch || isHovered) && (
                          <text
                            x={cx}
                            y={cy - 9}
                            textAnchor="middle"
                            fill="#ffffff"
                            fontSize="10"
                            fontWeight="600"
                            style={{
                              textShadow: '0 1px 3px rgba(0,0,0,0.9), 0 0 5px rgba(0,0,0,0.8)',
                              pointerEvents: 'none',
                            }}
                          >
                            {p.player_name}
                          </text>
                        )}
                      </g>
                    );
                  })}
                </>
              )}
            </svg>
          </div>

          {/* Floating Hover Tooltip */}
          {hoveredPlayer && !selectedPlayer && (
            <div
              style={{
                position: 'absolute',
                top: '1.5rem',
                right: '1.5rem',
                background: 'rgba(15, 23, 42, 0.94)',
                border: '1px solid rgba(56, 189, 248, 0.35)',
                borderRadius: '10px',
                padding: '0.85rem 1rem',
                maxWidth: '280px',
                boxShadow: '0 10px 25px rgba(0,0,0,0.6)',
                backdropFilter: 'blur(8px)',
                zIndex: 20,
                pointerEvents: 'none',
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.35rem' }}>
                <span style={{ fontWeight: 700, fontSize: '0.95rem', color: '#fff' }}>
                  {hoveredPlayer.player_name}
                </span>
                {hoveredPlayer.is_own_squad ? (
                  <span style={{ fontSize: '0.65rem', background: '#10b98125', color: '#34d399', padding: '2px 6px', borderRadius: '4px', fontWeight: 700 }}>
                    MACARÁ
                  </span>
                ) : null}
              </div>
              <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginBottom: '0.45rem' }}>
                {hoveredPlayer.club_name} • {hoveredPlayer.position} • {hoveredPlayer.age} yrs
              </div>
              <div style={{ display: 'flex', gap: '0.35rem', flexWrap: 'wrap', marginBottom: '0.6rem', fontSize: '0.7rem' }}>
                <span style={{ background: 'rgba(56, 189, 248, 0.12)', color: '#38bdf8', padding: '2px 6px', borderRadius: '4px', fontWeight: 600 }}>
                  {hoveredPlayer.transfer_value_m ? `£${hoveredPlayer.transfer_value_m}M` : 'Free'}
                </span>
                {hoveredPlayer.wage_per_week_k !== null && hoveredPlayer.wage_per_week_k !== undefined && (
                  <span style={{ background: 'rgba(245, 158, 11, 0.12)', color: '#fbbf24', padding: '2px 6px', borderRadius: '4px', fontWeight: 600 }}>
                    £{hoveredPlayer.wage_per_week_k}k/wk
                  </span>
                )}
                {hoveredPlayer.league_difficulty_coef !== null && hoveredPlayer.league_difficulty_coef !== undefined && (
                  <span style={{ background: hoveredPlayer.league_difficulty_coef >= 0.9 ? 'rgba(52, 211, 153, 0.15)' : 'rgba(255, 255, 255, 0.08)', color: hoveredPlayer.league_difficulty_coef >= 0.9 ? '#34d399' : '#94a3b8', padding: '2px 6px', borderRadius: '4px', fontWeight: 600 }}>
                    {hoveredPlayer.league_difficulty_coef}x Coef
                  </span>
                )}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem', fontSize: '0.75rem', borderTop: '1px solid rgba(255,255,255,0.08)', paddingTop: '0.5rem' }}>
                <div>
                  <span style={{ color: '#38bdf8', display: 'block', fontSize: '0.7rem' }}>{getMetricLabel(xMetric).slice(0, 16)}</span>
                  <strong style={{ fontSize: '0.85rem', color: '#fff' }}>{hoveredPlayer.x.toFixed(2)}</strong>
                </div>
                <div>
                  <span style={{ color: '#34d399', display: 'block', fontSize: '0.7rem' }}>{getMetricLabel(yMetric).slice(0, 16)}</span>
                  <strong style={{ fontSize: '0.85rem', color: '#fff' }}>{hoveredPlayer.y.toFixed(2)}</strong>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Selected Player Detail Drawer */}
        {selectedPlayer && (
          <div className="card" style={{ padding: '1.25rem', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', border: '1px solid rgba(56, 189, 248, 0.3)' }}>
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.5rem' }}>
                <div>
                  <h3 style={{ margin: 0, fontSize: '1.15rem', fontWeight: 700, color: '#fff' }}>
                    {selectedPlayer.player_name}
                  </h3>
                  <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                    {selectedPlayer.club_name} • {selectedPlayer.league}
                  </div>
                </div>
                <button
                  onClick={() => setSelectedPlayer(null)}
                  style={{ background: 'none', border: 'none', color: 'var(--text-dim)', cursor: 'pointer', fontSize: '1.25rem' }}
                >
                  ✕
                </button>
              </div>

              {selectedPlayer.is_own_squad ? (
                <div style={{ display: 'inline-block', padding: '0.2rem 0.6rem', borderRadius: '4px', background: 'rgba(16, 185, 129, 0.15)', color: '#34d399', fontSize: '0.7rem', fontWeight: 700, marginBottom: '1rem' }}>
                  OWN SQUAD PLAYER
                </div>
              ) : null}

              {/* Bio Grid */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem', marginBottom: '1.25rem', background: 'rgba(0,0,0,0.3)', padding: '0.75rem', borderRadius: '8px' }}>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>Position</span>
                  <strong style={{ fontSize: '0.85rem', color: '#fff' }}>{selectedPlayer.position}</strong>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>Age</span>
                  <strong style={{ fontSize: '0.85rem', color: '#fff' }}>{selectedPlayer.age} yrs</strong>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>Minutes</span>
                  <strong style={{ fontSize: '0.85rem', color: '#fff' }}>{selectedPlayer.minutes_played}</strong>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>Price</span>
                  <strong style={{ fontSize: '0.85rem', color: '#38bdf8' }}>
                    {selectedPlayer.transfer_value_m ? `£${selectedPlayer.transfer_value_m}M` : 'Free / Nil'}
                  </strong>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>Wage</span>
                  <strong style={{ fontSize: '0.85rem', color: '#fbbf24' }}>
                    {selectedPlayer.wage_per_week_k !== null && selectedPlayer.wage_per_week_k !== undefined ? `£${selectedPlayer.wage_per_week_k}k/wk` : 'N/A'}
                  </strong>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block' }}>League Coef</span>
                  <strong style={{ fontSize: '0.85rem', color: '#34d399' }}>
                    {selectedPlayer.league_difficulty_coef !== null && selectedPlayer.league_difficulty_coef !== undefined ? `${selectedPlayer.league_difficulty_coef}x` : '1.00x'}
                  </strong>
                </div>
              </div>

              {/* Plotted Metric Readout */}
              <div style={{ marginBottom: '1.25rem' }}>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-dim)', marginBottom: '0.5rem', textTransform: 'uppercase' }}>
                  Scatter Coordinates
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '0.5rem 0.75rem', background: 'rgba(56, 189, 248, 0.08)', borderRadius: '6px', borderLeft: '3px solid #38bdf8' }}>
                    <span style={{ fontSize: '0.8rem', color: '#e2e8f0' }}>{getMetricLabel(xMetric)}</span>
                    <strong style={{ fontSize: '0.85rem', color: '#38bdf8' }}>{selectedPlayer.x.toFixed(2)}</strong>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '0.5rem 0.75rem', background: 'rgba(52, 211, 153, 0.08)', borderRadius: '6px', borderLeft: '3px solid #34d399' }}>
                    <span style={{ fontSize: '0.8rem', color: '#e2e8f0' }}>{getMetricLabel(yMetric)}</span>
                    <strong style={{ fontSize: '0.85rem', color: '#34d399' }}>{selectedPlayer.y.toFixed(2)}</strong>
                  </div>
                </div>
              </div>

              {/* Value Gap Index */}
              {selectedPlayer.value_gap !== null && selectedPlayer.value_gap !== undefined && (
                <div style={{ padding: '0.75rem', borderRadius: '8px', background: selectedPlayer.value_gap > 0 ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)', border: `1px solid ${selectedPlayer.value_gap > 0 ? '#10b98130' : '#ef444430'}`, marginBottom: '1.25rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>Moneyball Value Gap</span>
                    <strong style={{ fontSize: '0.9rem', color: selectedPlayer.value_gap > 0 ? '#34d399' : '#f87171' }}>
                      {selectedPlayer.value_gap > 0 ? `+${selectedPlayer.value_gap.toFixed(1)}` : selectedPlayer.value_gap.toFixed(1)}
                    </strong>
                  </div>
                </div>
              )}
            </div>

            {/* Quick Actions */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              <button
                onClick={() => {
                  toggleShortlist({
                    player_key: selectedPlayer.player_key,
                    player_name: selectedPlayer.player_name,
                    position: selectedPlayer.position,
                    club_name: selectedPlayer.club_name,
                    league: selectedPlayer.league,
                    age: selectedPlayer.age,
                    minutes_played: selectedPlayer.minutes_played,
                    transfer_value_m: selectedPlayer.transfer_value_m ?? undefined,
                    wage_per_week_k: selectedPlayer.wage_per_week_k ?? undefined,
                    primary_stat: `${getMetricLabel(xMetric)}: ${selectedPlayer.x.toFixed(2)}`,
                    added_at: new Date().toISOString(),
                  });
                }}
                style={{
                  width: '100%',
                  padding: '0.65rem',
                  borderRadius: '8px',
                  border: isShortlisted(selectedPlayer.player_key) ? '1px solid #f59e0b' : '1px solid rgba(255,255,255,0.15)',
                  background: isShortlisted(selectedPlayer.player_key) ? 'rgba(245, 158, 11, 0.15)' : 'rgba(255,255,255,0.05)',
                  color: isShortlisted(selectedPlayer.player_key) ? '#f59e0b' : '#fff',
                  fontSize: '0.85rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '0.5rem',
                }}
              >
                <Star size={16} fill={isShortlisted(selectedPlayer.player_key) ? '#f59e0b' : 'none'} />
                {isShortlisted(selectedPlayer.player_key) ? 'Shortlisted' : 'Add to Shortlist'}
              </button>

              <Link
                href={`/player/${selectedPlayer.player_key}`}
                className="btn-primary"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '0.5rem',
                  padding: '0.65rem',
                  fontSize: '0.85rem',
                  textAlign: 'center',
                  textDecoration: 'none',
                }}
              >
                <span>Full Scout Profile</span>
                <ChevronRight size={16} />
              </Link>
            </div>
          </div>
        )}
      </div>

      {/* Cohort Leaders: Q1 Elite & Moneyball Value Gap Diamonds */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: '1.5rem' }}>
        {/* Q1 Elite Leaders */}
        <div className="card" style={{ padding: '1.25rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1rem' }}>
            <Award size={18} style={{ color: '#34d399' }} />
            <h3 style={{ margin: 0, fontSize: '1rem', fontWeight: 700 }}>
              Quadrant 1 Leaders (Above Median on Both Axes)
            </h3>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
            {topQ1.map((p) => (
              <div
                key={p.player_key}
                onClick={() => setSelectedPlayer(p)}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '0.6rem 0.85rem',
                  borderRadius: '8px',
                  background: 'rgba(255,255,255,0.02)',
                  border: '1px solid rgba(255,255,255,0.05)',
                  cursor: 'pointer',
                  transition: 'all 0.15s ease',
                }}
              >
                <div>
                  <div style={{ fontWeight: 600, fontSize: '0.85rem', color: '#fff' }}>
                    {p.player_name}
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                    {p.club_name} • {p.position}
                  </div>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: '0.8rem', color: '#38bdf8', fontWeight: 600 }}>
                    X: {p.x.toFixed(2)} | Y: {p.y.toFixed(2)}
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', display: 'flex', gap: '0.35rem', justifyContent: 'flex-end', marginTop: '2px' }}>
                    <span>{p.transfer_value_m ? `£${p.transfer_value_m}M` : 'Free'}</span>
                    {p.wage_per_week_k !== null && p.wage_per_week_k !== undefined && <span>• £{p.wage_per_week_k}k/wk</span>}
                    {p.league_difficulty_coef !== null && p.league_difficulty_coef !== undefined && (
                      <span style={{ color: p.league_difficulty_coef >= 0.9 ? '#34d399' : '#94a3b8' }}>({p.league_difficulty_coef}x)</span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Value Gap Diamonds */}
        <div className="card" style={{ padding: '1.25rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1rem' }}>
            <TrendingUp size={18} style={{ color: '#38bdf8' }} />
            <h3 style={{ margin: 0, fontSize: '1rem', fontWeight: 700 }}>
              Top Moneyball Value Gaps (High Output vs Low Cost)
            </h3>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
            {topValueGaps.map((p) => (
              <div
                key={p.player_key}
                onClick={() => setSelectedPlayer(p)}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '0.6rem 0.85rem',
                  borderRadius: '8px',
                  background: 'rgba(255,255,255,0.02)',
                  border: '1px solid rgba(255,255,255,0.05)',
                  cursor: 'pointer',
                  transition: 'all 0.15s ease',
                }}
              >
                <div>
                  <div style={{ fontWeight: 600, fontSize: '0.85rem', color: '#fff' }}>
                    {p.player_name}
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                    {p.club_name} • {p.position} • {p.age} yrs
                  </div>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: '0.85rem', color: '#34d399', fontWeight: 700 }}>
                    +{p.value_gap?.toFixed(1)} Gap
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', display: 'flex', gap: '0.35rem', justifyContent: 'flex-end', marginTop: '2px' }}>
                    <span style={{ color: '#38bdf8' }}>{p.transfer_value_m ? `£${p.transfer_value_m}M` : 'Free'}</span>
                    {p.wage_per_week_k !== null && p.wage_per_week_k !== undefined && <span>• £{p.wage_per_week_k}k/wk</span>}
                    {p.league_difficulty_coef !== null && p.league_difficulty_coef !== undefined && (
                      <span style={{ color: p.league_difficulty_coef >= 0.9 ? '#34d399' : '#94a3b8' }}>({p.league_difficulty_coef}x)</span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
