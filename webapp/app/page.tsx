'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import {
  Search,
  Filter,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Sparkles,
  Database,
  TrendingUp,
  GitCompare,
  ArrowRight,
  ShieldCheck,
  ChevronLeft,
  ChevronRight,
  BookOpen,
} from 'lucide-react';

interface PlayerRow {
  player_key: number;
  player_name: string;
  age: number;
  position: string;
  club_name: string;
  league: string;
  is_own_squad: number;
  minutes_played: number;
  wage_per_week_k: number;
  transfer_value_m: number;
  goals_per90: number;
  assists_per90: number;
  non_penalty_xGoals_per90: number;
  xAssits_per90: number;
  npxg_plus_xa_per90: number;
  progressive_passes_per90: number;
  defensive_actions_per90: number;
  cost_per_xgi: number;
}

const POSITIONS = [
  'ALL',
  'Striker',
  'Central Attacking Midfielder',
  'Left Attacking Midfielder',
  'Right Attacking Midfielder',
  'Central Midfielder',
  'Defensive Midfielder',
  'Left Midfielder',
  'Right Midfielder',
  'Central Defender',
  'Left Wingback',
  'Right Wingback',
  'Goalkeeper',
];

const SORT_COLUMNS: { key: string; label: string }[] = [
  { key: 'npxg_plus_xa_per90', label: 'NP-xG + xA / 90' },
  { key: 'non_penalty_xGoals_per90', label: 'NP-xG / 90' },
  { key: 'xAssits_per90', label: 'xA / 90' },
  { key: 'progressive_passes_per90', label: 'PrPass / 90' },
  { key: 'defensive_actions_per90', label: 'DefAct / 90' },
  { key: 'transfer_value_m', label: 'Transfer Value' },
  { key: 'wage_per_week_k', label: 'Wage / Week' },
  { key: 'minutes_played', label: 'Minutes Played' },
  { key: 'age', label: 'Age' },
  { key: 'player_name', label: 'Player Name' },
];

const getSortLabel = (key: string) => {
  const found = SORT_COLUMNS.find((c) => c.key === key);
  return found ? found.label : key.replace(/_/g, ' ');
};

export default function ScoutingHub() {
  const [players, setPlayers] = useState<PlayerRow[]>([]);
  const [loading, setLoading] = useState(true);

  // Filters
  const [search, setSearch] = useState('');
  const [position, setPosition] = useState('ALL');
  const [minMinutes, setMinMinutes] = useState(450);
  const [maxValue, setMaxValue] = useState<number>(999);
  const [isOwnSquad, setIsOwnSquad] = useState<string>('ALL');
  const [sortBy, setSortBy] = useState('npxg_plus_xa_per90');
  const [sortOrder, setSortOrder] = useState('DESC');
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({ total: 0, totalPages: 1 });

  // Fetch players on filter change
  useEffect(() => {
    const fetchPlayers = async () => {
      setLoading(true);
      try {
        const queryParams = new URLSearchParams({
          search,
          position,
          minMinutes: minMinutes.toString(),
          maxValue: maxValue.toString(),
          sortBy,
          sortOrder,
          page: page.toString(),
          pageSize: '20',
        });

        if (isOwnSquad !== 'ALL') {
          queryParams.set('isOwnSquad', isOwnSquad);
        }

        const res = await fetch(`/api/players?${queryParams.toString()}`);
        const data = await res.json();
        setPlayers(data.players || []);
        if (data.pagination) {
          setPagination({
            total: data.pagination.total,
            totalPages: data.pagination.totalPages,
          });
        }
      } catch (err) {
        console.error('Error fetching players:', err);
      } finally {
        setLoading(false);
      }
    };

    const timer = setTimeout(() => {
      fetchPlayers();
    }, 200);

    return () => clearTimeout(timer);
  }, [search, position, minMinutes, maxValue, isOwnSquad, sortBy, sortOrder, page]);

  const handleSort = (col: string) => {
    if (sortBy === col) {
      setSortOrder(sortOrder === 'DESC' ? 'ASC' : 'DESC');
    } else {
      setSortBy(col);
      setSortOrder('DESC');
    }
    setPage(1);
  };

  const renderSortTh = (colKey: string, label: string, align: 'left' | 'right' = 'right') => {
    const isSorted = sortBy === colKey;
    return (
      <th
        onClick={() => handleSort(colKey)}
        style={{
          textAlign: align,
          cursor: 'pointer',
          color: isSorted ? '#fbbf24' : undefined,
          fontWeight: isSorted ? 700 : undefined,
          userSelect: 'none',
          background: isSorted ? 'rgba(251, 191, 36, 0.08)' : undefined,
          transition: 'all 0.15s ease',
        }}
        title={`Sort by ${label} (${isSorted ? (sortOrder === 'DESC' ? 'High to Low' : 'Low to High') : 'Click to sort'})`}
      >
        <div
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.35rem',
            justifyContent: align === 'right' ? 'flex-end' : 'flex-start',
            width: '100%',
          }}
        >
          <span>{label}</span>
          {isSorted ? (
            sortOrder === 'DESC' ? (
              <ArrowDown size={13} color="#fbbf24" />
            ) : (
              <ArrowUp size={13} color="#fbbf24" />
            )
          ) : (
            <ArrowUpDown size={11} style={{ opacity: 0.3 }} />
          )}
        </div>
      </th>
    );
  };

  const getTdSortStyle = (colKey: string, defaultColor?: string) => {
    const isSorted = sortBy === colKey;
    return {
      textAlign: 'right' as const,
      fontWeight: isSorted ? 700 : 400,
      color: isSorted ? '#fbbf24' : (defaultColor || undefined),
      background: isSorted ? 'rgba(251, 191, 36, 0.05)' : undefined,
      transition: 'all 0.15s ease',
    };
  };

  return (
    <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
      {/* Hero Header */}
      <div
        className="glass-card"
        style={{
          background: 'radial-gradient(ellipse at 80% 20%, rgba(6, 182, 212, 0.12), transparent 60%), rgba(18, 24, 38, 0.75)',
          padding: '2.25rem',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: '1.5rem' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.65rem', marginBottom: '0.5rem' }}>
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '0.35rem',
                  padding: '0.25rem 0.65rem',
                  borderRadius: '9999px',
                  background: 'rgba(6, 182, 212, 0.15)',
                  color: '#38bdf8',
                  fontSize: '0.75rem',
                  fontWeight: 600,
                  border: '1px solid rgba(6, 182, 212, 0.3)',
                }}
              >
                <Database size={12} />
                SQL SERVER WAREHOUSE
              </span>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                11,359 Players Materialized
              </span>
            </div>
            <h1 style={{ fontSize: '2.25rem', fontWeight: 800, letterSpacing: '-0.03em', color: '#fff' }}>
              Football Manager Scouting Hub
            </h1>
            <p style={{ color: 'var(--text-muted)', fontSize: '1rem', marginTop: '0.35rem', maxWidth: '640px' }}>
              Discover transfer bargains, evaluate tactical output, and inspect FBref-style percentiles & Opta pizza charts powered by your live data warehouse.
            </p>
            <div style={{ marginTop: '0.75rem' }}>
              <Link
                href="/metrics"
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                  fontSize: '0.8rem',
                  fontWeight: 600,
                  color: '#38bdf8',
                  background: 'rgba(56, 189, 248, 0.1)',
                  border: '1px solid rgba(56, 189, 248, 0.25)',
                  padding: '0.35rem 0.75rem',
                  borderRadius: '20px',
                }}
              >
                <BookOpen size={13} />
                <span>New to the stats? Explore the Metrics & Graphs Guide &rarr;</span>
              </Link>
            </div>
          </div>

          {/* Quick Metrics */}
          <div style={{ display: 'flex', gap: '1rem' }}>
            <div
              style={{
                background: 'rgba(255, 255, 255, 0.04)',
                border: '1px solid var(--border-subtle)',
                borderRadius: '12px',
                padding: '1rem 1.25rem',
                minWidth: '130px',
              }}
            >
              <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600 }}>QUALIFIED POOL</div>
              <div className="mono-number" style={{ fontSize: '1.5rem', fontWeight: 800, color: '#38bdf8', marginTop: '2px' }}>
                {pagination.total.toLocaleString()}
              </div>
              <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', marginTop: '2px' }}>≥ {minMinutes} mins</div>
            </div>

            <div
              style={{
                background: 'rgba(255, 255, 255, 0.04)',
                border: '1px solid var(--border-subtle)',
                borderRadius: '12px',
                padding: '1rem 1.25rem',
                minWidth: '130px',
              }}
            >
              <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 600 }}>TOP SORT</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: '#fbbf24', marginTop: '6px' }}>
                {getSortLabel(sortBy)}
              </div>
              <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                {sortOrder === 'DESC' ? 'High to Low' : 'Low to High'}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Position Quick Filters */}
      <div style={{ display: 'flex', gap: '0.5rem', overflowX: 'auto', paddingBottom: '0.5rem' }}>
        {POSITIONS.map((pos) => (
          <button
            key={pos}
            onClick={() => {
              setPosition(pos);
              setPage(1);
            }}
            style={{
              padding: '0.45rem 0.85rem',
              borderRadius: '9999px',
              fontSize: '0.8rem',
              fontWeight: 600,
              whiteSpace: 'nowrap',
              transition: 'all 0.2s ease',
              border: position === pos ? '1px solid #06b6d4' : '1px solid var(--border-subtle)',
              background: position === pos ? 'rgba(6, 182, 212, 0.2)' : 'rgba(255, 255, 255, 0.03)',
              color: position === pos ? '#38bdf8' : 'var(--text-muted)',
            }}
          >
            {pos === 'ALL' ? 'All Positions' : pos}
          </button>
        ))}
      </div>

      {/* Search & Filter Controls */}
      <div
        className="glass-card"
        style={{
          padding: '1.25rem 1.5rem',
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
          gap: '1rem',
          alignItems: 'center',
        }}
      >
        {/* Search */}
        <div>
          <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', marginBottom: '0.35rem', fontWeight: 600 }}>
            PLAYER OR CLUB SEARCH
          </label>
          <div style={{ position: 'relative' }}>
            <Search size={14} style={{ position: 'absolute', left: '0.75rem', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-dim)' }} />
            <input
              type="text"
              placeholder="e.g. Beltrán, Fiorentina..."
              value={search}
              onChange={(e) => {
                setSearch(e.target.value);
                setPage(1);
              }}
              style={{
                width: '100%',
                background: 'var(--bg-input)',
                border: '1px solid var(--border-subtle)',
                borderRadius: '8px',
                padding: '0.5rem 0.75rem 0.5rem 2rem',
                color: '#fff',
                fontSize: '0.85rem',
              }}
            />
          </div>
        </div>

        {/* Squad Status */}
        <div>
          <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', marginBottom: '0.35rem', fontWeight: 600 }}>
            TARGET TYPE
          </label>
          <select
            value={isOwnSquad}
            onChange={(e) => {
              setIsOwnSquad(e.target.value);
              setPage(1);
            }}
            style={{
              width: '100%',
              background: 'var(--bg-input)',
              border: '1px solid var(--border-subtle)',
              borderRadius: '8px',
              padding: '0.5rem 0.75rem',
              color: '#fff',
              fontSize: '0.85rem',
            }}
          >
            <option value="ALL">All Players (Targets + Squad)</option>
            <option value="0">Scouting Targets Only</option>
            <option value="1">Own Squad Only</option>
          </select>
        </div>

        {/* Top Sort */}
        <div>
          <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', marginBottom: '0.35rem', fontWeight: 600 }}>
            TOP SORT
          </label>
          <div style={{ display: 'flex', gap: '0.35rem' }}>
            <select
              value={sortBy}
              onChange={(e) => {
                setSortBy(e.target.value);
                setPage(1);
              }}
              style={{
                width: '100%',
                background: 'var(--bg-input)',
                border: '1px solid var(--border-subtle)',
                borderRadius: '8px',
                padding: '0.5rem 0.75rem',
                color: '#fff',
                fontSize: '0.85rem',
              }}
            >
              {SORT_COLUMNS.map((sc) => (
                <option key={sc.key} value={sc.key}>
                  {sc.label}
                </option>
              ))}
            </select>
            <button
              onClick={() => {
                setSortOrder(sortOrder === 'DESC' ? 'ASC' : 'DESC');
                setPage(1);
              }}
              title={sortOrder === 'DESC' ? 'Descending (High to Low)' : 'Ascending (Low to High)'}
              style={{
                background: 'var(--bg-input)',
                border: '1px solid var(--border-subtle)',
                borderRadius: '8px',
                padding: '0.5rem 0.65rem',
                color: '#fbbf24',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transition: 'background 0.2s',
              }}
            >
              {sortOrder === 'DESC' ? <ArrowDown size={14} /> : <ArrowUp size={14} />}
            </button>
          </div>
        </div>

        {/* Max Valuation */}
        <div>
          <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', marginBottom: '0.35rem', fontWeight: 600 }}>
            MAX TRANSFER VALUE
          </label>
          <select
            value={maxValue}
            onChange={(e) => {
              setMaxValue(Number(e.target.value));
              setPage(1);
            }}
            style={{
              width: '100%',
              background: 'var(--bg-input)',
              border: '1px solid var(--border-subtle)',
              borderRadius: '8px',
              padding: '0.5rem 0.75rem',
              color: '#fff',
              fontSize: '0.85rem',
            }}
          >
            <option value="999">Any Valuation</option>
            <option value="50">Under £50M</option>
            <option value="25">Under £25M</option>
            <option value="15">Under £15M (Bargain Tier)</option>
            <option value="5">Under £5M (Value Gem)</option>
          </select>
        </div>

        {/* Min Minutes */}
        <div>
          <label style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-dim)', marginBottom: '0.35rem', fontWeight: 600 }}>
            MIN MINUTES PLAYED: <span className="mono-number" style={{ color: '#38bdf8' }}>{minMinutes}</span>
          </label>
          <input
            type="range"
            min="0"
            max="1800"
            step="90"
            value={minMinutes}
            onChange={(e) => {
              setMinMinutes(Number(e.target.value));
              setPage(1);
            }}
            style={{ width: '100%', accentColor: '#06b6d4', cursor: 'pointer' }}
          />
        </div>
      </div>

      {/* Main Players Data Table */}
      <div className="fbref-table-container">
        <table className="fbref-table" id="scouting-players-table">
          <thead>
            <tr>
              {renderSortTh('player_name', 'Player', 'left')}
              <th>Position</th>
              <th>Club & League</th>
              {renderSortTh('age', 'Age')}
              {renderSortTh('minutes_played', 'Mins')}
              {renderSortTh('transfer_value_m', 'Value')}
              {renderSortTh('wage_per_week_k', 'Wage/Wk')}
              {renderSortTh('npxg_plus_xa_per90', 'NP-xG+xA/90')}
              {renderSortTh('non_penalty_xGoals_per90', 'NP-xG/90')}
              {renderSortTh('xAssits_per90', 'xA/90')}
              {renderSortTh('progressive_passes_per90', 'PrPass/90')}
              {renderSortTh('defensive_actions_per90', 'DefAct/90')}
              <th style={{ textAlign: 'center', width: '130px' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={13} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                  Loading players from SQL Server warehouse...
                </td>
              </tr>
            ) : players.length === 0 ? (
              <tr>
                <td colSpan={13} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                  No players matched the current filters. Try lowering minimum minutes or clearing search.
                </td>
              </tr>
            ) : (
              players.map((p) => (
                <tr key={p.player_key}>
                  <td>
                    <Link
                      href={`/player/${p.player_key}`}
                      style={{
                        fontWeight: 600,
                        color: sortBy === 'player_name' ? '#fbbf24' : '#fff',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.45rem',
                      }}
                    >
                      <span>{p.player_name}</span>
                      {p.is_own_squad === 1 && (
                        <span className="badge badge-own-squad" style={{ fontSize: '0.65rem' }}>
                          SQUAD
                        </span>
                      )}
                    </Link>
                  </td>
                  <td>
                    <span className="badge badge-position">{p.position}</span>
                  </td>
                  <td>
                    <div style={{ color: '#e2e8f0', fontWeight: 500 }}>{p.club_name}</div>
                    <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)' }}>{p.league}</div>
                  </td>
                  <td style={getTdSortStyle('age')} className="mono-number">
                    {p.age}
                  </td>
                  <td style={getTdSortStyle('minutes_played')} className="mono-number">
                    {p.minutes_played?.toLocaleString()}
                  </td>
                  <td style={getTdSortStyle('transfer_value_m', '#38bdf8')} className="mono-number">
                    {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                  </td>
                  <td style={getTdSortStyle('wage_per_week_k')} className="mono-number">
                    {p.wage_per_week_k ? `£${p.wage_per_week_k}k` : '-'}
                  </td>
                  <td
                    style={
                      sortBy === 'npxg_plus_xa_per90'
                        ? {
                            textAlign: 'right',
                            fontWeight: 700,
                            color: (p.npxg_plus_xa_per90 || 0) >= 0.4 ? '#34d399' : '#fbbf24',
                            background: 'rgba(251, 191, 36, 0.05)',
                          }
                        : {
                            textAlign: 'right',
                          }
                    }
                    className="mono-number"
                  >
                    {p.npxg_plus_xa_per90?.toFixed(2) || '0.00'}
                  </td>
                  <td style={getTdSortStyle('non_penalty_xGoals_per90')} className="mono-number">
                    {p.non_penalty_xGoals_per90?.toFixed(2) || '0.00'}
                  </td>
                  <td style={getTdSortStyle('xAssits_per90')} className="mono-number">
                    {p.xAssits_per90?.toFixed(2) || '0.00'}
                  </td>
                  <td style={getTdSortStyle('progressive_passes_per90')} className="mono-number">
                    {p.progressive_passes_per90?.toFixed(2) || '0.00'}
                  </td>
                  <td style={getTdSortStyle('defensive_actions_per90')} className="mono-number">
                    {p.defensive_actions_per90?.toFixed(2) || '0.00'}
                  </td>
                  <td style={{ textAlign: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.4rem' }}>
                      <Link
                        href={`/player/${p.player_key}`}
                        className="btn-primary"
                        style={{ padding: '0.35rem 0.65rem', fontSize: '0.75rem' }}
                      >
                        Report
                      </Link>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination Footer */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.5rem 0' }}>
        <div style={{ fontSize: '0.85rem', color: 'var(--text-dim)' }}>
          Showing page <span className="mono-number" style={{ color: '#fff' }}>{page}</span> of{' '}
          <span className="mono-number" style={{ color: '#fff' }}>{pagination.totalPages}</span> ({pagination.total} players)
        </div>

        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button
            onClick={() => setPage(Math.max(1, page - 1))}
            disabled={page === 1}
            className="btn-secondary"
            style={{ opacity: page === 1 ? 0.4 : 1, padding: '0.45rem 0.85rem', fontSize: '0.8rem' }}
          >
            <ChevronLeft size={16} />
            <span>Previous</span>
          </button>
          <button
            onClick={() => setPage(Math.min(pagination.totalPages, page + 1))}
            disabled={page >= pagination.totalPages}
            className="btn-secondary"
            style={{ opacity: page >= pagination.totalPages ? 0.4 : 1, padding: '0.45rem 0.85rem', fontSize: '0.8rem' }}
          >
            <span>Next</span>
            <ChevronRight size={16} />
          </button>
        </div>
      </div>
    </div>
  );
}
