'use client';

import React, { useState, useMemo } from 'react';
import Link from 'next/link';
import { 
  Star, 
  Download, 
  Copy, 
  Check, 
  Trash2, 
  ExternalLink, 
  Users, 
  SlidersHorizontal, 
  Search, 
  Sparkles,
  ArrowRight
} from 'lucide-react';
import { useShortlist, exportShortlistCSV, copyPlayerKeysToClipboard } from '@/lib/shortlist';

export default function ShortlistPage() {
  const { shortlist, remove } = useShortlist();
  const [searchTerm, setSearchTerm] = useState('');
  const [posFilter, setPosFilter] = useState('ALL');
  const [copied, setCopied] = useState(false);

  // Filtered shortlist
  const filtered = useMemo(() => {
    return shortlist.filter((p) => {
      const matchSearch = searchTerm.trim().length === 0 ||
        p.player_name.toLowerCase().includes(searchTerm.toLowerCase().trim()) ||
        p.club_name.toLowerCase().includes(searchTerm.toLowerCase().trim());

      const matchPos = posFilter === 'ALL' ||
        (posFilter === 'GK' && p.position.includes('Goalkeeper')) ||
        (posFilter === 'DEF' && (p.position.includes('Defender') || p.position.includes('Back'))) ||
        (posFilter === 'MID' && p.position.includes('Midfielder')) ||
        (posFilter === 'ATT' && (p.position.includes('Striker') || p.position.includes('Forward') || p.position.includes('Winger')));

      return matchSearch && matchPos;
    });
  }, [shortlist, searchTerm, posFilter]);

  // Aggregate KPIs
  const stats = useMemo(() => {
    if (shortlist.length === 0) {
      return { avgAge: 0, totalVal: 0, avgWage: 0 };
    }
    const avgAge = +(shortlist.reduce((sum, p) => sum + (p.age || 0), 0) / shortlist.length).toFixed(1);
    const totalVal = +shortlist.reduce((sum, p) => sum + (p.transfer_value_m || 0), 0).toFixed(1);
    const avgWage = +(shortlist.reduce((sum, p) => sum + (p.wage_per_week_k || 0), 0) / shortlist.length).toFixed(2);
    return { avgAge, totalVal, avgWage };
  }, [shortlist]);

  const handleCopyIds = async () => {
    const count = await copyPlayerKeysToClipboard();
    if (count > 0) {
      setCopied(true);
      setTimeout(() => setCopied(false), 2500);
    }
  };

  return (
    <div className="container" style={{ paddingBottom: '5rem' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '2rem', flexWrap: 'wrap', gap: '1rem' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.5rem' }}>
            <div style={{ padding: '0.5rem', borderRadius: '10px', background: 'rgba(245, 158, 11, 0.1)', color: '#f59e0b' }}>
              <Star size={24} fill="#f59e0b" />
            </div>
            <div>
              <h1 style={{ fontSize: '1.75rem', fontWeight: 700, margin: 0, letterSpacing: '-0.02em' }}>
                Custom Shortlists &amp; FM Export
              </h1>
              <p style={{ margin: 0, fontSize: '0.875rem', color: 'var(--text-dim)' }}>
                Target recruitment board with 1-click Football Manager CSV export &amp; player ID copying
              </p>
            </div>
          </div>
        </div>

        {/* Action Buttons */}
        <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
          <button
            onClick={handleCopyIds}
            disabled={shortlist.length === 0}
            style={{
              padding: '0.6rem 1rem',
              borderRadius: '8px',
              border: '1px solid rgba(56, 189, 248, 0.3)',
              background: copied ? 'rgba(16, 185, 129, 0.2)' : 'rgba(56, 189, 248, 0.1)',
              color: copied ? '#34d399' : '#38bdf8',
              fontSize: '0.85rem',
              fontWeight: 600,
              cursor: shortlist.length === 0 ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              transition: 'all 0.2s',
            }}
          >
            {copied ? <Check size={16} /> : <Copy size={16} />}
            <span>{copied ? 'Copied IDs!' : 'Copy Player IDs'}</span>
          </button>

          <button
            onClick={() => exportShortlistCSV()}
            disabled={shortlist.length === 0}
            className="btn-primary"
            style={{
              padding: '0.6rem 1.25rem',
              fontSize: '0.85rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              cursor: shortlist.length === 0 ? 'not-allowed' : 'pointer',
              opacity: shortlist.length === 0 ? 0.5 : 1,
            }}
          >
            <Download size={16} />
            <span>Export FM Shortlist (.csv)</span>
          </button>
        </div>
      </div>

      {/* Aggregate KPI Summary Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem', marginBottom: '2rem' }}>
        <div className="card" style={{ padding: '1rem 1.25rem' }}>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)', textTransform: 'uppercase', display: 'block' }}>
            Bookmarked Targets
          </span>
          <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#f59e0b', marginTop: '4px' }}>
            {shortlist.length} <span style={{ fontSize: '0.9rem', color: 'var(--text-dim)', fontWeight: 500 }}>players</span>
          </div>
        </div>

        <div className="card" style={{ padding: '1rem 1.25rem' }}>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)', textTransform: 'uppercase', display: 'block' }}>
            Average Target Age
          </span>
          <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#38bdf8', marginTop: '4px' }}>
            {stats.avgAge} <span style={{ fontSize: '0.9rem', color: 'var(--text-dim)', fontWeight: 500 }}>yrs</span>
          </div>
        </div>

        <div className="card" style={{ padding: '1rem 1.25rem' }}>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)', textTransform: 'uppercase', display: 'block' }}>
            Combined Transfer Value
          </span>
          <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#34d399', marginTop: '4px' }}>
            £{stats.totalVal}M
          </div>
        </div>

        <div className="card" style={{ padding: '1rem 1.25rem' }}>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)', textTransform: 'uppercase', display: 'block' }}>
            Average Wage / Wk
          </span>
          <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#fff', marginTop: '4px' }}>
            £{stats.avgWage}k
          </div>
        </div>
      </div>

      {/* Filter / Search Bar */}
      <div className="card" style={{ padding: '1rem 1.25rem', marginBottom: '1.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '1rem' }}>
        {/* Search */}
        <div style={{ position: 'relative', minWidth: '260px' }}>
          <Search size={16} style={{ position: 'absolute', left: '12px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-dim)' }} />
          <input
            type="text"
            placeholder="Search targets by name or club..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{
              width: '100%',
              padding: '0.55rem 0.75rem 0.55rem 2.25rem',
              borderRadius: '8px',
              background: 'rgba(0,0,0,0.3)',
              border: '1px solid rgba(255,255,255,0.1)',
              color: '#fff',
              fontSize: '0.85rem',
            }}
          />
        </div>

        {/* Position Filter Pills */}
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          {[
            { id: 'ALL', label: 'All Roles' },
            { id: 'GK', label: 'Goalkeepers' },
            { id: 'DEF', label: 'Defenders' },
            { id: 'MID', label: 'Midfielders' },
            { id: 'ATT', label: 'Attackers' },
          ].map((item) => (
            <button
              key={item.id}
              onClick={() => setPosFilter(item.id)}
              style={{
                padding: '0.45rem 0.85rem',
                borderRadius: '20px',
                border: posFilter === item.id ? '1px solid #38bdf8' : '1px solid rgba(255,255,255,0.08)',
                background: posFilter === item.id ? 'rgba(56, 189, 248, 0.15)' : 'rgba(255,255,255,0.02)',
                color: posFilter === item.id ? '#38bdf8' : 'var(--text-dim)',
                fontSize: '0.8rem',
                fontWeight: 600,
                cursor: 'pointer',
              }}
            >
              {item.label}
            </button>
          ))}
        </div>
      </div>

      {/* Target Player Cards */}
      {shortlist.length === 0 ? (
        <div className="card" style={{ padding: '4rem 2rem', textAlign: 'center' }}>
          <div style={{ width: '48px', height: '48px', borderRadius: '50%', background: 'rgba(245, 158, 11, 0.1)', color: '#f59e0b', display: 'flex', alignItems: 'center', justifyContent: 'center', margin: '0 auto 1.25rem auto' }}>
            <Star size={24} />
          </div>
          <h2 style={{ fontSize: '1.25rem', fontWeight: 700, margin: '0 0 0.5rem 0' }}>
            No Targets Bookmarked Yet
          </h2>
          <p style={{ color: 'var(--text-dim)', fontSize: '0.875rem', maxWidth: '420px', margin: '0 auto 1.5rem auto' }}>
            Pin players using the star icon across the Screener, 4-Quadrant Scatter, or Succession engine to curate your transfer shortlist.
          </p>
          <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'center' }}>
            <Link href="/screener" className="btn-primary" style={{ padding: '0.6rem 1.25rem', fontSize: '0.85rem', textDecoration: 'none' }}>
              Launch Screener
            </Link>
            <Link href="/scatter" style={{ padding: '0.6rem 1.25rem', borderRadius: '8px', background: 'rgba(255,255,255,0.06)', color: '#fff', fontSize: '0.85rem', textDecoration: 'none', fontWeight: 600 }}>
              4-Quadrant Scatter
            </Link>
          </div>
        </div>
      ) : filtered.length === 0 ? (
        <div className="card" style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-dim)' }}>
          No shortlisted players matched &quot;{searchTerm}&quot; in the selected role.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
          {filtered.map((player) => (
            <div
              key={player.player_key}
              className="card"
              style={{
                padding: '1rem 1.25rem',
                display: 'grid',
                gridTemplateColumns: 'minmax(200px, 1.4fr) minmax(180px, 1fr) minmax(140px, 1fr) minmax(140px, 1fr) auto',
                gap: '1rem',
                alignItems: 'center',
                background: 'rgba(19, 23, 34, 0.6)',
              }}
            >
              {/* Player Name & Role */}
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <Link
                    href={`/player/${player.player_key}`}
                    style={{ fontWeight: 700, fontSize: '0.95rem', color: '#fff', textDecoration: 'none' }}
                  >
                    {player.player_name}
                  </Link>
                </div>
                <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                  {player.position} • {player.club_name} ({player.league})
                </div>
                {player.notes && (
                  <div style={{ fontSize: '0.7rem', color: '#fbbf24', marginTop: '4px', fontStyle: 'italic' }}>
                    📝 {player.notes}
                  </div>
                )}
              </div>

              {/* Bio & Minutes */}
              <div>
                <div style={{ fontSize: '0.8rem', color: '#fff' }}>
                  Age: <strong style={{ color: '#38bdf8' }}>{player.age} yrs</strong>
                </div>
                <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                  {player.minutes_played?.toLocaleString()} mins played
                </div>
              </div>

              {/* Valuation & Wage */}
              <div>
                <div style={{ fontSize: '0.85rem', fontWeight: 700, color: '#34d399' }}>
                  {player.transfer_value_m ? `£${player.transfer_value_m}M` : 'Free'}
                </div>
                <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                  {player.wage_per_week_k ? `£${player.wage_per_week_k}k/wk` : '-'}
                </div>
              </div>

              {/* Key Stat / Reason */}
              <div>
                <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', display: 'block', textTransform: 'uppercase' }}>
                  Highlighted Output
                </span>
                <span style={{ fontSize: '0.8rem', fontWeight: 600, color: '#38bdf8' }}>
                  {player.primary_stat || 'Target Scouted'}
                </span>
              </div>

              {/* Action Buttons */}
              <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                <Link
                  href={`/player/${player.player_key}`}
                  style={{
                    padding: '0.45rem 0.75rem',
                    borderRadius: '6px',
                    background: 'rgba(255,255,255,0.06)',
                    color: '#fff',
                    fontSize: '0.75rem',
                    textDecoration: 'none',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.35rem',
                  }}
                >
                  <span>Profile</span>
                  <ExternalLink size={12} />
                </Link>

                <button
                  onClick={() => remove(player.player_key)}
                  title="Remove from shortlist"
                  style={{
                    padding: '0.45rem',
                    borderRadius: '6px',
                    background: 'rgba(239, 68, 68, 0.1)',
                    border: '1px solid rgba(239, 68, 68, 0.2)',
                    color: '#f87171',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                  }}
                >
                  <Trash2 size={14} />
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
