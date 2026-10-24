'use client';

import React from 'react';
import Link from 'next/link';
import { Users, ArrowUpRight, GitCompare } from 'lucide-react';

export interface SimilarPlayer {
  player_key: number;
  player_name: string;
  age: number;
  club_name: string;
  league: string;
  wage_per_week_k: number;
  transfer_value_m: number;
  minutes_played: number;
  non_penalty_xGoals_per90: number;
  xAssits_per90: number;
  progressive_passes_per90: number;
  defensive_actions_per90: number;
  similarity_score: number;
}

interface SimilarPlayersWidgetProps {
  currentPlayerKey: number;
  players: SimilarPlayer[];
}

export default function SimilarPlayersWidget({
  currentPlayerKey,
  players,
}: SimilarPlayersWidgetProps) {
  if (!players || players.length === 0) {
    return (
      <div className="glass-card" style={{ padding: '1.5rem', textAlign: 'center', color: 'var(--text-dim)' }}>
        No similarity matches found for this position.
      </div>
    );
  }

  return (
    <div className="glass-card" style={{ padding: '1.5rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.25rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <Users size={18} color="#38bdf8" />
          <h3 style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>
            FBref Similar Players
          </h3>
        </div>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
          Euclidean Vector Model
        </span>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
        {players.map((p) => (
          <div
            key={p.player_key}
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              padding: '0.85rem 1rem',
              borderRadius: '8px',
              background: 'rgba(255, 255, 255, 0.03)',
              border: '1px solid rgba(255, 255, 255, 0.06)',
              transition: 'all 0.2s ease',
            }}
          >
            {/* Player Info */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem' }}>
              <div
                style={{
                  width: '38px',
                  height: '38px',
                  borderRadius: '50%',
                  background: 'linear-gradient(135deg, rgba(56, 189, 248, 0.2), rgba(37, 99, 235, 0.3))',
                  border: '1px solid rgba(56, 189, 248, 0.4)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: '0.85rem',
                  fontWeight: 700,
                  color: '#38bdf8',
                }}
              >
                {p.player_name.slice(0, 2).toUpperCase()}
              </div>

              <div>
                <Link
                  href={`/player/${p.player_key}`}
                  style={{
                    fontWeight: 600,
                    fontSize: '0.95rem',
                    color: '#fff',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.35rem',
                  }}
                >
                  <span>{p.player_name}</span>
                  <ArrowUpRight size={14} color="var(--text-dim)" />
                </Link>
                <div style={{ fontSize: '0.78rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                  {p.club_name} • {p.age} yrs • {p.league}
                </div>
              </div>
            </div>

            {/* Metrics & Match Score */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem' }}>
              <div style={{ textAlign: 'right' }}>
                <div className="mono-number" style={{ fontSize: '0.9rem', fontWeight: 700, color: '#38bdf8' }}>
                  {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                </div>
                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                  {p.wage_per_week_k ? `£${p.wage_per_week_k}k/wk` : '-'}
                </div>
              </div>

              {/* Match Score Badge */}
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  justifyContent: 'center',
                  background: 'rgba(16, 185, 129, 0.12)',
                  border: '1px solid rgba(16, 185, 129, 0.35)',
                  padding: '0.35rem 0.65rem',
                  borderRadius: '6px',
                  minWidth: '65px',
                }}
              >
                <span className="mono-number" style={{ fontSize: '0.95rem', fontWeight: 800, color: '#34d399' }}>
                  {p.similarity_score}%
                </span>
                <span style={{ fontSize: '0.62rem', fontWeight: 600, color: 'var(--text-dim)', textTransform: 'uppercase' }}>
                  Match
                </span>
              </div>

              {/* Compare Button */}
              <Link
                href={`/compare?p1=${currentPlayerKey}&p2=${p.player_key}`}
                className="btn-secondary"
                style={{ padding: '0.45rem 0.75rem', fontSize: '0.78rem' }}
                title="Compare side-by-side"
              >
                <GitCompare size={14} />
                <span>Compare</span>
              </Link>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
