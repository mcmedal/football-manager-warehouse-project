'use client';

import React, { useState, useEffect, Suspense } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import Link from 'next/link';
import { GitCompare, Search, ArrowRight, ArrowLeft, Check, Sparkles } from 'lucide-react';
import PizzaRadarChart, { RadarSlice } from '@/components/PizzaRadarChart';

interface PlayerComparePayload {
  player: {
    player_key: number;
    player_id?: number;
    player_name: string;
    age: number;
    position: string;
    club_name: string;
    league: string;
    league_difficulty_coef: number;
    wage_per_week_k: number;
    transfer_value_m: number;
    is_own_squad: number;
    season?: string;
    is_current_season?: number;
  };
  stats: Record<string, number> | null;
  percentiles: Record<string, number>;
}

// Robust, analytically grounded competition translation engine
function buildAdjustedSlices(rawSlices: RadarSlice[], coef: number): RadarSlice[] {
  if (Math.abs(coef - 1.0) < 0.01) return rawSlices;

  return rawSlices.map((slice) => {
    let adjVal = slice.value;
    let adjPct = slice.percentile;

    // 1. Goalkeeper Shot-Stopping: EXEMPT from league discounting (Post-Shot xGOT is trajectory/speed normalized)
    if (
      slice.label.includes('xGP') ||
      slice.label.includes('Goals Saved') ||
      slice.label.includes('xGOT') ||
      slice.label.includes('Save Percentage')
    ) {
      return { ...slice };
    }

    // 2. Rates and Percentages ([0, 1] bounded logit dampening)
    if (slice.unit === '%' || slice.label.includes('%') || slice.label.includes('Rate')) {
      const p = Math.max(0.01, Math.min(0.99, slice.value / 100));
      const logitP = Math.log(p / (1 - p));
      const deltaLogit = (1.0 - coef) * 0.65;
      const adjLogit = logitP - deltaLogit;
      adjVal = Number(((1 / (1 + Math.exp(-adjLogit))) * 100).toFixed(1));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * (adjVal / Math.max(1, slice.value)))));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // 3. Negative metrics (Lower is better: turnovers, possession lost)
    // Moving to a tougher league with higher press intensity INCREASES turnovers
    if (
      slice.label === 'Possession Lost' ||
      slice.label.includes('Turnover') ||
      slice.label.includes('Mistakes')
    ) {
      adjVal = Number((slice.value / coef).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * coef)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // 4. Ball Recoveries & Defensive Volume (Defending against tougher teams increases defensive actions)
    if (
      slice.label === 'Possession Won' ||
      slice.label === 'Defensive Actions' ||
      slice.label === 'Duels Won / 90'
    ) {
      const defFactor = 1.0 + (1.0 - coef) * 0.15;
      adjVal = Number((slice.value * defFactor).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // 5. Passing & Progression - Mild technical decay
    if (slice.category === 'passing') {
      const passCoef = 1.0 - (1.0 - coef) * 0.40;
      adjVal = Number((slice.value * passCoef).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * passCoef)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // 6. Attacking Volume & Goal Threat (xG, Shots, Finishing) - Sharp drop against elite defenses
    if (slice.category === 'attacking') {
      adjVal = Number((slice.value * coef).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * coef)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // Default fallback
    adjVal = Number((slice.value * coef).toFixed(2));
    adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * coef)));
    return { ...slice, value: adjVal, percentile: adjPct };
  });
}

function CompareContent() {
  const searchParams = useSearchParams();
  const router = useRouter();

  const p1Param = searchParams.get('p1');
  const p2Param = searchParams.get('p2');

  // Default to active 2024/25 stars: Alejandro Corozo (63591, Macará own squad) vs Louis Matip (55006, Como)
  const [p1Key, setP1Key] = useState<string>(p1Param || '63591');
  const [p2Key, setP2Key] = useState<string>(p2Param || '55006');

  // Synchronize when URL searchParams change
  useEffect(() => {
    if (p1Param && p1Param !== p1Key) setP1Key(p1Param);
    if (p2Param && p2Param !== p2Key) setP2Key(p2Param);
  }, [p1Param, p2Param]);

  const [data, setData] = useState<{
    player1: PlayerComparePayload;
    player2: PlayerComparePayload;
    managedClub?: {
      club_name: string;
      league: string;
      league_difficulty_coef: number;
    };
  } | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isAdjusted, setIsAdjusted] = useState<boolean>(true);

  // Search states for picking players
  const [p1Search, setP1Search] = useState('');
  const [p2Search, setP2Search] = useState('');
  const [p1Results, setP1Results] = useState<any[]>([]);
  const [p2Results, setP2Results] = useState<any[]>([]);

  useEffect(() => {
    if (!p1Key || !p2Key) return;

    async function loadComparison() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(`/api/compare?p1=${p1Key}&p2=${p2Key}`);
        if (!res.ok) {
          const errData = await res.json().catch(() => ({}));
          throw new Error(errData.error || 'Failed to load player comparison data');
        }
        const json = await res.json();
        setData(json);
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : 'Comparison computation error');
      } finally {
        setLoading(false);
      }
    }

    loadComparison();
  }, [p1Key, p2Key]);

  // Handle player 1 live search
  useEffect(() => {
    if (p1Search.trim().length < 2) {
      setP1Results([]);
      return;
    }
    const timer = setTimeout(async () => {
      const res = await fetch(`/api/search?q=${encodeURIComponent(p1Search.trim())}`);
      const json = await res.json();
      setP1Results(json.results || []);
    }, 200);
    return () => clearTimeout(timer);
  }, [p1Search]);

  // Handle player 2 live search
  useEffect(() => {
    if (p2Search.trim().length < 2) {
      setP2Results([]);
      return;
    }
    const timer = setTimeout(async () => {
      const res = await fetch(`/api/search?q=${encodeURIComponent(p2Search.trim())}`);
      const json = await res.json();
      setP2Results(json.results || []);
    }, 200);
    return () => clearTimeout(timer);
  }, [p2Search]);

  const selectP1 = (key: number) => {
    setP1Key(key.toString());
    setP1Search('');
    setP1Results([]);
    router.push(`/compare?p1=${key}&p2=${p2Key}`);
  };

  const selectP2 = (key: number) => {
    setP2Key(key.toString());
    setP2Search('');
    setP2Results([]);
    router.push(`/compare?p1=${p1Key}&p2=${key}`);
  };

  // Build Slices for Player 1 & Player 2 (Aware of Outfield vs Goalkeeper)
  const buildSlices = (pPayload: PlayerComparePayload): RadarSlice[] => {
    const s = pPayload.stats as any;
    const p = pPayload.percentiles as any;
    const isGK = pPayload.player.position?.toLowerCase().includes('goalkeeper');

    if (isGK) {
      return [
        {
          label: 'Save Percentage',
          category: 'attacking',
          value: Number(((s?.saves_percentage || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: p.pct_save_pct || 0,
        },
        {
          label: 'xG Prevented / 90',
          category: 'attacking',
          value: Number((s?.xGoals_prevented_per90 || 0).toFixed(2)),
          percentile: p.pct_xg_prevented || 0,
        },
        {
          label: 'Hold Rate OE',
          category: 'possession',
          value: Number((s?.hold_rate_over_expected || 0).toFixed(3)),
          percentile: p.pct_hold_rate_oe || 50,
        },
        {
          label: 'Parry Rebound Risk',
          category: 'possession',
          value: Number(((s?.parry_rate || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: p.pct_parry_rate_inv || 50,
        },
        {
          label: 'Sweeper Actions / 90',
          category: 'defending',
          value: Number((s?.sweeper_actions_per90 || 0).toFixed(2)),
          percentile: p.pct_sweeper_actions || 0,
        },
        {
          label: 'Completed Passes / 90',
          category: 'passing',
          value: Number((s?.completed_passes_per90 || 0).toFixed(1)),
          percentile: p.pct_completed_passes || 0,
        },
        {
          label: 'Pass Accuracy %',
          category: 'passing',
          value: Number(((s?.pass_accuracy || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: p.pct_pass_acc || 0,
        },
        {
          label: 'Penalty Save %',
          category: 'defending',
          value: Number(((s?.pen_save_shrunk || s?.penalties_save_percentage || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: p.pct_pen_save || 0,
        },
      ];
    }

    return [
      {
        label: 'Non-Penalty xG',
        category: 'attacking',
        value: Number((s?.non_penalty_xGoals_per90 || 0).toFixed(2)),
        percentile: p.pct_npxg || 0,
      },
      {
        label: 'xAssists (xA)',
        category: 'attacking',
        value: Number((s?.xAssits_per90 || 0).toFixed(2)),
        percentile: p.pct_xa || 0,
      },
      {
        label: 'Shots / 90',
        category: 'attacking',
        value: Number((s?.shots_per90 || 0).toFixed(2)),
        percentile: p.pct_shots || 0,
      },
      {
        label: 'xG per Shot',
        category: 'attacking',
        value: Number((s?.xGoals_per_shot || 0).toFixed(2)),
        percentile: p.pct_xg_per_shot || 0,
      },
      {
        label: 'Prog. Passes',
        category: 'passing',
        value: Number((s?.progressive_passes_per90 || 0).toFixed(2)),
        percentile: p.pct_prog_passes || 0,
      },
      {
        label: 'Key Passes',
        category: 'passing',
        value: Number((s?.open_play_key_passes_per90 || 0).toFixed(2)),
        percentile: p.pct_key_passes || 0,
      },
      {
        label: 'Pass Accuracy %',
        category: 'passing',
        value: Number(((s?.pass_accuracy || 0) * 100).toFixed(1)),
        unit: '%',
        percentile: p.pct_pass_acc || 0,
      },
      {
        label: 'Dribbles Made',
        category: 'possession',
        value: Number((s?.dribbles_made_per90 || 0).toFixed(2)),
        percentile: p.pct_dribbles || 0,
      },
      {
        label: 'Possession Won',
        category: 'possession',
        value: Number((s?.possession_won_per90 || 0).toFixed(2)),
        percentile: p.pct_poss_won || 0,
      },
      {
        label: 'Possession Lost',
        category: 'possession',
        value: Number((s?.possession_lost_per90 || 0).toFixed(2)),
        percentile: p.pct_poss_lost || 0,
      },
      {
        label: 'Defensive Actions',
        category: 'defending',
        value: Number((s?.defensive_actions_per90 || 0).toFixed(2)),
        percentile: p.pct_def_actions || 0,
      },
      {
        label: 'Aerial Duels Won',
        category: 'defending',
        value: Number((s?.aerial_duels_won_per90 || 0).toFixed(2)),
        percentile: p.pct_aerials_won || 0,
      },
    ];
  };


  const p1RawSlices = data ? buildSlices(data.player1) : [];
  const p2RawSlices = data ? buildSlices(data.player2) : [];

  const userLeagueCoef = data?.managedClub?.league_difficulty_coef ?? 0.70;
  const c1 = data?.player1.player.league_difficulty_coef ?? 1.0;
  const c2 = data?.player2.player.league_difficulty_coef ?? 1.0;

  // Relative translation multipliers calibrated against user's managed club standard (Macará @ 0.70x)
  const rel1 = data?.player1.player.is_own_squad === 1 ? 1.0 : Number((c1 / userLeagueCoef).toFixed(2));
  const rel2 = data?.player2.player.is_own_squad === 1 ? 1.0 : Number((c2 / userLeagueCoef).toFixed(2));

  const coefSpread = Math.abs(c1 - c2);
  const isCrossTierDisparity = coefSpread >= 0.20;

  const p1Slices = isAdjusted && data ? buildAdjustedSlices(p1RawSlices, rel1) : p1RawSlices;
  const p2Slices = isAdjusted && data ? buildAdjustedSlices(p2RawSlices, rel2) : p2RawSlices;

  return (
    <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
      {/* Header */}
      <div>
        <h1 style={{ fontSize: '2.25rem', fontWeight: 800, color: '#fff', letterSpacing: '-0.03em' }}>
          Head-to-Head Player Comparison
        </h1>
        <p style={{ color: 'var(--text-muted)', fontSize: '0.95rem', marginTop: '0.25rem' }}>
          Side-by-side radar overlay and statistical metric differential breakdown
        </p>
      </div>

      {/* Mode Toggle Controls */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: '1rem',
          background: 'rgba(15, 23, 42, 0.65)',
          border: '1px solid var(--border-subtle)',
          borderRadius: '12px',
          padding: '0.85rem 1.25rem',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.65rem' }}>
          <Sparkles size={16} color="var(--primary)" />
          <span style={{ fontSize: '0.85rem', fontWeight: 600, color: '#fff' }}>
            Competition Translation Engine
          </span>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
            {isAdjusted
              ? `Calibrated relative to managed club baseline: ${data?.managedClub?.club_name || 'Macará'} (${data?.managedClub?.league || 'LIGA MX'} • ${userLeagueCoef.toFixed(2)}x)`
              : 'Active: Raw unadjusted Football Manager outputs'}
          </span>
        </div>

        <div
          style={{
            display: 'inline-flex',
            background: 'rgba(0, 0, 0, 0.4)',
            padding: '3px',
            borderRadius: '8px',
            border: '1px solid var(--border-subtle)',
          }}
        >
          <button
            onClick={() => setIsAdjusted(false)}
            style={{
              padding: '0.35rem 0.85rem',
              borderRadius: '6px',
              fontSize: '0.78rem',
              fontWeight: 600,
              border: 'none',
              cursor: 'pointer',
              background: !isAdjusted ? 'var(--primary)' : 'transparent',
              color: !isAdjusted ? '#fff' : 'var(--text-dim)',
              transition: 'all 0.15s ease',
            }}
          >
            Raw FM Output
          </button>
          <button
            onClick={() => setIsAdjusted(true)}
            style={{
              padding: '0.35rem 0.85rem',
              borderRadius: '6px',
              fontSize: '0.78rem',
              fontWeight: 600,
              border: 'none',
              cursor: 'pointer',
              background: isAdjusted ? 'var(--primary)' : 'transparent',
              color: isAdjusted ? '#fff' : 'var(--text-dim)',
              transition: 'all 0.15s ease',
            }}
          >
            League-Adjusted (Normalized)
          </button>
        </div>
      </div>

      {/* Cross-Tier Spread Notice */}
      {data && isCrossTierDisparity && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.85rem',
            padding: '0.9rem 1.25rem',
            borderRadius: '10px',
            background: 'rgba(245, 158, 11, 0.1)',
            border: '1px solid rgba(245, 158, 11, 0.35)',
            color: '#fbbf24',
            fontSize: '0.85rem',
            lineHeight: 1.45,
          }}
        >
          <span style={{ fontSize: '1.25rem', flexShrink: 0 }}>⚠️</span>
          <div>
            <strong style={{ color: '#fff' }}>Cross-Tier Translation Disparity Detected:</strong>{' '}
            Comparing <strong>{data.player1.player.league}</strong> ({c1.toFixed(2)}x) against{' '}
            <strong>{data.player2.player.league}</strong> ({c2.toFixed(2)}x) bridges a{' '}
            <strong>{Math.round(coefSpread * 100)}% competition coefficient spread</strong>.{' '}
            {isAdjusted
              ? `Profiles are calibrated relative to your managed squad (${data.managedClub?.club_name || 'Macará'} in ${data.managedClub?.league || 'LIGA MX'} @ ${userLeagueCoef.toFixed(2)}x) — Player 1: ${rel1.toFixed(2)}x, Player 2: ${rel2.toFixed(2)}x.`
              : 'In Raw FM mode, performance figures reflect differing league resistance; switch to League-Adjusted for fair comparison.'}
          </div>
        </div>
      )}

      {/* Dual Selector Bar */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
        {/* Player 1 Selector */}
        <div className="glass-card" style={{ padding: '1.25rem', position: 'relative' }}>
          <label style={{ display: 'block', fontSize: '0.75rem', fontWeight: 700, color: '#38bdf8', marginBottom: '0.45rem' }}>
            PLAYER 1 (BLUE)
          </label>
          <input
            type="text"
            placeholder="Change Player 1..."
            value={p1Search}
            onChange={(e) => setP1Search(e.target.value)}
            style={{
              width: '100%',
              background: 'var(--bg-input)',
              border: '1px solid rgba(56, 189, 248, 0.4)',
              borderRadius: '8px',
              padding: '0.55rem 0.85rem',
              color: '#fff',
              fontSize: '0.85rem',
            }}
          />
          {p1Results.length > 0 && (
            <div className="search-results-popover">
              {p1Results.map((p, idx) => {
                const isCurrent = p.is_current_season === 1;
                return (
                  <div
                    key={`${p.player_key}-${idx}`}
                    className="search-result-item"
                    onClick={() => selectP1(p.player_key)}
                    style={{ cursor: 'pointer', padding: '0.65rem 0.85rem' }}
                  >
                    <div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', flexWrap: 'wrap' }}>
                        <span style={{ fontWeight: 600, color: '#fff', fontSize: '0.88rem' }}>{p.player_name}</span>
                        <span
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '3px',
                            padding: '1px 6px',
                            borderRadius: '4px',
                            fontSize: '0.65rem',
                            fontWeight: 700,
                            background: isCurrent ? 'rgba(16, 185, 129, 0.15)' : 'rgba(148, 163, 184, 0.12)',
                            color: isCurrent ? '#34d399' : '#94a3b8',
                            border: isCurrent ? '1px solid rgba(16, 185, 129, 0.35)' : '1px solid rgba(148, 163, 184, 0.2)',
                          }}
                        >
                          {isCurrent && <span style={{ fontSize: '0.55rem' }}>●</span>}
                          {p.season ? `${p.season} ` : ''}{isCurrent ? 'Current' : 'Previous'}
                        </span>
                      </div>
                      <div style={{ fontSize: '0.74rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                        {p.club_name} • {p.position} • {p.league}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right', flexShrink: 0, marginLeft: '0.75rem' }}>
                      <div className="mono-number" style={{ fontSize: '0.85rem', color: '#38bdf8', fontWeight: 600 }}>
                        {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                      </div>
                      {p.is_own_squad === 1 && (
                        <span style={{ fontSize: '0.65rem', color: '#34d399', fontWeight: 700 }}>OWN SQUAD</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Player 2 Selector */}
        <div className="glass-card" style={{ padding: '1.25rem', position: 'relative' }}>
          <label style={{ display: 'block', fontSize: '0.75rem', fontWeight: 700, color: '#f43f5e', marginBottom: '0.45rem' }}>
            PLAYER 2 (ROSE)
          </label>
          <input
            type="text"
            placeholder="Change Player 2..."
            value={p2Search}
            onChange={(e) => setP2Search(e.target.value)}
            style={{
              width: '100%',
              background: 'var(--bg-input)',
              border: '1px solid rgba(244, 63, 94, 0.4)',
              borderRadius: '8px',
              padding: '0.55rem 0.85rem',
              color: '#fff',
              fontSize: '0.85rem',
            }}
          />
          {p2Results.length > 0 && (
            <div className="search-results-popover">
              {p2Results.map((p, idx) => {
                const isCurrent = p.is_current_season === 1;
                return (
                  <div
                    key={`${p.player_key}-${idx}`}
                    className="search-result-item"
                    onClick={() => selectP2(p.player_key)}
                    style={{ cursor: 'pointer', padding: '0.65rem 0.85rem' }}
                  >
                    <div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', flexWrap: 'wrap' }}>
                        <span style={{ fontWeight: 600, color: '#fff', fontSize: '0.88rem' }}>{p.player_name}</span>
                        <span
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '3px',
                            padding: '1px 6px',
                            borderRadius: '4px',
                            fontSize: '0.65rem',
                            fontWeight: 700,
                            background: isCurrent ? 'rgba(16, 185, 129, 0.15)' : 'rgba(148, 163, 184, 0.12)',
                            color: isCurrent ? '#34d399' : '#94a3b8',
                            border: isCurrent ? '1px solid rgba(16, 185, 129, 0.35)' : '1px solid rgba(148, 163, 184, 0.2)',
                          }}
                        >
                          {isCurrent && <span style={{ fontSize: '0.55rem' }}>●</span>}
                          {p.season ? `${p.season} ` : ''}{isCurrent ? 'Current' : 'Previous'}
                        </span>
                      </div>
                      <div style={{ fontSize: '0.74rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                        {p.club_name} • {p.position} • {p.league}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right', flexShrink: 0, marginLeft: '0.75rem' }}>
                      <div className="mono-number" style={{ fontSize: '0.85rem', color: '#f43f5e', fontWeight: 600 }}>
                        {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                      </div>
                      {p.is_own_squad === 1 && (
                        <span style={{ fontSize: '0.65rem', color: '#34d399', fontWeight: 700 }}>OWN SQUAD</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* Comparison Presentation */}
      {error ? (
        <div
          className="glass-card"
          style={{
            padding: '2.5rem',
            textAlign: 'center',
            border: '1px solid rgba(239, 68, 68, 0.4)',
            background: 'rgba(239, 68, 68, 0.05)',
            borderRadius: '12px',
          }}
        >
          <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>⚠️</div>
          <h3 style={{ color: '#fff', fontSize: '1.2rem', fontWeight: 700, marginBottom: '0.5rem' }}>
            Comparison Data Unavailable
          </h3>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem', marginBottom: '1.25rem', maxWidth: '500px', margin: '0 auto 1.25rem' }}>
            {error}
          </p>
          <button
            onClick={() => {
              setP1Key('63591');
              setP2Key('55006');
              router.push('/compare?p1=63591&p2=55006');
            }}
            className="btn-primary"
            style={{ padding: '0.55rem 1.25rem', fontSize: '0.85rem' }}
          >
            Reset to Recommended Comparison (Corozo vs Matip)
          </button>
        </div>
      ) : loading ? (
        <div style={{ textAlign: 'center', padding: '5rem 0', color: 'var(--text-dim)' }}>
          Computing head-to-head metrics from warehouse...
        </div>
      ) : data ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          {/* Top Banner Player Cards */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 80px 1fr', alignItems: 'center', gap: '1rem' }}>
            {/* Player 1 Card */}
            <div
              className="glass-card"
              style={{
                border: '1px solid rgba(56, 189, 248, 0.4)',
                background: 'linear-gradient(135deg, rgba(56, 189, 248, 0.1), rgba(18, 24, 38, 0.8))',
                padding: '1.5rem',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
                <span style={{ fontSize: '0.75rem', color: '#38bdf8', fontWeight: 700, textTransform: 'uppercase' }}>
                  Player 1 • {data.player1.player.position}
                </span>
                <span
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '3px',
                    padding: '1px 6px',
                    borderRadius: '4px',
                    fontSize: '0.68rem',
                    fontWeight: 700,
                    background: data.player1.player.is_current_season === 1 ? 'rgba(16, 185, 129, 0.15)' : 'rgba(148, 163, 184, 0.12)',
                    color: data.player1.player.is_current_season === 1 ? '#34d399' : '#94a3b8',
                    border: data.player1.player.is_current_season === 1 ? '1px solid rgba(16, 185, 129, 0.35)' : '1px solid rgba(148, 163, 184, 0.25)',
                  }}
                >
                  {data.player1.player.is_current_season === 1 && <span style={{ fontSize: '0.55rem' }}>●</span>}
                  {data.player1.player.season || '2024/25'} {data.player1.player.is_current_season === 1 ? 'Current' : 'Previous'}
                </span>
              </div>
              <h2 style={{ fontSize: '1.6rem', fontWeight: 800, color: '#fff', marginTop: '0.2rem' }}>
                <Link href={`/player/${data.player1.player.player_key}`}>{data.player1.player.player_name}</Link>
              </h2>
              <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
                {data.player1.player.club_name} ({data.player1.player.league} • {c1.toFixed(2)}x{isAdjusted && rel1 !== 1.0 ? ` → ${rel1.toFixed(2)}x vs Macará` : ''}) • {data.player1.player.age} yrs
              </div>
              <div style={{ display: 'flex', gap: '1rem', marginTop: '1rem' }}>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>VALUATION:</span>
                  <div className="mono-number" style={{ fontSize: '1.1rem', fontWeight: 700, color: '#38bdf8' }}>
                    {data.player1.player.transfer_value_m ? `£${data.player1.player.transfer_value_m}M` : '-'}
                  </div>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>WAGE:</span>
                  <div className="mono-number" style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fbbf24' }}>
                    {data.player1.player.wage_per_week_k ? `£${data.player1.player.wage_per_week_k}k` : '-'}
                  </div>
                </div>
              </div>
            </div>

            {/* VS Badge */}
            <div style={{ display: 'flex', justifyContent: 'center' }}>
              <div
                style={{
                  width: '48px',
                  height: '48px',
                  borderRadius: '50%',
                  background: 'rgba(255, 255, 255, 0.08)',
                  border: '1px solid var(--border-subtle)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontWeight: 800,
                  fontSize: '0.85rem',
                  color: 'var(--text-muted)',
                }}
              >
                VS
              </div>
            </div>

            {/* Player 2 Card */}
            <div
              className="glass-card"
              style={{
                border: '1px solid rgba(244, 63, 94, 0.4)',
                background: 'linear-gradient(135deg, rgba(244, 63, 94, 0.1), rgba(18, 24, 38, 0.8))',
                padding: '1.5rem',
                textAlign: 'right',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', justifyContent: 'flex-end', flexWrap: 'wrap' }}>
                <span
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '3px',
                    padding: '1px 6px',
                    borderRadius: '4px',
                    fontSize: '0.68rem',
                    fontWeight: 700,
                    background: data.player2.player.is_current_season === 1 ? 'rgba(16, 185, 129, 0.15)' : 'rgba(148, 163, 184, 0.12)',
                    color: data.player2.player.is_current_season === 1 ? '#34d399' : '#94a3b8',
                    border: data.player2.player.is_current_season === 1 ? '1px solid rgba(16, 185, 129, 0.35)' : '1px solid rgba(148, 163, 184, 0.25)',
                  }}
                >
                  {data.player2.player.is_current_season === 1 && <span style={{ fontSize: '0.55rem' }}>●</span>}
                  {data.player2.player.season || '2024/25'} {data.player2.player.is_current_season === 1 ? 'Current' : 'Previous'}
                </span>
                <span style={{ fontSize: '0.75rem', color: '#f43f5e', fontWeight: 700, textTransform: 'uppercase' }}>
                  Player 2 • {data.player2.player.position}
                </span>
              </div>
              <h2 style={{ fontSize: '1.6rem', fontWeight: 800, color: '#fff', marginTop: '0.2rem' }}>
                <Link href={`/player/${data.player2.player.player_key}`}>{data.player2.player.player_name}</Link>
              </h2>
              <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
                {data.player2.player.club_name} ({data.player2.player.league} • {c2.toFixed(2)}x{isAdjusted && rel2 !== 1.0 ? ` → ${rel2.toFixed(2)}x vs Macará` : ''}) • {data.player2.player.age} yrs
              </div>
              <div style={{ display: 'flex', gap: '1rem', marginTop: '1rem', justifyContent: 'flex-end' }}>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>VALUATION:</span>
                  <div className="mono-number" style={{ fontSize: '1.1rem', fontWeight: 700, color: '#f43f5e' }}>
                    {data.player2.player.transfer_value_m ? `£${data.player2.player.transfer_value_m}M` : '-'}
                  </div>
                </div>
                <div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>WAGE:</span>
                  <div className="mono-number" style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fbbf24' }}>
                    {data.player2.player.wage_per_week_k ? `£${data.player2.player.wage_per_week_k}k` : '-'}
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Main Visuals Grid */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1.25fr', gap: '1.75rem' }}>
            {/* Overlaid Radar Chart */}
            <div className="glass-card" style={{ padding: '1.75rem' }}>
              <div style={{ textAlign: 'center', marginBottom: '1rem' }}>
                <h3 style={{ fontSize: '1.15rem', fontWeight: 700, color: '#fff' }}>
                  Overlaid Radar Percentile Profiles
                </h3>
                <div style={{ display: 'flex', justifyContent: 'center', gap: '1.5rem', marginTop: '0.5rem', fontSize: '0.85rem' }}>
                  <span style={{ color: '#38bdf8', fontWeight: 600 }}>● {data.player1.player.player_name}</span>
                  <span style={{ color: '#f43f5e', fontWeight: 600 }}>● {data.player2.player.player_name}</span>
                </div>
              </div>

              <PizzaRadarChart
                slices={p1Slices}
                playerName={data.player1.player.player_name}
                position={data.player1.player.position}
                comparisonSlices={p2Slices}
                comparisonName={data.player2.player.player_name}
                ghostSlices={isAdjusted ? p1RawSlices : undefined}
                ghostLabel={`${data.player1.player.player_name} (Raw Domestic)`}
              />
            </div>

            {/* Differential Breakdown Table */}
            <div className="glass-card" style={{ padding: '1.75rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                <h3 style={{ fontSize: '1.15rem', fontWeight: 700, color: '#fff' }}>
                  Statistical Differential Table
                </h3>
                <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                  {isAdjusted ? 'Values: League-Adjusted' : 'Values: Raw FM Output'}
                </span>
              </div>

              <div className="fbref-table-container">
                <table className="fbref-table">
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left' }}>Metric</th>
                      <th style={{ textAlign: 'center', color: '#38bdf8' }}>{data.player1.player.player_name.split(' ').pop()}</th>
                      <th style={{ textAlign: 'center', width: '60px' }}>Edge</th>
                      <th style={{ textAlign: 'center', color: '#f43f5e' }}>{data.player2.player.player_name.split(' ').pop()}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {p1Slices.map((s1, idx) => {
                      const s2 = p2Slices[idx];
                      if (!s2) return null;
                      const leader = s1.value > s2.value ? 1 : s2.value > s1.value ? 2 : 0;

                      return (
                        <tr key={idx}>
                          <td style={{ fontWeight: 500, color: '#e2e8f0' }}>{s1.label}</td>

                          {/* P1 Value & % */}
                          <td style={{ textAlign: 'center', background: leader === 1 ? 'rgba(56, 189, 248, 0.08)' : 'transparent' }}>
                            <span className="mono-number" style={{ fontWeight: 700, color: leader === 1 ? '#38bdf8' : '#fff' }}>
                              {s1.value}
                            </span>
                            <span style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginLeft: '4px' }}>
                              ({Math.round(s1.percentile)}%)
                            </span>
                          </td>

                          {/* Edge Arrow */}
                          <td style={{ textAlign: 'center' }}>
                            {leader === 1 ? (
                              <span style={{ color: '#38bdf8', fontWeight: 800 }}>◀</span>
                            ) : leader === 2 ? (
                              <span style={{ color: '#f43f5e', fontWeight: 800 }}>▶</span>
                            ) : (
                              <span style={{ color: 'var(--text-dim)' }}>=</span>
                            )}
                          </td>

                          {/* P2 Value & % */}
                          <td style={{ textAlign: 'center', background: leader === 2 ? 'rgba(244, 63, 94, 0.08)' : 'transparent' }}>
                            <span className="mono-number" style={{ fontWeight: 700, color: leader === 2 ? '#f43f5e' : '#fff' }}>
                              {s2.value}
                            </span>
                            <span style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginLeft: '4px' }}>
                              ({Math.round(s2.percentile)}%)
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default function ComparePage() {
  return (
    <Suspense fallback={<div style={{ textAlign: 'center', padding: '4rem 0', color: 'var(--text-dim)' }}>Loading comparison module...</div>}>
      <CompareContent />
    </Suspense>
  );
}
