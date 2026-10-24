'use client';

import React, { useEffect, useState, use, Suspense } from 'react';
import Link from 'next/link';

export const dynamic = 'force-dynamic';
import {
  Shield,
  TrendingUp,
  Coins,
  Clock,
  Award,
  GitCompare,
  ArrowLeft,
  Calendar,
  Sparkles,
  Compass,
  Zap,
  Printer,
  Sliders,
} from 'lucide-react';
import PizzaRadarChart, { RadarSlice } from '@/components/PizzaRadarChart';
import PercentileTable from '@/components/PercentileTable';
import SimilarPlayersWidget, { SimilarPlayer } from '@/components/SimilarPlayersWidget';

interface PlayerDetailResponse {
  player: {
    player_key: number;
    player_id: number;
    player_name: string;
    age: number;
    position: string;
    is_own_squad: number;
    club_name: string;
    league: string;
    league_difficulty_coef: number;
    wage_per_week_k: number;
    transfer_value_m: number;
    season: string;
  };
  stats: Record<string, number> | null;
  percentiles: Record<string, number>;
  history: Array<{
    season: string;
    age: number;
    club_name: string;
    league: string;
    wage_per_week_k: number;
    transfer_value_m: number;
    minutes_played: number;
    goals: number;
    assists: number;
    xGoals_per90: number;
    xAssits_per90: number;
  }>;
  tacticalContext?: {
    tactical_archetype: string;
    tactical_framework: string;
    tactical_description: string;
    passes_attempted_per90: number;
    pass_accuracy: number;
    pressures_attempted_per90: number;
    headers_attempted_per90: number;
    clearances_per90: number;
    crosses_attempted_per90: number;
    blocks_per90: number;
    outlier_tag: string | null;
    outlier_rationale: string | null;
  } | null;
  managedClub?: {
    club_name: string;
    league: string;
    league_difficulty_coef: number;
  };
}

// Rigorous multi-metric translation engine calibrated relative to managed club standard
function buildAdjustedSlices(rawSlices: RadarSlice[], relativeCoef: number): RadarSlice[] {
  if (Math.abs(relativeCoef - 1.0) < 0.01) return rawSlices;

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
      const deltaLogit = (1.0 - relativeCoef) * 0.65;
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
      adjVal = Number((slice.value / relativeCoef).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * relativeCoef)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // 4. Ball Recoveries & Defensive Volume (Defending against tougher teams increases defensive actions)
    if (
      slice.label === 'Possession Won' ||
      slice.label === 'Defensive Actions' ||
      slice.label === 'Duels Won / 90'
    ) {
      const defFactor = 1.0 + (1.0 - relativeCoef) * 0.15;
      adjVal = Number((slice.value * defFactor).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // 5. Passing & Progression - Mild technical decay
    if (slice.category === 'passing') {
      const passCoef = 1.0 - (1.0 - relativeCoef) * 0.40;
      adjVal = Number((slice.value * passCoef).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * passCoef)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // 6. Attacking Volume & Goal Threat (xG, Shots, Finishing) - Sharp drop against elite defenses
    if (slice.category === 'attacking') {
      adjVal = Number((slice.value * relativeCoef).toFixed(2));
      adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * relativeCoef)));
      return { ...slice, value: adjVal, percentile: adjPct };
    }

    // Default fallback
    adjVal = Number((slice.value * relativeCoef).toFixed(2));
    adjPct = Math.max(1, Math.min(99, Math.round(slice.percentile * relativeCoef)));
    return { ...slice, value: adjVal, percentile: adjPct };
  });
}

function PlayerDetailContent({ params }: { params: Promise<{ id: string }> }) {
  const resolvedParams = use(params);
  const playerKey = resolvedParams.id;

  const [data, setData] = useState<PlayerDetailResponse | null>(null);
  const [similarPlayers, setSimilarPlayers] = useState<SimilarPlayer[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isAdjusted, setIsAdjusted] = useState<boolean>(true);

  useEffect(() => {
    async function loadPlayerData() {
      setLoading(true);
      setError(null);
      try {
        const [playerRes, similarRes] = await Promise.all([
          fetch(`/api/players/${playerKey}`),
          fetch(`/api/players/${playerKey}/similar`),
        ]);

        if (!playerRes.ok) {
          throw new Error('Failed to load player dossier');
        }

        const playerData = await playerRes.json();
        const simData = await similarRes.json();

        setData(playerData);
        setSimilarPlayers(simData.similarPlayers || []);
        // Smart default: External scouting targets default to League-Adjusted; own squad defaults to Raw FM Output
        setIsAdjusted(playerData.player.is_own_squad === 0);
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    }

    loadPlayerData();
  }, [playerKey]);

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: '6rem 0', color: 'var(--text-dim)' }}>
        <div style={{ fontSize: '1.25rem', fontWeight: 600, color: '#fff', marginBottom: '0.5rem' }}>
          Loading Player Analytics Dossier...
        </div>
        <p>Crunching position percentiles and statistical twins from data warehouse...</p>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="glass-card" style={{ padding: '3rem', textAlign: 'center' }}>
        <h2 style={{ color: '#f43f5e', marginBottom: '1rem' }}>Player Not Found</h2>
        <p style={{ color: 'var(--text-muted)', marginBottom: '1.5rem' }}>
          {error || 'Could not locate player in warehouse.'}
        </p>
        <Link href="/" className="btn-primary">
          <ArrowLeft size={16} />
          <span>Return to Scouting Hub</span>
        </Link>
      </div>
    );
  }

  const { player, stats, percentiles, history, tacticalContext, managedClub } = data;
  const isGK = player.position?.toLowerCase().includes('goalkeeper');

  // Build Radar Slices mapping raw values and computed percentiles
  const slices: RadarSlice[] = !isGK
    ? [
        {
          label: 'Non-Penalty xG',
          category: 'attacking',
          value: Number((stats?.non_penalty_xGoals_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_npxg || 0,
        },
        {
          label: 'Expected Assists (xA)',
          category: 'attacking',
          value: Number(((stats as any)?.xAssists_per90 || stats?.xAssits_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_xa || 0,
        },
        {
          label: 'Goals - xG (Shrunk)',
          category: 'attacking',
          value: Number(((stats as any)?.goals_minus_xg_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_goals_minus_xg || 0,
        },
        {
          label: 'Shots / 90',
          category: 'attacking',
          value: Number((stats?.shots_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_shots || 0,
        },
        {
          label: 'xG per Shot',
          category: 'attacking',
          value: Number((stats?.xGoals_per_shot || 0).toFixed(2)),
          percentile: percentiles.pct_xg_per_shot || 0,
        },
        {
          label: 'Shot on Target %',
          category: 'attacking',
          value: Number(((stats?.shot_on_target_rate || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: percentiles.pct_shot_accuracy || 0,
        },
        {
          label: 'Prog. Passes',
          category: 'passing',
          value: Number((stats?.progressive_passes_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_prog_passes || 0,
        },
        {
          label: 'Open-Play Key Passes',
          category: 'passing',
          value: Number((stats?.open_play_key_passes_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_key_passes || 0,
        },
        {
          label: 'Pass Accuracy %',
          category: 'passing',
          value: Number(((stats?.pass_accuracy || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: percentiles.pct_pass_acc || 0,
        },
        {
          label: 'Dribbles Made',
          category: 'possession',
          value: Number((stats?.dribbles_made_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_dribbles || 0,
        },
        {
          label: 'Sprints per km',
          category: 'possession',
          value: Number(((stats as any)?.sprints_per_km || 0).toFixed(2)),
          percentile: percentiles.pct_sprints_per_km || 0,
        },
        {
          label: 'Possession Won',
          category: 'possession',
          value: Number(((stats as any)?.possession_won_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_poss_won || 0,
        },
        {
          label: 'Possession Lost',
          category: 'possession',
          value: Number(((stats as any)?.possession_lost_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_poss_lost || 0,
        },
        {
          label: 'Defensive Actions',
          category: 'defending',
          value: Number((stats?.defensive_actions_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_def_actions || 0,
        },
        {
          label: 'Duels Won / 90',
          category: 'defending',
          value: Number(((stats as any)?.duels_won_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_duels_won || 0,
        },
        {
          label: 'Press Success %',
          category: 'defending',
          value: Number(((stats as any)?.pressure_accuracy || (stats?.pressure_efficiency || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: percentiles.pct_pressure_acc || percentiles.pct_pressure_eff || 0,
        },
        {
          label: 'Aerial Duels Won',
          category: 'defending',
          value: Number((stats?.aerial_duels_won_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_aerials_won || 0,
        },
      ]
    : [
        {
          label: 'xGP / Shot (Shrunk)',
          category: 'attacking',
          value: Number(((stats as any)?.xgp_per_shot_shrunk || (stats?.xGoals_prevented_per90 || 0)).toFixed(3)),
          percentile: (percentiles as any).pct_xgp_per_shot || percentiles.pct_xg_prevented || 0,
        },
        {
          label: 'Projected Goals Saved',
          category: 'attacking',
          value: Number(((stats as any)?.xgp_per_season || 0).toFixed(1)),
          percentile: (percentiles as any).pct_xgp_season || percentiles.pct_shot_stopping_delta || 0,
        },
        {
          label: 'Goals / xGOT Ratio',
          category: 'attacking',
          value: Number(((stats as any)?.goals_vs_xgot_ratio || 1.0).toFixed(2)),
          percentile: (percentiles as any).pct_goals_vs_xgot_inv || 50,
        },
        {
          label: 'Save Percentage',
          category: 'attacking',
          value: Number(((stats?.saves_percentage || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: percentiles.pct_save_pct || 0,
        },
        {
          label: 'Hold Rate Over Expected',
          category: 'possession',
          value: Number(((stats as any)?.hold_rate_over_expected || 0).toFixed(3)),
          percentile: (percentiles as any).pct_hold_rate_oe || percentiles.pct_hold_rate || 0,
        },
        {
          label: 'Parry Rebound Risk',
          category: 'possession',
          value: Number((((stats as any)?.parry_rate || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: (percentiles as any).pct_parry_rate_inv || 50,
        },
        {
          label: 'Sweeper Actions / 90',
          category: 'defending',
          value: Number(((stats as any)?.sweeper_actions_per90 || stats?.sweeper_index || 0).toFixed(2)),
          percentile: (percentiles as any).pct_sweeper_actions || percentiles.pct_sweeper_idx || 0,
        },
        {
          label: 'Proactive High Claims',
          category: 'defending',
          value: Number((stats?.interceptions_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_interceptions || 0,
        },
        {
          label: 'Clearances / 90',
          category: 'defending',
          value: Number((stats?.clearances_per90 || 0).toFixed(2)),
          percentile: percentiles.pct_clearances || 0,
        },
        {
          label: 'Completed Passes / 90',
          category: 'passing',
          value: Number(((stats as any)?.completed_passes_per90 || stats?.distribution_value || 0).toFixed(1)),
          percentile: (percentiles as any).pct_completed_passes || percentiles.pct_distribution || 0,
        },
        {
          label: 'Pass Accuracy %',
          category: 'passing',
          value: Number(((stats?.pass_accuracy || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: percentiles.pct_pass_acc || 0,
        },
        {
          label: 'Penalty Save % (Shrunk)',
          category: 'defending',
          value: Number((((stats as any)?.pen_save_shrunk || stats?.penalties_save_percentage || 0) * 100).toFixed(1)),
          unit: '%',
          percentile: percentiles.pct_pen_save || 0,
        },
      ];

  // Relative translation calibrated against user's managed club baseline
  const userLeagueCoef = managedClub?.league_difficulty_coef ?? 0.70;
  const targetLeagueCoef = player.league_difficulty_coef ?? 1.00;
  const relativeTranslationCoef = player.is_own_squad === 1
    ? 1.00
    : Number((targetLeagueCoef / userLeagueCoef).toFixed(2));

  // Compute adjusted slices and ghost outline slices
  const activeSlices = isAdjusted ? buildAdjustedSlices(slices, relativeTranslationCoef) : slices;
  const ghostSlices = isAdjusted && Math.abs(relativeTranslationCoef - 1.0) >= 0.02 ? slices : undefined;

  return (
    <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
      {/* Executive Print-Only Dossier Header */}
      <div className="print-only" style={{ borderBottom: '2px solid #0f172a', paddingBottom: '0.75rem', marginBottom: '0.5rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <div style={{ fontSize: '1.6rem', fontWeight: 900, color: '#0f172a', letterSpacing: '-0.02em' }}>
              OFFICIAL SCOUTING DOSSIER
            </div>
            <div style={{ fontSize: '0.8rem', color: '#475569', fontWeight: 700 }}>
              RECRUITMENT & PERFORMANCE INTELLIGENCE BRIEFING
            </div>
          </div>
          <div style={{ textAlign: 'right', fontSize: '0.78rem', color: '#64748b' }}>
            <div><strong>Generated:</strong> {new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}</div>
            <div><strong>Warehouse:</strong> FM24 Gold Analytics</div>
          </div>
        </div>
      </div>

      {/* Breadcrumb & Top Action Toolbar */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }} className="no-print">
        <Link
          href="/"
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.45rem',
            color: 'var(--text-muted)',
            fontSize: '0.85rem',
            transition: 'color 0.2s',
          }}
        >
          <ArrowLeft size={16} />
          <span>Back to Scouting Hub</span>
        </Link>

        <div style={{ display: 'flex', gap: '0.6rem', alignItems: 'center' }}>
          <button
            onClick={() => window.print()}
            className="btn-secondary"
            style={{ fontSize: '0.8rem', padding: '0.45rem 0.85rem', cursor: 'pointer' }}
            title="Export clean 1-page printable / PDF executive scouting brief"
          >
            <Printer size={15} />
            <span>Export Brief (PDF / Print)</span>
          </button>

          {similarPlayers.length > 0 && (
            <Link
              href={`/compare?p1=${player.player_key}&p2=${similarPlayers[0].player_key}`}
              className="btn-secondary"
              style={{ fontSize: '0.8rem', padding: '0.45rem 0.85rem' }}
            >
              <GitCompare size={15} />
              <span>Compare with #1 Similar ({similarPlayers[0].player_name.split(' ').pop()})</span>
            </Link>
          )}
        </div>
      </div>

      {/* Header Profile Card */}
      <div
        className="glass-card"
        style={{
          background: 'linear-gradient(135deg, rgba(16, 21, 34, 0.95), rgba(10, 13, 20, 0.98))',
          padding: '2rem',
          border: '1px solid rgba(255, 255, 255, 0.1)',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: '2rem' }}>
          {/* Bio Info */}
          <div style={{ display: 'flex', gap: '1.5rem', alignItems: 'center' }}>
            <div
              style={{
                width: '74px',
                height: '74px',
                borderRadius: '16px',
                background: 'linear-gradient(135deg, #0284c7, #2563eb)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: '1.75rem',
                fontWeight: 800,
                color: '#fff',
                boxShadow: '0 8px 24px rgba(37, 99, 235, 0.35)',
              }}
            >
              {player.player_name.slice(0, 2).toUpperCase()}
            </div>

            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.65rem', marginBottom: '0.35rem' }}>
                <span className="badge badge-position">{player.position}</span>
                {player.is_own_squad === 1 ? (
                  <span className="badge badge-own-squad">CURRENT SQUAD STARTER</span>
                ) : (
                  <span className="badge badge-target">SCOUTING TRANSFER TARGET</span>
                )}
                <span style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
                  Season {player.season}
                </span>
              </div>

              <h1 style={{ fontSize: '2.4rem', fontWeight: 800, color: '#fff', letterSpacing: '-0.03em' }}>
                {player.player_name}
              </h1>

              <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginTop: '0.35rem', color: 'var(--text-muted)', fontSize: '0.95rem' }}>
                <span>
                  <strong style={{ color: '#fff' }}>{player.club_name}</strong> ({player.league})
                </span>
                <span>•</span>
                <span>{player.age} years old</span>
                <span>•</span>
                <span>
                  League Coef:{' '}
                  <strong style={{ color: '#38bdf8' }}>{player.league_difficulty_coef}x</strong>
                </span>
              </div>
            </div>
          </div>

          {/* Financials & Valuation Badges */}
          <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
            <div
              style={{
                background: 'rgba(255, 255, 255, 0.04)',
                border: '1px solid var(--border-subtle)',
                borderRadius: '12px',
                padding: '0.85rem 1.25rem',
                minWidth: '130px',
              }}
            >
              <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', fontWeight: 600 }}>TRANSFER VALUATION</div>
              <div className="mono-number" style={{ fontSize: '1.6rem', fontWeight: 800, color: '#38bdf8', marginTop: '2px' }}>
                {player.transfer_value_m ? `£${player.transfer_value_m}M` : 'Undisclosed'}
              </div>
            </div>

            <div
              style={{
                background: 'rgba(255, 255, 255, 0.04)',
                border: '1px solid var(--border-subtle)',
                borderRadius: '12px',
                padding: '0.85rem 1.25rem',
                minWidth: '130px',
              }}
            >
              <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', fontWeight: 600 }}>WAGE PER WEEK</div>
              <div className="mono-number" style={{ fontSize: '1.6rem', fontWeight: 800, color: '#fbbf24', marginTop: '2px' }}>
                {player.wage_per_week_k ? `£${player.wage_per_week_k}k` : '-'}
              </div>
            </div>

            {stats?.cost_per_xgi && (
              <div
                style={{
                  background: 'rgba(255, 255, 255, 0.04)',
                  border: '1px solid var(--border-subtle)',
                  borderRadius: '12px',
                  padding: '0.85rem 1.25rem',
                  minWidth: '130px',
                }}
              >
                <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', fontWeight: 600 }}>MONEYBALL COST/xGI</div>
                <div className="mono-number" style={{ fontSize: '1.6rem', fontWeight: 800, color: '#34d399', marginTop: '2px' }}>
                  £{(stats.cost_per_xgi / 1000).toFixed(1)}k
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Tactical Environment & System Estimation */}
        {tacticalContext && (
          <div
            style={{
              marginTop: '1.25rem',
              padding: '0.85rem 1.25rem',
              borderRadius: '10px',
              background: 'rgba(255, 255, 255, 0.03)',
              border: '1px solid var(--border-subtle)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: '0.75rem',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
              <div
                style={{
                  width: '34px',
                  height: '34px',
                  borderRadius: '8px',
                  background: 'rgba(56, 189, 248, 0.12)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: '#38bdf8',
                }}
              >
                <Compass size={18} />
              </div>
              <div>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase', fontWeight: 700, letterSpacing: '0.04em' }}>
                  Tactical Environment (Estimate)
                </div>
                <div style={{ fontSize: '0.95rem', fontWeight: 700, color: '#fff', marginTop: '1px' }}>
                  Most likely plays in a{' '}
                  <span style={{ color: '#38bdf8' }}>{tacticalContext.tactical_archetype}</span> system
                  <span style={{ fontSize: '0.78rem', color: 'var(--text-muted)', fontWeight: 400, marginLeft: '6px' }}>
                    ({tacticalContext.tactical_framework.split('. ')[1] || tacticalContext.tactical_framework})
                  </span>
                </div>
              </div>
            </div>

            {tacticalContext.outlier_tag ? (
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.45rem',
                  background: 'rgba(245, 158, 11, 0.12)',
                  border: '1px solid rgba(245, 158, 11, 0.35)',
                  padding: '0.35rem 0.75rem',
                  borderRadius: '6px',
                }}
                title={tacticalContext.outlier_rationale || undefined}
              >
                <Zap size={14} color="#fbbf24" />
                <div style={{ fontSize: '0.78rem', fontWeight: 700, color: '#fbbf24' }}>
                  Tactical Outlier: {tacticalContext.outlier_tag}
                </div>
              </div>
            ) : (
              <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                Aggregated from team metric skews (directional heuristic)
              </div>
            )}
          </div>
        )}

        {tacticalContext?.outlier_rationale && (
          <div
            style={{
              marginTop: '0.5rem',
              fontSize: '0.78rem',
              color: 'var(--text-muted)',
              paddingLeft: '0.25rem',
            }}
          >
            <strong style={{ color: '#fbbf24' }}>Scout Note:</strong> {tacticalContext.outlier_rationale}
          </div>
        )}
      </div>

      {/* Analytics Normalization Bar */}
      <div
        className="glass-card no-print"
        style={{
          padding: '0.85rem 1.25rem',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: '1rem',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', fontSize: '0.8rem', fontWeight: 700, color: '#fff' }}>
            <Sliders size={15} color="#38bdf8" />
            <span>COMPETITION TRANSLATION:</span>
          </div>
          <div
            style={{
              display: 'inline-flex',
              background: 'rgba(255, 255, 255, 0.05)',
              borderRadius: '8px',
              padding: '3px',
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
                background: !isAdjusted ? '#0284c7' : 'transparent',
                color: !isAdjusted ? '#fff' : 'var(--text-muted)',
                transition: 'all 0.2s',
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
                background: isAdjusted ? '#0284c7' : 'transparent',
                color: isAdjusted ? '#fff' : 'var(--text-muted)',
                transition: 'all 0.2s',
              }}
            >
              League-Adjusted (Normalized)
            </button>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
          <span style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>Managed Baseline:</span>
          <span
            style={{
              padding: '0.2rem 0.65rem',
              borderRadius: '6px',
              fontSize: '0.78rem',
              fontWeight: 700,
              background: 'rgba(52, 211, 153, 0.15)',
              color: '#34d399',
              border: '1px solid rgba(52, 211, 153, 0.3)',
            }}
            title="Managed club baseline against which external scout targets are calibrated"
          >
            {managedClub?.club_name || 'Macará'} ({managedClub?.league || 'LIGA MX'} • {userLeagueCoef.toFixed(2)}x)
          </span>

          <span style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>Relative Translation:</span>
          <span
            style={{
              padding: '0.2rem 0.65rem',
              borderRadius: '6px',
              fontSize: '0.78rem',
              fontWeight: 700,
              background: relativeTranslationCoef === 1.0
                ? 'rgba(255, 255, 255, 0.08)'
                : relativeTranslationCoef > 1.0
                  ? 'rgba(56, 189, 248, 0.15)'
                  : 'rgba(245, 158, 11, 0.15)',
              color: relativeTranslationCoef === 1.0
                ? 'var(--text-muted)'
                : relativeTranslationCoef > 1.0
                  ? '#38bdf8'
                  : '#fbbf24',
              border: relativeTranslationCoef === 1.0
                ? '1px solid var(--border-subtle)'
                : relativeTranslationCoef > 1.0
                  ? '1px solid rgba(56, 189, 248, 0.3)'
                  : '1px solid rgba(245, 158, 11, 0.3)',
            }}
            title={
              player.is_own_squad === 1
                ? 'Domestic squad player - evaluated directly at managed club standard'
                : `${player.league} (${targetLeagueCoef.toFixed(2)}x) relative to ${managedClub?.league || 'LIGA MX'} (${userLeagueCoef.toFixed(2)}x)`
            }
          >
            {relativeTranslationCoef.toFixed(2)}x {player.is_own_squad === 1 ? '(Domestic Squad)' : `(${player.league})`}
          </span>

          <span
            style={{
              fontSize: '0.75rem',
              padding: '0.2rem 0.55rem',
              borderRadius: '4px',
              background: (stats?.minutes_played || 0) >= 900 ? 'rgba(52, 211, 153, 0.15)' : 'rgba(251, 191, 36, 0.15)',
              color: (stats?.minutes_played || 0) >= 900 ? '#34d399' : '#fbbf24',
              border: (stats?.minutes_played || 0) >= 900 ? '1px solid rgba(52, 211, 153, 0.3)' : '1px solid rgba(251, 191, 36, 0.3)',
              fontWeight: 600,
            }}
          >
            {(stats?.minutes_played || 0) >= 900 ? 'Reliable Sample' : 'Small Sample Floor'}
          </span>
        </div>
      </div>

      {/* Main Analytics Grid: Pizza Chart vs FBref Percentiles Table */}
      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(320px, 1.15fr) minmax(320px, 1.35fr)', gap: '1.75rem' }}>
        {/* Opta / StatsBomb Pizza Radar Chart */}
        <div className="glass-card" style={{ padding: '1.75rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <div>
              <h2 style={{ fontSize: '1.25rem', fontWeight: 700, color: '#fff' }}>
                Opta / StatsBomb Radar Profile
              </h2>
              <p style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
                {isAdjusted
                  ? relativeTranslationCoef === 1.0
                    ? `Direct baseline for ${player.position}s in ${player.league}`
                    : `Calibrated projection to ${managedClub?.club_name || 'Macará'} (${managedClub?.league || 'LIGA MX'}) standard for ${player.position}s (dashed line: raw domestic)`
                  : `Raw in-game percentiles vs ${player.position} peers`}
              </p>
            </div>
          </div>

          <PizzaRadarChart
            slices={activeSlices}
            ghostSlices={ghostSlices}
            ghostLabel="Raw Domestic Baseline"
            playerName={player.player_name}
            position={player.position}
          />
        </div>

        {/* FBref Percentile Table */}
        <div className="glass-card" style={{ padding: '1.75rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <div>
              <h2 style={{ fontSize: '1.25rem', fontWeight: 700, color: '#fff' }}>
                FBref Scouting Report & Percentiles
              </h2>
              <p style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
                {isAdjusted
                  ? `League-adjusted output & rank vs ${player.position} peers`
                  : `Stat per 90 and percentile rank vs ${player.position} peers`}
              </p>
            </div>
            <span
              className="mono-number"
              style={{ fontSize: '0.8rem', color: 'var(--text-dim)', background: 'rgba(255,255,255,0.05)', padding: '0.25rem 0.6rem', borderRadius: '4px' }}
            >
              {stats?.minutes_played || 0} Mins
            </span>
          </div>

          <PercentileTable slices={activeSlices} />
        </div>
      </div>

      {/* Bottom Section: Similar Players & Career History */}
      {/* Bottom Grid: Similar Players & Career History */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.75rem' }} className="no-print">
        {/* Similar Players */}
        <SimilarPlayersWidget
          currentPlayerKey={player.player_key}
          players={similarPlayers}
        />

        {/* Career Growth & SCD Type 2 Snapshots */}
        <div className="glass-card" style={{ padding: '1.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', marginBottom: '1.25rem' }}>
            <TrendingUp size={18} color="#10b981" />
            <h3 style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>
              Historical Seasons & Development (SCD Type 2)
            </h3>
          </div>

          {history && history.length > 0 ? (
            <div className="fbref-table-container">
              <table className="fbref-table">
                <thead>
                  <tr>
                    <th>Season</th>
                    <th>Club</th>
                    <th style={{ textAlign: 'right' }}>Age</th>
                    <th style={{ textAlign: 'right' }}>Mins</th>
                    <th style={{ textAlign: 'right' }}>Goals</th>
                    <th style={{ textAlign: 'right' }}>xG/90</th>
                    <th style={{ textAlign: 'right' }}>Valuation</th>
                  </tr>
                </thead>
                <tbody>
                  {history.map((h, idx) => (
                    <tr key={idx}>
                      <td style={{ fontWeight: 600, color: '#38bdf8' }}>{h.season || 'Current'}</td>
                      <td>{h.club_name}</td>
                      <td style={{ textAlign: 'right' }} className="mono-number">
                        {h.age}
                      </td>
                      <td style={{ textAlign: 'right' }} className="mono-number">
                        {h.minutes_played?.toLocaleString() || 0}
                      </td>
                      <td style={{ textAlign: 'right', fontWeight: 700 }} className="mono-number">
                        {h.goals ?? '-'}
                      </td>
                      <td style={{ textAlign: 'right' }} className="mono-number">
                        {h.xGoals_per90 ? h.xGoals_per90.toFixed(2) : '-'}
                      </td>
                      <td style={{ textAlign: 'right', color: '#fbbf24' }} className="mono-number">
                        {h.transfer_value_m ? `£${h.transfer_value_m}M` : '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div style={{ color: 'var(--text-dim)', textAlign: 'center', padding: '2rem' }}>
              No historical snapshots recorded yet.
            </div>
          )}
        </div>
      </div>

      {/* Executive Sign-Off & Recommendation Block (Print-Only) */}
      <div
        className="print-only"
        style={{
          marginTop: '1.5rem',
          border: '1.5px solid #0f172a',
          borderRadius: '8px',
          padding: '1rem',
        }}
      >
        <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a', marginBottom: '0.5rem', textTransform: 'uppercase' }}>
          Scout Department Final Verdict & Sign-Off
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '1rem', marginBottom: '0.75rem' }}>
          <div style={{ border: '1px solid #94a3b8', borderRadius: '4px', padding: '0.4rem', fontSize: '0.8rem' }}>
            [ &nbsp; ] Priority Sign (Target A)
          </div>
          <div style={{ border: '1px solid #94a3b8', borderRadius: '4px', padding: '0.4rem', fontSize: '0.8rem' }}>
            [ &nbsp; ] Shortlist & Monitor
          </div>
          <div style={{ border: '1px solid #94a3b8', borderRadius: '4px', padding: '0.4rem', fontSize: '0.8rem' }}>
            [ &nbsp; ] Contingency / Backup
          </div>
          <div style={{ border: '1px solid #94a3b8', borderRadius: '4px', padding: '0.4rem', fontSize: '0.8rem' }}>
            [ &nbsp; ] Reject / Sub-Benchmark
          </div>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', color: '#475569', marginTop: '0.75rem', borderTop: '1px dashed #cbd5e1', paddingTop: '0.5rem' }}>
          <div>Lead Analyst: ___________________________</div>
          <div>Director of Football: ___________________________</div>
          <div>Evaluation Date: ____ / ____ / 2024</div>
        </div>
      </div>
    </div>
  );
}

export default function PlayerPage(props: { params: Promise<{ id: string }> }) {
  return (
    <Suspense fallback={<div style={{ textAlign: 'center', padding: '6rem 0', color: 'var(--text-dim)' }}>Loading Player Analytics Dossier...</div>}>
      <PlayerDetailContent params={props.params} />
    </Suspense>
  );
}
