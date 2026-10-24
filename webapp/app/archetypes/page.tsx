'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import { 
  Crosshair, 
  Users, 
  Shield, 
  TrendingUp, 
  Sparkles, 
  ArrowRight, 
  Layers, 
  Flag, 
  Flame, 
  Compass, 
  Activity,
  CheckCircle2,
  AlertTriangle,
  Zap
} from 'lucide-react';

interface ArchetypeSummary {
  tactical_archetype: string;
  player_count: number;
  avg_value: number;
  avg_npxg_xa: number;
}

interface ArchetypePlayer {
  player_key: number;
  player_name: string;
  age: number;
  position: string;
  club_name: string;
  league: string;
  minutes_played: number;
  transfer_value_m: number;
  wage_per_week_k: number;
  tactical_archetype: string;
  npxg_plus_xa_per90: number;
  progressive_passes_per90: number;
  defensive_actions_per90: number;
  cost_per_xgi: number;
}

interface TeamArchetypeSummary {
  tactical_archetype: string;
  tactical_framework: string;
  team_count: number;
  avg_passes_per90: number;
  avg_pass_accuracy: number;
  avg_pressures_per90: number;
  avg_headers_per90: number;
  avg_clearances_per90: number;
  avg_crosses_per90: number;
  avg_blocks_per90: number;
}

interface TeamData {
  team_key: string;
  club_name: string;
  league: string;
  active_players: number;
  total_minutes: number;
  tactical_archetype: string;
  tactical_framework: string;
  tactical_description: string;
  passes_attempted_per90: number;
  pass_accuracy: number;
  pressures_attempted_per90: number;
  progressive_passes_per90: number;
  headers_attempted_per90: number;
  clearances_per90: number;
  crosses_attempted_per90: number;
  blocks_per90: number;
  dribbles_made_per90: number;
  sprints_per90: number;
  gegenpressing_score: number;
  tiki_taka_score: number;
  vertical_tiki_taka_score: number;
  fluid_counter_score: number;
  direct_counter_score: number;
  wing_play_score: number;
  route_one_score: number;
  catenaccio_score: number;
}

interface OutlierPlayer {
  player_key: number;
  player_name: string;
  age: number;
  position: string;
  club_name: string;
  league: string;
  is_own_squad: boolean;
  minutes_played: number;
  transfer_value_m: number;
  wage_per_week_k: number;
  team_tactical_archetype: string;
  team_tactical_framework: string;
  outlier_tag: string;
  outlier_rationale: string;
  player_pass_accuracy: number;
  player_passes_attempted_per90: number;
  player_clearances_per90: number;
  player_npxg_plus_xa_per90: number;
  player_chances_created_per90: number;
  player_open_play_key_passes_per90: number;
  player_progressive_passes_per90: number;
  player_dribbles_made_per90: number;
  player_pressures_attempted_per90: number;
  team_passes_attempted_per90: number;
  team_pass_accuracy: number;
  team_clearances_per90: number;
  team_pressures_attempted_per90: number;
}

interface TagItem {
  outlier_tag: string;
  count: number;
}

export default function ArchetypesPage() {
  const [activeTab, setActiveTab] = useState<'roles' | 'teams' | 'outliers'>('teams');

  // --- 1. Player Roles State ---
  const [archetypes, setArchetypes] = useState<ArchetypeSummary[]>([]);
  const [selectedRole, setSelectedRole] = useState<string>('Pressing / Advanced Forward');
  const [players, setPlayers] = useState<ArchetypePlayer[]>([]);
  const [loadingRoles, setLoadingRoles] = useState(false);
  const [loadingPlayers, setLoadingPlayers] = useState(false);

  // --- 2. Team Archetypes State ---
  const [teamSummaries, setTeamSummaries] = useState<TeamArchetypeSummary[]>([]);
  const [selectedTeamArchetype, setSelectedTeamArchetype] = useState<string>('Gegenpressing');
  const [teams, setTeams] = useState<TeamData[]>([]);
  const [loadingTeamSummaries, setLoadingTeamSummaries] = useState(false);
  const [loadingTeams, setLoadingTeams] = useState(false);

  // --- 3. Tactical Outliers State ---
  const [outliers, setOutliers] = useState<OutlierPlayer[]>([]);
  const [tags, setTags] = useState<TagItem[]>([]);
  const [selectedTag, setSelectedTag] = useState<string>('All');
  const [loadingOutliers, setLoadingOutliers] = useState(false);

  // Initial Load for Active Tab
  useEffect(() => {
    if (activeTab === 'roles' && archetypes.length === 0) {
      loadArchetypes();
    } else if (activeTab === 'teams' && teamSummaries.length === 0) {
      loadTeamSummaries();
    } else if (activeTab === 'outliers' && outliers.length === 0) {
      loadOutliers();
    }
  }, [activeTab]);

  // Load Player Roles Summaries
  async function loadArchetypes() {
    setLoadingRoles(true);
    try {
      const res = await fetch('/api/archetypes?type=roles');
      const json = await res.json();
      setArchetypes(json.archetypes || []);
    } catch (err) {
      console.error('Error loading archetypes:', err);
    } finally {
      setLoadingRoles(false);
    }
  }

  // Load Players for Selected Role
  useEffect(() => {
    if (activeTab !== 'roles' || !selectedRole) return;
    async function loadPlayers() {
      setLoadingPlayers(true);
      try {
        const res = await fetch(`/api/archetypes?type=roles&archetype=${encodeURIComponent(selectedRole)}`);
        const json = await res.json();
        setPlayers(json.players || []);
      } catch (err) {
        console.error('Error loading archetype players:', err);
      } finally {
        setLoadingPlayers(false);
      }
    }
    loadPlayers();
  }, [activeTab, selectedRole]);

  // Load Team Archetype Summaries
  async function loadTeamSummaries() {
    setLoadingTeamSummaries(true);
    try {
      const res = await fetch('/api/archetypes?type=teams');
      const json = await res.json();
      setTeamSummaries(json.summary || []);
    } catch (err) {
      console.error('Error loading team summaries:', err);
    } finally {
      setLoadingTeamSummaries(false);
    }
  }

  // Load Teams for Selected Archetype
  useEffect(() => {
    if (activeTab !== 'teams' || !selectedTeamArchetype) return;
    async function loadTeams() {
      setLoadingTeams(true);
      try {
        const res = await fetch(`/api/archetypes?type=teams&archetype=${encodeURIComponent(selectedTeamArchetype)}`);
        const json = await res.json();
        setTeams(json.teams || []);
      } catch (err) {
        console.error('Error loading teams for archetype:', err);
      } finally {
        setLoadingTeams(false);
      }
    }
    loadTeams();
  }, [activeTab, selectedTeamArchetype]);

  // Load Outliers
  async function loadOutliers(tagFilter: string = selectedTag) {
    setLoadingOutliers(true);
    try {
      const param = tagFilter !== 'All' ? `&outlierTag=${encodeURIComponent(tagFilter)}` : '';
      const res = await fetch(`/api/archetypes?type=outliers${param}`);
      const json = await res.json();
      setOutliers(json.outliers || []);
      if (json.tags && tags.length === 0) {
        setTags(json.tags);
      }
    } catch (err) {
      console.error('Error loading outliers:', err);
    } finally {
      setLoadingOutliers(false);
    }
  }

  const handleTagChange = (tag: string) => {
    setSelectedTag(tag);
    loadOutliers(tag);
  };

  return (
    <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
      {/* Header & Sub-Navigation */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: '1.25rem' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
            <span className="badge badge-position">TACTICAL INTELLIGENCE</span>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
              gold.dim_team_tactical_archetypes & gold.vw_player_system_outliers
            </span>
          </div>
          <h1 style={{ fontSize: '2.25rem', fontWeight: 800, color: '#fff', letterSpacing: '-0.03em' }}>
            Tactical Archetypes & System Outliers
          </h1>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.95rem', maxWidth: '780px' }}>
            Evaluate team-level tactical frameworks and pinpoint performance outliers—players whose high-value traits operate against their club’s prevailing tactical constraints.
          </p>
        </div>

        {/* Tab Controls */}
        <div style={{ display: 'flex', gap: '0.4rem', background: 'rgba(255, 255, 255, 0.05)', padding: '4px', borderRadius: '8px' }}>
          <button
            onClick={() => setActiveTab('teams')}
            style={{
              padding: '0.5rem 1rem',
              fontSize: '0.85rem',
              fontWeight: 700,
              borderRadius: '6px',
              border: 'none',
              cursor: 'pointer',
              color: activeTab === 'teams' ? '#fff' : 'var(--text-dim)',
              background: activeTab === 'teams' ? '#0284c7' : 'transparent',
              transition: 'all 0.15s ease',
              display: 'flex',
              alignItems: 'center',
              gap: '0.4rem',
            }}
          >
            <Compass size={15} />
            Team Frameworks
          </button>
          <button
            onClick={() => setActiveTab('outliers')}
            style={{
              padding: '0.5rem 1rem',
              fontSize: '0.85rem',
              fontWeight: 700,
              borderRadius: '6px',
              border: 'none',
              cursor: 'pointer',
              color: activeTab === 'outliers' ? '#fff' : 'var(--text-dim)',
              background: activeTab === 'outliers' ? '#0284c7' : 'transparent',
              transition: 'all 0.15s ease',
              display: 'flex',
              alignItems: 'center',
              gap: '0.4rem',
            }}
          >
            <Zap size={15} />
            Tactical Outliers Finder
          </button>
          <button
            onClick={() => setActiveTab('roles')}
            style={{
              padding: '0.5rem 1rem',
              fontSize: '0.85rem',
              fontWeight: 700,
              borderRadius: '6px',
              border: 'none',
              cursor: 'pointer',
              color: activeTab === 'roles' ? '#fff' : 'var(--text-dim)',
              background: activeTab === 'roles' ? '#0284c7' : 'transparent',
              transition: 'all 0.15s ease',
              display: 'flex',
              alignItems: 'center',
              gap: '0.4rem',
            }}
          >
            <Users size={15} />
            Player Roles (22)
          </button>
        </div>
      </div>

      {/* ========================================================================= */}
      {/* TAB 1: TEAM TACTICAL FRAMEWORKS                                          */}
      {/* ========================================================================= */}
      {activeTab === 'teams' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          {/* Archetype Selector Cards */}
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(240px, 1fr))',
              gap: '0.85rem',
            }}
          >
            {loadingTeamSummaries ? (
              <div style={{ color: 'var(--text-dim)', padding: '1rem' }}>Loading team tactical frameworks...</div>
            ) : (
              teamSummaries.map((s) => {
                const isSelected = selectedTeamArchetype === s.tactical_archetype;
                return (
                  <button
                    key={s.tactical_archetype}
                    onClick={() => setSelectedTeamArchetype(s.tactical_archetype)}
                    className="glass-card"
                    style={{
                      padding: '1.1rem',
                      textAlign: 'left',
                      cursor: 'pointer',
                      border: isSelected ? '1px solid #38bdf8' : '1px solid var(--border-subtle)',
                      background: isSelected ? 'rgba(56, 189, 248, 0.14)' : 'var(--bg-card)',
                      transition: 'all 0.2s ease',
                      position: 'relative',
                    }}
                  >
                    <div style={{ fontSize: '0.7rem', color: isSelected ? '#38bdf8' : 'var(--text-dim)', textTransform: 'uppercase', fontWeight: 700 }}>
                      {s.tactical_framework.split('. ')[1] || s.tactical_framework}
                    </div>
                    <div style={{ fontWeight: 800, fontSize: '1rem', color: '#fff', margin: '3px 0 6px 0' }}>
                      {s.tactical_archetype}
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '0.78rem', color: 'var(--text-muted)' }}>
                      <span>{s.team_count} Clubs</span>
                      <span className="mono-number" style={{ color: '#38bdf8' }}>{s.avg_passes_per90} passes/90</span>
                    </div>

                    <div style={{ display: 'flex', gap: '0.5rem', marginTop: '0.6rem', fontSize: '0.72rem' }}>
                      <span className="badge" style={{ background: 'rgba(255,255,255,0.06)' }}>
                        Press: {s.avg_pressures_per90}/90
                      </span>
                      <span className="badge" style={{ background: 'rgba(255,255,255,0.06)' }}>
                        Acc: {s.avg_pass_accuracy}%
                      </span>
                    </div>
                  </button>
                );
              })
            )}
          </div>

          {/* Selected Archetype Team Table */}
          <div className="glass-card" style={{ padding: '1.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '1.25rem', flexWrap: 'wrap', gap: '1rem' }}>
              <div>
                <h2 style={{ fontSize: '1.35rem', fontWeight: 800, color: '#fff' }}>
                  Clubs Operating in: {selectedTeamArchetype}
                </h2>
                <p style={{ fontSize: '0.85rem', color: 'var(--text-dim)', maxWidth: '700px', marginTop: '2px' }}>
                  {teams[0]?.tactical_description || 'Tactical profile aggregated from minutes-weighted player performances.'}
                </p>
              </div>
              <span className="mono-number" style={{ fontSize: '0.85rem', color: '#38bdf8' }}>
                Showing top {teams.length} clubs
              </span>
            </div>

            <div className="fbref-table-container">
              <table className="fbref-table">
                <thead>
                  <tr>
                    <th>Club</th>
                    <th>League</th>
                    <th style={{ textAlign: 'right' }}>Active Players</th>
                    <th style={{ textAlign: 'right' }}>Total Mins</th>
                    <th style={{ textAlign: 'right', color: '#38bdf8' }}>Passes/90</th>
                    <th style={{ textAlign: 'right' }}>Pass Acc</th>
                    <th style={{ textAlign: 'right', color: '#fb7185' }}>Pressures/90</th>
                    <th style={{ textAlign: 'right' }}>Headers/90</th>
                    <th style={{ textAlign: 'right' }}>Clearances/90</th>
                    <th style={{ textAlign: 'right' }}>Crosses/90</th>
                    <th style={{ textAlign: 'right' }}>Blocks/90</th>
                  </tr>
                </thead>
                <tbody>
                  {loadingTeams ? (
                    <tr>
                      <td colSpan={11} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                        Loading clubs for this tactical framework...
                      </td>
                    </tr>
                  ) : teams.length === 0 ? (
                    <tr>
                      <td colSpan={11} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                        No clubs found for this archetype.
                      </td>
                    </tr>
                  ) : (
                    teams.map((t) => (
                      <tr key={t.team_key}>
                        <td style={{ fontWeight: 700, color: '#fff' }}>
                          {t.club_name}
                        </td>
                        <td style={{ color: 'var(--text-dim)', fontSize: '0.8rem' }}>
                          {t.league}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {t.active_players}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {t.total_minutes?.toLocaleString()}
                        </td>
                        <td style={{ textAlign: 'right', color: '#38bdf8', fontWeight: 700 }} className="mono-number">
                          {t.passes_attempted_per90?.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {(t.pass_accuracy * 100)?.toFixed(1)}%
                        </td>
                        <td style={{ textAlign: 'right', color: '#fb7185', fontWeight: 600 }} className="mono-number">
                          {t.pressures_attempted_per90?.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {t.headers_attempted_per90?.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {t.clearances_per90?.toFixed(2)}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {t.crosses_attempted_per90?.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {t.blocks_per90?.toFixed(2)}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ========================================================================= */}
      {/* TAB 2: TACTICAL SYSTEM OUTLIERS FINDER                                    */}
      {/* ========================================================================= */}
      {activeTab === 'outliers' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.75rem' }}>
          {/* Heuristic Explainer Card */}
          <div
            className="glass-card"
            style={{
              padding: '1.25rem 1.5rem',
              borderLeft: '4px solid #38bdf8',
              background: 'rgba(56, 189, 248, 0.05)',
            }}
          >
            <div style={{ fontWeight: 700, color: '#38bdf8', fontSize: '0.9rem', marginBottom: '4px' }}>
              Directional Heuristics & Anomaly Detection
            </div>
            <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem', lineHeight: '1.5' }}>
              These heuristics map tactical system expectations against individual performance profiles. Players flagged below operate with high-value technical or physical traits that diverge significantly from their team’s structural constraints—such as a striker maintaining elite passing precision in an aerial Route One setup, or a defender forced into emergency clearances in a Tiki-Taka side.
            </p>
          </div>

          {/* Filter Pills */}
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', alignItems: 'center' }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-dim)', fontWeight: 600, marginRight: '0.25rem' }}>
              Outlier Type:
            </span>
            <button
              onClick={() => handleTagChange('All')}
              className="badge"
              style={{
                cursor: 'pointer',
                padding: '0.35rem 0.75rem',
                fontSize: '0.78rem',
                background: selectedTag === 'All' ? '#0284c7' : 'rgba(255,255,255,0.06)',
                color: selectedTag === 'All' ? '#fff' : 'var(--text-dim)',
                border: selectedTag === 'All' ? '1px solid #38bdf8' : '1px solid var(--border-subtle)',
              }}
            >
              All Outliers ({tags.reduce((acc, t) => acc + t.count, 0)})
            </button>
            {tags.map((t) => (
              <button
                key={t.outlier_tag}
                onClick={() => handleTagChange(t.outlier_tag)}
                className="badge"
                style={{
                  cursor: 'pointer',
                  padding: '0.35rem 0.75rem',
                  fontSize: '0.78rem',
                  background: selectedTag === t.outlier_tag ? '#0284c7' : 'rgba(255,255,255,0.06)',
                  color: selectedTag === t.outlier_tag ? '#fff' : 'var(--text-dim)',
                  border: selectedTag === t.outlier_tag ? '1px solid #38bdf8' : '1px solid var(--border-subtle)',
                }}
              >
                {t.outlier_tag} ({t.count})
              </button>
            ))}
          </div>

          {/* Outliers Table */}
          <div className="glass-card" style={{ padding: '1.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.25rem', flexWrap: 'wrap', gap: '1rem' }}>
              <div>
                <h2 style={{ fontSize: '1.35rem', fontWeight: 800, color: '#fff' }}>
                  {selectedTag === 'All' ? 'Flagged System Outliers' : selectedTag}
                </h2>
                <p style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
                  Players with statistically verified divergence from team tactical frameworks
                </p>
              </div>
              <span className="mono-number" style={{ fontSize: '0.85rem', color: '#38bdf8' }}>
                Showing {outliers.length} flagged players
              </span>
            </div>

            <div className="fbref-table-container">
              <table className="fbref-table">
                <thead>
                  <tr>
                    <th>Player</th>
                    <th>Pos</th>
                    <th>Club & System</th>
                    <th>Outlier Classification</th>
                    <th style={{ textAlign: 'left' }}>System Divergence Analysis</th>
                    <th style={{ textAlign: 'right' }}>Mins</th>
                    <th style={{ textAlign: 'right' }}>Value</th>
                    <th style={{ textAlign: 'center', width: '90px' }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {loadingOutliers ? (
                    <tr>
                      <td colSpan={8} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                        Querying database for tactical anomalies...
                      </td>
                    </tr>
                  ) : outliers.length === 0 ? (
                    <tr>
                      <td colSpan={8} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                        No outliers found for this filter.
                      </td>
                    </tr>
                  ) : (
                    outliers.map((o) => (
                      <tr key={o.player_key}>
                        <td>
                          <Link
                            href={`/player/${o.player_key}`}
                            style={{ fontWeight: 700, color: '#fff' }}
                          >
                            {o.player_name}
                          </Link>
                          {o.is_own_squad && (
                            <span className="badge badge-success" style={{ marginLeft: '6px', fontSize: '0.65rem' }}>
                              OWN SQUAD
                            </span>
                          )}
                        </td>
                        <td>
                          <span className="badge badge-position">{o.position}</span>
                        </td>
                        <td>
                          <div style={{ color: '#e2e8f0', fontWeight: 600 }}>{o.club_name}</div>
                          <div style={{ fontSize: '0.72rem', color: '#38bdf8', fontWeight: 600 }}>
                            {o.team_tactical_archetype}
                          </div>
                        </td>
                        <td>
                          <span
                            style={{
                              display: 'inline-block',
                              padding: '0.2rem 0.55rem',
                              borderRadius: '4px',
                              fontSize: '0.75rem',
                              fontWeight: 700,
                              background: o.outlier_tag.includes('Striker')
                                ? 'rgba(52, 211, 153, 0.15)'
                                : o.outlier_tag.includes('Defender')
                                ? 'rgba(244, 63, 94, 0.15)'
                                : o.outlier_tag.includes('Talisman')
                                ? 'rgba(251, 191, 36, 0.15)'
                                : 'rgba(56, 189, 248, 0.15)',
                              color: o.outlier_tag.includes('Striker')
                                ? '#34d399'
                                : o.outlier_tag.includes('Defender')
                                ? '#fb7185'
                                : o.outlier_tag.includes('Talisman')
                                ? '#fbbf24'
                                : '#38bdf8',
                              border: '1px solid currentColor',
                            }}
                          >
                            {o.outlier_tag}
                          </span>
                        </td>
                        <td style={{ fontSize: '0.8rem', color: 'var(--text-muted)', maxWidth: '340px' }}>
                          <div>{o.outlier_rationale}</div>
                          <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '2px' }} className="mono-number">
                            {o.outlier_tag.includes('Striker') && (
                              <span>Player Acc: {(o.player_pass_accuracy * 100).toFixed(1)}% vs Team Acc: {(o.team_pass_accuracy * 100).toFixed(1)}%</span>
                            )}
                            {o.outlier_tag.includes('Defender') && (
                              <span>Player Clr: {o.player_clearances_per90?.toFixed(2)}/90 vs Team Clr: {o.team_clearances_per90?.toFixed(2)}/90</span>
                            )}
                            {o.outlier_tag.includes('Talisman') && (
                              <span>Player NP-xG+xA: {o.player_npxg_plus_xa_per90?.toFixed(2)}/90 | KP: {o.player_open_play_key_passes_per90?.toFixed(2)}/90</span>
                            )}
                            {o.outlier_tag.includes('Carrier') && (
                              <span>Player Dribbles: {o.player_dribbles_made_per90?.toFixed(2)}/90 | ProgP: {o.player_progressive_passes_per90?.toFixed(2)}/90</span>
                            )}
                            {o.outlier_tag.includes('Pressing') && (
                              <span>Player Press: {o.player_pressures_attempted_per90?.toFixed(1)}/90 vs Team Press: {o.team_pressures_attempted_per90?.toFixed(1)}/90</span>
                            )}
                          </div>
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {o.minutes_played?.toLocaleString()}
                        </td>
                        <td style={{ textAlign: 'right', color: '#fbbf24' }} className="mono-number">
                          {o.transfer_value_m ? `£${o.transfer_value_m}M` : '-'}
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          <Link
                            href={`/player/${o.player_key}`}
                            className="btn-primary"
                            style={{ padding: '0.3rem 0.6rem', fontSize: '0.75rem' }}
                          >
                            Dossier
                          </Link>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ========================================================================= */}
      {/* TAB 3: INDIVIDUAL PLAYER ROLES (22)                                       */}
      {/* ========================================================================= */}
      {activeTab === 'roles' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          {/* Archetype Selector Grid */}
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(210px, 1fr))',
              gap: '0.75rem',
            }}
          >
            {loadingRoles ? (
              <div style={{ color: 'var(--text-dim)', padding: '1rem' }}>Loading tactical roles...</div>
            ) : (
              archetypes.map((role) => {
                const isSelected = selectedRole === role.tactical_archetype;
                return (
                  <button
                    key={role.tactical_archetype}
                    onClick={() => setSelectedRole(role.tactical_archetype)}
                    className="glass-card"
                    style={{
                      padding: '1rem',
                      textAlign: 'left',
                      cursor: 'pointer',
                      border: isSelected ? '1px solid #06b6d4' : '1px solid var(--border-subtle)',
                      background: isSelected ? 'rgba(6, 182, 212, 0.15)' : 'var(--bg-card)',
                      transition: 'all 0.2s ease',
                    }}
                  >
                    <div style={{ fontWeight: 700, fontSize: '0.88rem', color: isSelected ? '#38bdf8' : '#fff' }}>
                      {role.tactical_archetype}
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '0.5rem' }}>
                      <span className="mono-number" style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                        {role.player_count} players
                      </span>
                      <span className="mono-number" style={{ fontSize: '0.75rem', color: '#fbbf24' }}>
                        avg £{role.avg_value}M
                      </span>
                    </div>
                  </button>
                );
              })
            )}
          </div>

          {/* Selected Archetype Roster Table */}
          <div className="glass-card" style={{ padding: '1.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.25rem', flexWrap: 'wrap', gap: '1rem' }}>
              <div>
                <h2 style={{ fontSize: '1.35rem', fontWeight: 800, color: '#fff' }}>
                  Top Performers: {selectedRole}
                </h2>
                <p style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
                  Ranked by combined non-penalty xG + expected assists per 90
                </p>
              </div>
              <span className="mono-number" style={{ fontSize: '0.85rem', color: '#38bdf8' }}>
                Showing top {players.length} qualified players
              </span>
            </div>

            <div className="fbref-table-container">
              <table className="fbref-table">
                <thead>
                  <tr>
                    <th>Player</th>
                    <th>Position</th>
                    <th>Club & League</th>
                    <th style={{ textAlign: 'right' }}>Age</th>
                    <th style={{ textAlign: 'right' }}>Mins</th>
                    <th style={{ textAlign: 'right' }}>Value</th>
                    <th style={{ textAlign: 'right' }}>Wage</th>
                    <th style={{ textAlign: 'right', color: '#fbbf24' }}>NP-xG+xA/90</th>
                    <th style={{ textAlign: 'right' }}>Prog. Passes</th>
                    <th style={{ textAlign: 'right' }}>Def. Actions</th>
                    <th style={{ textAlign: 'center', width: '100px' }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {loadingPlayers ? (
                    <tr>
                      <td colSpan={11} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                        Querying warehouse for archetype roster...
                      </td>
                    </tr>
                  ) : players.length === 0 ? (
                    <tr>
                      <td colSpan={11} style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-dim)' }}>
                        No players found for this role.
                      </td>
                    </tr>
                  ) : (
                    players.map((p) => (
                      <tr key={p.player_key}>
                        <td>
                          <Link
                            href={`/player/${p.player_key}`}
                            style={{ fontWeight: 600, color: '#fff' }}
                          >
                            {p.player_name}
                          </Link>
                        </td>
                        <td>
                          <span className="badge badge-position">{p.position}</span>
                        </td>
                        <td>
                          <div style={{ color: '#e2e8f0' }}>{p.club_name}</div>
                          <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)' }}>{p.league}</div>
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {p.age}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {p.minutes_played?.toLocaleString()}
                        </td>
                        <td style={{ textAlign: 'right', color: '#38bdf8' }} className="mono-number">
                          {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {p.wage_per_week_k ? `£${p.wage_per_week_k}k` : '-'}
                        </td>
                        <td
                          style={{
                            textAlign: 'right',
                            fontWeight: 700,
                            color: (p.npxg_plus_xa_per90 || 0) >= 0.4 ? '#34d399' : '#fbbf24',
                          }}
                          className="mono-number"
                        >
                          {p.npxg_plus_xa_per90?.toFixed(2) || '0.00'}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {p.progressive_passes_per90?.toFixed(2) || '0.00'}
                        </td>
                        <td style={{ textAlign: 'right' }} className="mono-number">
                          {p.defensive_actions_per90?.toFixed(2) || '0.00'}
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          <Link
                            href={`/player/${p.player_key}`}
                            className="btn-primary"
                            style={{ padding: '0.3rem 0.6rem', fontSize: '0.75rem' }}
                          >
                            Dossier
                          </Link>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
