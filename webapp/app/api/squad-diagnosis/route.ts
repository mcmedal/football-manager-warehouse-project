import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

interface DepartmentPlayer {
  player_key: number;
  player_name: string;
  position: string;
  age: number;
  minutes_played: number;
  transfer_value_m: number | null;
  wage_per_week_k: number | null;
  value_gap: number | null;
  primary_stat: number;
  secondary_stat: number;
}

export async function GET(request: NextRequest) {
  try {
    // 1. Fetch own squad goalkeepers & league benchmark
    const ownGkSql = `
      SELECT 
        player_key, player_name, position, age, minutes_played, 
        transfer_value_m, wage_per_week_k, value_gap,
        CAST(xGoals_prevented_per90 AS float) AS primary_stat,
        CAST(saves_percentage AS float) AS secondary_stat,
        CAST(sweeper_actions_per90 AS float) AS tertiary_stat
      FROM gold.mart_gk_scouting_summary
      WHERE is_own_squad = 1
      ORDER BY minutes_played DESC
    `;

    const leagueGkBenchmarkSql = `
      SELECT 
        AVG(CAST(xGoals_prevented_per90 AS float)) as avg_psxg_net,
        AVG(CAST(saves_percentage AS float)) as avg_save_pct,
        AVG(CAST(age AS float)) as avg_age
      FROM gold.mart_gk_scouting_summary
      WHERE league = 'LIGA MX' AND minutes_played >= 450
    `;

    // 2. Fetch own squad outfield players
    const ownOutfieldSql = `
      SELECT 
        player_key, player_name, position, age, minutes_played, 
        transfer_value_m, wage_per_week_k, value_gap,
        CAST(aerial_duels_won_per90 AS float) AS aerial_won_p90,
        CAST(tackle_accuracy AS float) AS tackle_acc,
        CAST(progressive_actions_per90 AS float) AS prog_actions,
        CAST(completed_crosses_per90 AS float) AS crosses_p90,
        CAST(net_ball_security AS float) AS ball_security,
        CAST(progressive_passes_per90 AS float) AS prog_passes,
        CAST(npxg_plus_xa_per90 AS float) AS npxgi,
        CAST(conversion_rate AS float) AS conversion_rate,
        CAST(defensive_actions_per90 AS float) AS def_actions
      FROM gold.mart_outfield_scouting_summary
      WHERE is_own_squad = 1
      ORDER BY minutes_played DESC
    `;

    // 3. Fetch league benchmark for outfield positions
    const leagueOutfieldBenchmarkSql = `
      SELECT 
        CASE 
          WHEN position LIKE '%Central Defender%' THEN 'CB'
          WHEN position LIKE '%Left Defender%' OR position LIKE '%Right Defender%' OR position LIKE '%Wing Back%' THEN 'FB'
          WHEN position LIKE '%Central Midfielder%' OR position LIKE '%Defensive Midfielder%' OR position LIKE '%Right Midfielder%' OR position LIKE '%Left Midfielder%' THEN 'MID'
          ELSE 'ATT'
        END AS dept,
        AVG(CAST(aerial_duels_won_per90 AS float)) AS avg_aerial_won_p90,
        AVG(CAST(tackle_accuracy AS float)) AS avg_tackle_acc,
        AVG(CAST(progressive_actions_per90 AS float)) AS avg_prog_actions,
        AVG(CAST(completed_crosses_per90 AS float)) AS avg_crosses_p90,
        AVG(CAST(net_ball_security AS float)) AS avg_ball_security,
        AVG(CAST(progressive_passes_per90 AS float)) AS avg_prog_passes,
        AVG(CAST(npxg_plus_xa_per90 AS float)) AS avg_npxgi,
        AVG(CAST(conversion_rate AS float)) AS avg_conversion_rate,
        AVG(CAST(age AS float)) AS avg_age
      FROM gold.mart_outfield_scouting_summary
      WHERE league = 'LIGA MX' AND minutes_played >= 450
      GROUP BY 
        CASE 
          WHEN position LIKE '%Central Defender%' THEN 'CB'
          WHEN position LIKE '%Left Defender%' OR position LIKE '%Right Defender%' OR position LIKE '%Wing Back%' THEN 'FB'
          WHEN position LIKE '%Central Midfielder%' OR position LIKE '%Defensive Midfielder%' OR position LIKE '%Right Midfielder%' OR position LIKE '%Left Midfielder%' THEN 'MID'
          ELSE 'ATT'
        END
    `;

    const [ownGk, leagueGk, ownOutfield, leagueOutfield] = await Promise.all([
      executeQuery<any>(ownGkSql),
      executeQuery<any>(leagueGkBenchmarkSql),
      executeQuery<any>(ownOutfieldSql),
      executeQuery<any>(leagueOutfieldBenchmarkSql),
    ]);

    const leagueBenchmarks: Record<string, any> = {};
    leagueOutfield.forEach((row) => {
      leagueBenchmarks[row.dept] = row;
    });

    const gkBenchmark = leagueGk[0] || { avg_psxg_net: 0.0, avg_save_pct: 68.0, avg_age: 26.5 };

    // Group Macara Outfield into departments
    const depts: Record<string, any[]> = {
      CB: [],
      FB: [],
      MID: [],
      ATT: [],
    };

    ownOutfield.forEach((p) => {
      const pos = p.position || '';
      if (pos.includes('Central Defender')) {
        depts.CB.push(p);
      } else if (pos.includes('Left Defender') || pos.includes('Right Defender') || pos.includes('Wing Back')) {
        depts.FB.push(p);
      } else if (pos.includes('Central Midfielder') || pos.includes('Defensive Midfielder') || pos.includes('Right Midfielder') || pos.includes('Left Midfielder')) {
        depts.MID.push(p);
      } else {
        depts.ATT.push(p);
      }
    });

    // Department 1: Goalkeeping
    const starterGk = ownGk[0] || {};
    const gkAvgAge = +(ownGk.reduce((s, p) => s + p.age, 0) / (ownGk.length || 1)).toFixed(1);
    const gkPrimary = +(starterGk.primary_stat || 0).toFixed(2);
    const gkBenchPrimary = +(gkBenchmark.avg_psxg_net || 0).toFixed(2);
    const gkAlerts: string[] = [];
    let gkStatus: 'healthy' | 'warning' | 'critical' = 'healthy';

    if (starterGk.primary_stat > 0.05) {
      gkAlerts.push('🟢 Starter Carlos Troya provides positive PSxG Net (+0.07/90 goals prevented).');
    } else if (starterGk.primary_stat < -0.05) {
      gkAlerts.push('🔴 Shot stopping underperformance: Goalkeeper conceding more than post-shot xG expected.');
      gkStatus = 'critical';
    } else {
      gkAlerts.push('🟡 Average shot stopping: Performing near expected post-shot xG baseline.');
      gkStatus = 'warning';
    }

    if (ownGk.length < 3) {
      gkAlerts.push('🟡 Thin goalkeeper depth (fewer than 3 senior keepers).');
    }

    const gkDepartment = {
      id: 'GK',
      name: 'Goalkeeping',
      icon: '🧤',
      count: ownGk.length,
      avgAge: gkAvgAge,
      status: gkStatus,
      starter: starterGk.player_name || 'N/A',
      starterAge: starterGk.age || 0,
      starterMinutes: starterGk.minutes_played || 0,
      primaryMetric: {
        name: 'PSxG Net / 90',
        value: gkPrimary,
        leagueAvg: gkBenchPrimary,
        unit: 'goals',
        diff: +(gkPrimary - gkBenchPrimary).toFixed(2),
      },
      secondaryMetric: {
        name: 'Save %',
        value: +(starterGk.secondary_stat || 0).toFixed(1),
        leagueAvg: +(gkBenchmark.avg_save_pct || 68.0).toFixed(1),
        unit: '%',
        diff: +((starterGk.secondary_stat || 0) - (gkBenchmark.avg_save_pct || 68.0)).toFixed(1),
      },
      alerts: gkAlerts,
      players: ownGk,
      solutionAction: 'Scout Elite Shot-Stoppers',
      solutionQuery: '/screener?mode=gk&preset=gk_shotstoppers&sortBy=xGoals_prevented_per90&sortOrder=DESC&tab=Shot+Stopping&from=Goalkeeping+Unit',
    };

    // Department 2: Central Defense (CB)
    const cbBench = leagueBenchmarks.CB || { avg_aerial_won_p90: 2.5, avg_tackle_acc: 72.0, avg_age: 26.5 };
    const cbPlayers = depts.CB;
    const cbAvgAge = +(cbPlayers.reduce((s, p) => s + p.age, 0) / (cbPlayers.length || 1)).toFixed(1);
    const cbStarters = cbPlayers.slice(0, 2);
    const cbAerial = +(cbStarters.reduce((s, p) => s + (p.aerial_won_p90 || 0), 0) / (cbStarters.length || 1)).toFixed(2);
    const cbTackle = +(cbStarters.reduce((s, p) => s + (p.tackle_acc || 0), 0) / (cbStarters.length || 1)).toFixed(1);

    const cbAlerts: string[] = [];
    let cbStatus: 'healthy' | 'warning' | 'critical' = 'healthy';

    const agingCb = cbPlayers.find((p) => p.age >= 32);
    if (agingCb) {
      cbAlerts.push(`🔴 Aging Core: ${agingCb.player_name} is ${agingCb.age} years old and entering athletic decline.`);
      cbStatus = 'warning';
    }

    if (cbAerial < (cbBench.avg_aerial_won_p90 - 0.4)) {
      cbAlerts.push(`🔴 Aerial Vulnerability: Starter aerial duels won (${cbAerial}/90) ranks below league benchmark (${cbBench.avg_aerial_won_p90.toFixed(2)}/90).`);
      cbStatus = 'critical';
    } else {
      cbAlerts.push(`🟢 Aerial Stability: Starters winning ${cbAerial} aerial duels/90.`);
    }

    const cbDepartment = {
      id: 'CB',
      name: 'Central Defense',
      icon: '🛡️',
      count: cbPlayers.length,
      avgAge: cbAvgAge,
      status: cbStatus,
      starter: cbStarters.map((p) => p.player_name).join(' & '),
      starterAge: +(cbStarters.reduce((s, p) => s + p.age, 0) / (cbStarters.length || 1)).toFixed(1),
      starterMinutes: cbStarters.reduce((s, p) => s + p.minutes_played, 0),
      primaryMetric: {
        name: 'Aerial Duels Won / 90',
        value: cbAerial,
        leagueAvg: +(cbBench.avg_aerial_won_p90 || 2.5).toFixed(2),
        unit: '/90',
        diff: +(cbAerial - cbBench.avg_aerial_won_p90).toFixed(2),
      },
      secondaryMetric: {
        name: 'Tackle Win %',
        value: cbTackle,
        leagueAvg: +(cbBench.avg_tackle_acc || 72.0).toFixed(1),
        unit: '%',
        diff: +(cbTackle - cbBench.avg_tackle_acc).toFixed(1),
      },
      alerts: cbAlerts,
      players: cbPlayers,
      solutionAction: 'Scout Dominant Aerial CBs',
      solutionQuery: '/screener?mode=outfield&position=Central+Defender&preset=aerial&sortBy=aerial_duels_won_per90&sortOrder=DESC&tab=Defending&from=Central+Defense+(Aerial+Deficit)',
    };

    // Department 3: Fullbacks / Wingbacks (FB)
    const fbBench = leagueBenchmarks.FB || { avg_prog_actions: 5.5, avg_crosses_p90: 0.6, avg_age: 25.0 };
    const fbPlayers = depts.FB;
    const fbAvgAge = +(fbPlayers.reduce((s, p) => s + p.age, 0) / (fbPlayers.length || 1)).toFixed(1);
    const fbStarters = fbPlayers.slice(0, 2);
    const fbProg = +(fbStarters.reduce((s, p) => s + (p.prog_actions || 0), 0) / (fbStarters.length || 1)).toFixed(2);
    const fbCross = +(fbStarters.reduce((s, p) => s + (p.crosses_p90 || 0), 0) / (fbStarters.length || 1)).toFixed(2);

    const fbAlerts: string[] = [];
    let fbStatus: 'healthy' | 'warning' | 'critical' = 'healthy';

    if (fbPlayers.length <= 4) {
      fbAlerts.push('🟡 Roster Warning: Only 4 fullbacks in squad. Zero reserve depth in case of injury suspension.');
      fbStatus = 'warning';
    }

    if (fbProg >= fbBench.avg_prog_actions) {
      fbAlerts.push(`🟢 High Wing Progression: Starters produce ${fbProg} progressive actions/90.`);
    } else {
      fbAlerts.push(`🟡 Flank Transition: Progression (${fbProg}/90) slightly below league benchmark (${fbBench.avg_prog_actions.toFixed(1)}/90).`);
    }

    const fbDepartment = {
      id: 'FB',
      name: 'Fullbacks & Wingbacks',
      icon: '⚡',
      count: fbPlayers.length,
      avgAge: fbAvgAge,
      status: fbStatus,
      starter: fbStarters.map((p) => p.player_name).join(' & '),
      starterAge: +(fbStarters.reduce((s, p) => s + p.age, 0) / (fbStarters.length || 1)).toFixed(1),
      starterMinutes: fbStarters.reduce((s, p) => s + p.minutes_played, 0),
      primaryMetric: {
        name: 'Progressive Actions / 90',
        value: fbProg,
        leagueAvg: +(fbBench.avg_prog_actions || 5.5).toFixed(2),
        unit: '/90',
        diff: +(fbProg - fbBench.avg_prog_actions).toFixed(2),
      },
      secondaryMetric: {
        name: 'Completed Crosses / 90',
        value: fbCross,
        leagueAvg: +(fbBench.avg_crosses_p90 || 0.6).toFixed(2),
        unit: '/90',
        diff: +(fbCross - fbBench.avg_crosses_p90).toFixed(2),
      },
      alerts: fbAlerts,
      players: fbPlayers,
      solutionAction: 'Scout Dynamic Flank Progressors',
      solutionQuery: '/screener?mode=outfield&position=FLANK_DEF&preset=flank_progression&sortBy=progressive_actions_per90&sortOrder=DESC&tab=Defending&from=Fullbacks+&+Wingbacks+(Progression+&+Depth)',
    };

    // Department 4: Midfield (MID)
    const midBench = leagueBenchmarks.MID || { avg_ball_security: 22.0, avg_prog_passes: 4.8, avg_age: 26.0 };
    const midPlayers = depts.MID;
    const midAvgAge = +(midPlayers.reduce((s, p) => s + p.age, 0) / (midPlayers.length || 1)).toFixed(1);
    const midStarters = midPlayers.slice(0, 3);
    const midSecurity = +(midStarters.reduce((s, p) => s + (p.ball_security || 0), 0) / (midStarters.length || 1)).toFixed(1);
    const midProg = +(midStarters.reduce((s, p) => s + (p.prog_passes || 0), 0) / (midStarters.length || 1)).toFixed(2);

    const midAlerts: string[] = [];
    let midStatus: 'healthy' | 'warning' | 'critical' = 'healthy';

    const agingMid = midPlayers.find((p) => p.age >= 31);
    if (agingMid) {
      midAlerts.push(`🔴 Succession Urgent: Engine room starter ${agingMid.player_name} is ${agingMid.age} years old.`);
      midStatus = 'warning';
    }

    if (midSecurity > midBench.avg_ball_security) {
      midAlerts.push(`🟢 High Ball Security: Midfield unit retains possession efficiently (+${(midSecurity - midBench.avg_ball_security).toFixed(1)} net score).`);
    }

    const midDepartment = {
      id: 'MID',
      name: 'Central & Defensive Midfield',
      icon: '🧠',
      count: midPlayers.length,
      avgAge: midAvgAge,
      status: midStatus,
      starter: midStarters.map((p) => p.player_name).join(', '),
      starterAge: +(midStarters.reduce((s, p) => s + p.age, 0) / (midStarters.length || 1)).toFixed(1),
      starterMinutes: midStarters.reduce((s, p) => s + p.minutes_played, 0),
      primaryMetric: {
        name: 'Ball Security Score',
        value: midSecurity,
        leagueAvg: +(midBench.avg_ball_security || 22.0).toFixed(1),
        unit: 'pts',
        diff: +(midSecurity - midBench.avg_ball_security).toFixed(1),
      },
      secondaryMetric: {
        name: 'Progressive Passes / 90',
        value: midProg,
        leagueAvg: +(midBench.avg_prog_passes || 4.8).toFixed(2),
        unit: '/90',
        diff: +(midProg - midBench.avg_prog_passes).toFixed(2),
      },
      alerts: midAlerts,
      players: midPlayers,
      solutionAction: 'Scout Progressive Playmakers',
      solutionQuery: '/screener?mode=outfield&position=Central+Midfielder&preset=playmakers&sortBy=progressive_passes_per90&sortOrder=DESC&tab=Creation&from=Central+Midfield+(Engine+Room+Succession)',
    };

    // Department 5: Attack & Wingers (ATT)
    const attBench = leagueBenchmarks.ATT || { avg_npxgi: 0.38, avg_conversion_rate: 13.5, avg_age: 25.5 };
    const attPlayers = depts.ATT;
    const attAvgAge = +(attPlayers.reduce((s, p) => s + p.age, 0) / (attPlayers.length || 1)).toFixed(1);
    const attStarters = attPlayers.slice(0, 3);
    const attNpxgi = +(attStarters.reduce((s, p) => s + (p.npxgi || 0), 0) / (attStarters.length || 1)).toFixed(2);
    const attConv = +(attStarters.reduce((s, p) => s + (p.conversion_rate || 0), 0) / (attStarters.length || 1)).toFixed(1);

    const attAlerts: string[] = [];
    let attStatus: 'healthy' | 'warning' | 'critical' = 'healthy';

    if (attNpxgi > attBench.avg_npxgi) {
      attAlerts.push(`🟢 High Threat Output: Attackers generating ${attNpxgi} NP-xGI/90 (vs ${attBench.avg_npxgi.toFixed(2)} league avg).`);
    } else {
      attAlerts.push(`🟡 Creativity Deficit: Attack unit generating ${attNpxgi} NP-xGI/90, below league average.`);
      attStatus = 'warning';
    }

    const attDepartment = {
      id: 'ATT',
      name: 'Attacking Unit & Strikers',
      icon: '🎯',
      count: attPlayers.length,
      avgAge: attAvgAge,
      status: attStatus,
      starter: attStarters.map((p) => p.player_name).join(', '),
      starterAge: +(attStarters.reduce((s, p) => s + p.age, 0) / (attStarters.length || 1)).toFixed(1),
      starterMinutes: attStarters.reduce((s, p) => s + p.minutes_played, 0),
      primaryMetric: {
        name: 'NP-xG + xA / 90',
        value: attNpxgi,
        leagueAvg: +(attBench.avg_npxgi || 0.38).toFixed(2),
        unit: '/90',
        diff: +(attNpxgi - attBench.avg_npxgi).toFixed(2),
      },
      secondaryMetric: {
        name: 'Conversion %',
        value: attConv,
        leagueAvg: +(attBench.avg_conversion_rate || 13.5).toFixed(1),
        unit: '%',
        diff: +(attConv - attBench.avg_conversion_rate).toFixed(1),
      },
      alerts: attAlerts,
      players: attPlayers,
      solutionAction: 'Scout Clinical Goalscorers',
      solutionQuery: '/screener?mode=outfield&position=Striker&preset=finishing&sortBy=npxg_plus_xa_per90&sortOrder=DESC&tab=Attacking&from=Attacking+Unit+(Goal+Output)',
    };

    const allDepartments = [gkDepartment, cbDepartment, fbDepartment, midDepartment, attDepartment];

    // Overall squad health score
    const criticalCount = allDepartments.filter((d) => d.status === 'critical').length;
    const warningCount = allDepartments.filter((d) => d.status === 'warning').length;
    const squadHealth = Math.max(50, 100 - criticalCount * 18 - warningCount * 8);

    return NextResponse.json({
      clubName: 'Macará',
      league: 'LIGA MX',
      totalSquadSize: ownGk.length + ownOutfield.length,
      averageSquadAge: +(
        (ownGk.reduce((s, p) => s + p.age, 0) + ownOutfield.reduce((s, p) => s + p.age, 0)) /
        (ownGk.length + ownOutfield.length)
      ).toFixed(1),
      squadHealthScore: squadHealth,
      departments: allDepartments,
    });
  } catch (error) {
    console.error('Squad diagnosis API error:', error);
    return NextResponse.json({ error: 'Failed to compute squad diagnosis' }, { status: 500 });
  }
}
