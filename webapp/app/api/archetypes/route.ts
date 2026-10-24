import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const viewType = searchParams.get('type') || 'roles'; // 'roles' | 'teams' | 'outliers'
    const archetype = searchParams.get('archetype');
    const framework = searchParams.get('framework');
    const outlierTag = searchParams.get('outlierTag');

    // -------------------------------------------------------------
    // 1. TEAM TACTICAL ARCHETYPES
    // -------------------------------------------------------------
    if (viewType === 'teams') {
      if (!archetype && !framework) {
        // Summary counts and averages across all tactical archetypes
        const summary = await executeQuery<{
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
        }>(
          `
          SELECT 
            tactical_archetype,
            tactical_framework,
            COUNT(*) AS team_count,
            ROUND(AVG(passes_attempted_per90), 1) AS avg_passes_per90,
            ROUND(AVG(pass_accuracy) * 100, 1) AS avg_pass_accuracy,
            ROUND(AVG(pressures_attempted_per90), 1) AS avg_pressures_per90,
            ROUND(AVG(headers_attempted_per90), 1) AS avg_headers_per90,
            ROUND(AVG(clearances_per90), 2) AS avg_clearances_per90,
            ROUND(AVG(crosses_attempted_per90), 1) AS avg_crosses_per90,
            ROUND(AVG(blocks_per90), 2) AS avg_blocks_per90
          FROM gold.vw_team_tactical_archetypes
          GROUP BY tactical_archetype, tactical_framework
          ORDER BY team_count DESC
          `
        );
        return NextResponse.json({ summary });
      }

      // Specific archetype or framework team roster
      const params: Record<string, string> = {};
      let whereClause = '1=1';

      if (archetype) {
        whereClause += ' AND tactical_archetype = @archetype';
        params.archetype = archetype;
      }
      if (framework) {
        whereClause += ' AND tactical_framework = @framework';
        params.framework = framework;
      }

      const teams = await executeQuery(
        `
        SELECT TOP 100
          team_key,
          club_name,
          league,
          active_players,
          total_minutes,
          tactical_archetype,
          tactical_framework,
          tactical_description,
          passes_attempted_per90,
          pass_accuracy,
          pressures_attempted_per90,
          progressive_passes_per90,
          headers_attempted_per90,
          clearances_per90,
          crosses_attempted_per90,
          blocks_per90,
          dribbles_made_per90,
          sprints_per90,
          gegenpressing_score,
          tiki_taka_score,
          vertical_tiki_taka_score,
          fluid_counter_score,
          direct_counter_score,
          wing_play_score,
          route_one_score,
          catenaccio_score
        FROM gold.vw_team_tactical_archetypes
        WHERE ${whereClause}
        ORDER BY total_minutes DESC
        `,
        params
      );

      return NextResponse.json({ teams });
    }

    // -------------------------------------------------------------
    // 2. PLAYER TACTICAL SYSTEM OUTLIERS
    // -------------------------------------------------------------
    if (viewType === 'outliers') {
      const params: Record<string, string> = {};
      let whereClause = 'outlier_tag IS NOT NULL';

      if (outlierTag && outlierTag !== 'All') {
        whereClause += ' AND outlier_tag = @outlierTag';
        params.outlierTag = outlierTag;
      }

      const outliers = await executeQuery(
        `
        SELECT TOP 80
          player_key,
          player_name,
          age,
          position,
          club_name,
          league,
          is_own_squad,
          minutes_played,
          transfer_value_m,
          wage_per_week_k,
          team_tactical_archetype,
          team_tactical_framework,
          outlier_tag,
          outlier_rationale,
          player_pass_accuracy,
          player_passes_attempted_per90,
          player_clearances_per90,
          player_npxg_plus_xa_per90,
          player_chances_created_per90,
          player_open_play_key_passes_per90,
          player_progressive_passes_per90,
          player_dribbles_made_per90,
          player_pressures_attempted_per90,
          player_turnover_rate,
          team_passes_attempted_per90,
          team_pass_accuracy,
          team_clearances_per90,
          team_pressures_attempted_per90
        FROM gold.vw_player_system_outliers
        WHERE ${whereClause}
        ORDER BY 
          CASE WHEN outlier_tag LIKE '%Striker%' THEN player_pass_accuracy 
               WHEN outlier_tag LIKE '%Defender%' THEN player_clearances_per90
               WHEN outlier_tag LIKE '%Talisman%' THEN player_npxg_plus_xa_per90
               ELSE player_progressive_passes_per90 END DESC
        `,
        params
      );

      // Distinct outlier tags for filter badges
      const tags = await executeQuery<{ outlier_tag: string; count: number }>(
        `
        SELECT outlier_tag, COUNT(*) AS count
        FROM gold.vw_player_system_outliers
        WHERE outlier_tag IS NOT NULL
        GROUP BY outlier_tag
        ORDER BY count DESC
        `
      );

      return NextResponse.json({ outliers, tags });
    }

    // -------------------------------------------------------------
    // 3. INDIVIDUAL PLAYER TACTICAL ROLES (Default / Backward Compatible)
    // -------------------------------------------------------------
    if (!archetype) {
      const summary = await executeQuery<{
        tactical_archetype: string;
        player_count: number;
        avg_value: number;
        avg_npxg_xa: number;
      }>(
        `
        SELECT 
          tactical_archetype,
          COUNT(*) AS player_count,
          ROUND(AVG(transfer_value_m), 1) AS avg_value,
          ROUND(AVG(npxg_plus_xa_per90), 2) AS avg_npxg_xa
        FROM gold.vw_tactical_archetypes
        GROUP BY tactical_archetype
        ORDER BY player_count DESC
        `
      );
      return NextResponse.json({ archetypes: summary });
    }

    // Return players for a specific player archetype
    const players = await executeQuery(
      `
      SELECT TOP 50
        player_key,
        player_name,
        age,
        position,
        club_name,
        league,
        minutes_played,
        transfer_value_m,
        wage_per_week_k,
        tactical_archetype,
        npxg_plus_xa_per90,
        progressive_passes_per90,
        defensive_actions_per90,
        cost_per_xgi
      FROM gold.vw_tactical_archetypes
      WHERE tactical_archetype = @archetype
      ORDER BY npxg_plus_xa_per90 DESC
      `,
      { archetype }
    );

    return NextResponse.json({ players });
  } catch (error) {
    console.error('Archetypes API error:', error);
    return NextResponse.json({ error: 'Failed to fetch archetypes' }, { status: 500 });
  }
}
