import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);

    const search = searchParams.get('search') || '';
    const position = searchParams.get('position') || 'ALL';
    const league = searchParams.get('league') || 'ALL';
    const minMinutes = parseInt(searchParams.get('minMinutes') || '450', 10);
    const maxAge = parseInt(searchParams.get('maxAge') || '99', 10);
    const minAge = parseInt(searchParams.get('minAge') || '15', 10);
    const maxValue = parseFloat(searchParams.get('maxValue') || '999');
    const isOwnSquad = searchParams.get('isOwnSquad'); // '1', '0', or null

    const sortByParam = searchParams.get('sortBy') || 'npxg_plus_xa_per90';
    const sortOrderParam = (searchParams.get('sortOrder') || 'DESC').toUpperCase();
    const sortOrder = sortOrderParam === 'ASC' ? 'ASC' : 'DESC';

    const page = Math.max(1, parseInt(searchParams.get('page') || '1', 10));
    const pageSize = Math.min(100, Math.max(10, parseInt(searchParams.get('pageSize') || '25', 10)));
    const offset = (page - 1) * pageSize;

    // Allowed sort columns to prevent SQL injection
    const allowedSortColumns: Record<string, string> = {
      player_name: 's.player_name',
      age: 's.age',
      minutes_played: 's.minutes_played',
      transfer_value_m: 's.transfer_value_m',
      wage_per_week_k: 's.wage_per_week_k',
      npxg_plus_xa_per90: 's.npxg_plus_xa_per90',
      non_penalty_xGoals_per90: 's.non_penalty_xGoals_per90',
      xAssits_per90: 's.xAssits_per90',
      progressive_passes_per90: 's.progressive_passes_per90',
      defensive_actions_per90: 's.defensive_actions_per90',
      cost_per_xgi: 's.cost_per_xgi',
    };

    const sortColumn = allowedSortColumns[sortByParam] || 's.npxg_plus_xa_per90';

    const isGoalkeeperQuery = position === 'Goalkeeper';
    const sourceTable = isGoalkeeperQuery ? 'gold.mart_gk_scouting_summary' : 'gold.mart_outfield_scouting_summary';

    const conditions: string[] = ['s.minutes_played >= @minMinutes', 's.age >= @minAge', 's.age <= @maxAge'];
    const params: Record<string, unknown> = {
      minMinutes,
      minAge,
      maxAge,
      maxValue,
      offset,
      pageSize,
    };

    if (maxValue < 999) {
      conditions.push('(s.transfer_value_m <= @maxValue OR s.transfer_value_m IS NULL)');
    }

    if (search.trim()) {
      conditions.push('(s.player_name LIKE @search OR s.club_name LIKE @search)');
      params.search = `%${search.trim()}%`;
    }

    if (position !== 'ALL' && !isGoalkeeperQuery) {
      conditions.push('s.position = @position');
      params.position = position;
    }

    if (league !== 'ALL') {
      conditions.push('s.league = @league');
      params.league = league;
    }

    if (isOwnSquad === '1' || isOwnSquad === '0') {
      conditions.push('s.is_own_squad = @isOwnSquad');
      params.isOwnSquad = parseInt(isOwnSquad, 10);
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    // Count query
    const countQuery = `
      SELECT COUNT(*) AS total
      FROM ${sourceTable} s
      ${whereClause}
    `;

    // Data query
    const dataQuery = isGoalkeeperQuery
      ? `
      SELECT
        s.player_key,
        s.player_name,
        s.age,
        s.position,
        s.club_name,
        s.league,
        s.is_own_squad,
        s.minutes_played,
        s.wage_per_week_k,
        s.transfer_value_m,
        s.saves_made_per90 AS goals_per90,
        s.saves_percentage AS assists_per90,
        s.xGoals_prevented_per90 AS non_penalty_xGoals_per90,
        s.shot_stopping_delta AS xAssits_per90,
        s.xGoals_prevented_per90 AS npxg_plus_xa_per90,
        s.sweeper_index AS progressive_passes_per90,
        s.pass_accuracy,
        s.hold_rate_pct AS defensive_actions_per90,
        NULL AS cost_per_xgi
      FROM gold.mart_gk_scouting_summary s
      ${whereClause}
      ORDER BY ${sortByParam === 'npxg_plus_xa_per90' ? 's.xGoals_prevented_per90' : sortColumn} ${sortOrder}
      OFFSET @offset ROWS
      FETCH NEXT @pageSize ROWS ONLY
      `
      : `
      SELECT
        s.player_key,
        s.player_name,
        s.age,
        s.position,
        s.club_name,
        s.league,
        s.is_own_squad,
        s.minutes_played,
        s.wage_per_week_k,
        s.transfer_value_m,
        s.goals_per90,
        s.assists_per90,
        s.non_penalty_xGoals_per90,
        s.xAssits_per90,
        s.npxg_plus_xa_per90,
        s.progressive_passes_per90,
        s.pass_accuracy,
        s.defensive_actions_per90,
        s.cost_per_xgi
      FROM gold.mart_outfield_scouting_summary s
      ${whereClause}
      ORDER BY ${sortColumn} ${sortOrder}
      OFFSET @offset ROWS
      FETCH NEXT @pageSize ROWS ONLY
    `;

    const [countRows, dataRows] = await Promise.all([
      executeQuery<{ total: number }>(countQuery, params),
      executeQuery(dataQuery, params),
    ]);

    const total = countRows[0]?.total || 0;
    const totalPages = Math.ceil(total / pageSize);

    return NextResponse.json({
      players: dataRows,
      pagination: {
        page,
        pageSize,
        total,
        totalPages,
      },
    });
  } catch (error) {
    console.error('Players API error:', error);
    return NextResponse.json({ error: 'Failed to fetch players' }, { status: 500 });
  }
}
