import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

export interface SearchPlayerResult {
  player_key: number;
  player_name: string;
  position: string;
  club_name: string;
  league: string;
  transfer_value_m: number;
  is_own_squad: number;
  season: string;
  is_current_season: number;
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get('q') || '';
    const seasonFilter = searchParams.get('season') || 'all'; // 'current' | 'all'

    if (!query || query.trim().length < 2) {
      return NextResponse.json({ results: [] });
    }

    const cleanQuery = `%${query.trim()}%`;
    const seasonCondition = seasonFilter === 'current' ? 'AND p.dwh_current_validity = 1' : '';

    const rows = await executeQuery<SearchPlayerResult>(
      `
      SELECT TOP 14
        p.player_key,
        p.player_name,
        p.position,
        t.club_name,
        t.league,
        v.transfer_value_m,
        p.is_own_squad,
        p.season,
        CAST(p.dwh_current_validity AS INT) AS is_current_season
      FROM gold.dim_player_info p
      INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key
      OUTER APPLY (
        SELECT TOP 1 transfer_value_m, wage_per_week_k
        FROM gold.fact_players_value v
        WHERE v.player_key = p.player_key
        ORDER BY v.dwh_current_validity DESC, v.dwh_create_date DESC
      ) v
      WHERE (p.player_name LIKE @q OR t.club_name LIKE @q)
        ${seasonCondition}
      ORDER BY 
        CASE WHEN p.player_name LIKE @exact THEN 0 ELSE 1 END,
        p.dwh_current_validity DESC,
        p.season DESC,
        v.transfer_value_m DESC
      `,
      { q: cleanQuery, exact: `${query.trim()}%` }
    );

    return NextResponse.json({ results: rows });
  } catch (error) {
    console.error('Search API error:', error);
    return NextResponse.json({ error: 'Search failed' }, { status: 500 });
  }
}

