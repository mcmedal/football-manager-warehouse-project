import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get('q') || '';

    if (!query || query.trim().length < 2) {
      return NextResponse.json({ results: [] });
    }

    const cleanQuery = `%${query.trim()}%`;
    const rows = await executeQuery<{
      player_key: number;
      player_name: string;
      position: string;
      club_name: string;
      league: string;
      transfer_value_m: number;
      is_own_squad: number;
    }>(
      `
      SELECT TOP 8
        p.player_key,
        p.player_name,
        p.position,
        t.club_name,
        t.league,
        v.transfer_value_m,
        p.is_own_squad
      FROM gold.dim_player_info p
      INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key
      LEFT JOIN gold.fact_players_value v ON p.player_key = v.player_key
      WHERE p.player_name LIKE @q OR t.club_name LIKE @q
      ORDER BY 
        CASE WHEN p.player_name LIKE @exact THEN 0 ELSE 1 END,
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
