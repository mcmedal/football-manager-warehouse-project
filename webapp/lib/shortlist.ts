'use client';

import { useState, useEffect } from 'react';

export interface ShortlistedPlayer {
  player_key: number;
  player_name: string;
  position: string;
  club_name: string;
  league: string;
  age: number;
  minutes_played: number;
  transfer_value_m?: number;
  wage_per_week_k?: number;
  primary_stat?: string;
  primary_value?: string | number;
  notes?: string;
  tags?: string[];
  added_at: string;
}

const STORAGE_KEY = 'fm_analytics_shortlist_v1';
const EVENT_NAME = 'fm_shortlist_updated';

export function getShortlist(): ShortlistedPlayer[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch (e) {
    console.error('Failed to read shortlist from localStorage', e);
    return [];
  }
}

export function addToShortlist(player: ShortlistedPlayer): void {
  if (typeof window === 'undefined') return;
  try {
    const list = getShortlist();
    if (!list.some((p) => p.player_key === player.player_key)) {
      list.push(player);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
      window.dispatchEvent(new Event(EVENT_NAME));
    }
  } catch (e) {
    console.error('Failed to add player to shortlist', e);
  }
}

export function removeFromShortlist(player_key: number): void {
  if (typeof window === 'undefined') return;
  try {
    const list = getShortlist().filter((p) => p.player_key !== player_key);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
    window.dispatchEvent(new Event(EVENT_NAME));
  } catch (e) {
    console.error('Failed to remove player from shortlist', e);
  }
}

export function toggleShortlist(player: ShortlistedPlayer): boolean {
  const current = isShortlisted(player.player_key);
  if (current) {
    removeFromShortlist(player.player_key);
    return false;
  } else {
    addToShortlist(player);
    return true;
  }
}

export function isShortlisted(player_key: number): boolean {
  if (typeof window === 'undefined') return false;
  return getShortlist().some((p) => p.player_key === player_key);
}

export function useShortlist() {
  const [shortlist, setShortlist] = useState<ShortlistedPlayer[]>([]);

  useEffect(() => {
    setShortlist(getShortlist());

    const handleUpdate = () => {
      setShortlist(getShortlist());
    };

    window.addEventListener(EVENT_NAME, handleUpdate);
    window.addEventListener('storage', handleUpdate);
    return () => {
      window.removeEventListener(EVENT_NAME, handleUpdate);
      window.removeEventListener('storage', handleUpdate);
    };
  }, []);

  return {
    shortlist,
    isShortlisted: (key: number) => shortlist.some((p) => p.player_key === key),
    add: addToShortlist,
    remove: removeFromShortlist,
    toggle: toggleShortlist,
  };
}

export function exportShortlistCSV(): void {
  const list = getShortlist();
  if (list.length === 0) return;

  const headers = ['Player Key', 'Player Name', 'Position', 'Club', 'League', 'Age', 'Minutes', 'Transfer Value (£M)', 'Wage (£k/wk)', 'Notes'];
  const rows = list.map((p) => [
    p.player_key,
    `"${p.player_name.replace(/"/g, '""')}"`,
    `"${p.position}"`,
    `"${p.club_name.replace(/"/g, '""')}"`,
    `"${p.league.replace(/"/g, '""')}"`,
    p.age,
    p.minutes_played,
    p.transfer_value_m ?? '',
    p.wage_per_week_k ?? '',
    `"${(p.notes || '').replace(/"/g, '""')}"`,
  ]);

  const csvContent = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n');
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.setAttribute('href', url);
  link.setAttribute('download', `FM_Scouting_Shortlist_${new Date().toISOString().slice(0, 10)}.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export async function copyPlayerKeysToClipboard(): Promise<number> {
  const list = getShortlist();
  if (list.length === 0) return 0;
  const ids = list.map((p) => p.player_key).join(', ');
  await navigator.clipboard.writeText(ids);
  return list.length;
}
