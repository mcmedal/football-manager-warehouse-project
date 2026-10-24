'use client';

import React, { useState, useEffect, useRef } from 'react';
import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { 
  Search, 
  BarChart3, 
  Users, 
  Crosshair, 
  SlidersHorizontal, 
  ScatterChart, 
  UserPlus, 
  Activity, 
  Star 
} from 'lucide-react';
import { useShortlist } from '@/lib/shortlist';

interface SearchResult {
  player_key: number;
  player_name: string;
  position: string;
  club_name: string;
  league: string;
  transfer_value_m: number;
  is_own_squad: number;
}

export default function Navbar() {
  const pathname = usePathname();
  const router = useRouter();
  const { shortlist } = useShortlist();
  const [searchTerm, setSearchTerm] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleOutsideClick = (e: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleOutsideClick);
    return () => document.removeEventListener('mousedown', handleOutsideClick);
  }, []);

  useEffect(() => {
    if (searchTerm.trim().length < 2) {
      setResults([]);
      setIsOpen(false);
      return;
    }

    const timer = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await fetch(`/api/search?q=${encodeURIComponent(searchTerm.trim())}`);
        const data = await res.json();
        setResults(data.results || []);
        setIsOpen(true);
      } catch (err) {
        console.error('Search error:', err);
      } finally {
        setLoading(false);
      }
    }, 250);

    return () => clearTimeout(timer);
  }, [searchTerm]);

  const handleSelectPlayer = (key: number) => {
    setIsOpen(false);
    setSearchTerm('');
    router.push(`/player/${key}`);
  };

  return (
    <nav className="navbar" id="main-navigation">
      <div className="nav-inner">
        <div style={{ display: 'flex', alignItems: 'center', gap: '1.5rem' }}>
          <Link href="/" className="nav-brand">
            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  width: '32px',
                  height: '32px',
                  borderRadius: '8px',
                  background: 'linear-gradient(135deg, #0ea5e9, #3b82f6)',
                  color: '#fff',
                }}
              >
                ⚽
              </span>
              <span>FM Analytics</span>
            </span>
            <span className="nav-brand-badge">Opta / FBref DW</span>
          </Link>

          <div className="nav-links">
            <Link
              href="/"
              className={`nav-link ${pathname === '/' ? 'active' : ''}`}
              id="nav-scouting-hub"
            >
              <BarChart3 size={15} />
              <span>Hub</span>
            </Link>
            <Link
              href="/screener"
              className={`nav-link ${pathname.startsWith('/screener') ? 'active' : ''}`}
              id="nav-player-screener"
            >
              <SlidersHorizontal size={15} />
              <span>Screener</span>
            </Link>
            <Link
              href="/scatter"
              className={`nav-link ${pathname.startsWith('/scatter') ? 'active' : ''}`}
              id="nav-scatter-explorer"
            >
              <ScatterChart size={15} />
              <span>Scatter</span>
            </Link>
            <Link
              href="/succession"
              className={`nav-link ${pathname.startsWith('/succession') ? 'active' : ''}`}
              id="nav-succession-engine"
            >
              <UserPlus size={15} />
              <span>Succession</span>
            </Link>
            <Link
              href="/squad-diagnosis"
              className={`nav-link ${pathname.startsWith('/squad-diagnosis') ? 'active' : ''}`}
              id="nav-squad-diagnosis"
            >
              <Activity size={15} />
              <span>Diagnosis</span>
            </Link>
            <Link
              href="/compare"
              className={`nav-link ${pathname.startsWith('/compare') ? 'active' : ''}`}
              id="nav-head-to-head"
            >
              <Users size={15} />
              <span>H2H</span>
            </Link>
            <Link
              href="/shortlist"
              className={`nav-link ${pathname.startsWith('/shortlist') ? 'active' : ''}`}
              id="nav-scouting-shortlist"
            >
              <Star size={15} fill={shortlist.length > 0 ? '#f59e0b' : 'none'} color={shortlist.length > 0 ? '#f59e0b' : 'currentColor'} />
              <span>Shortlist</span>
              {shortlist.length > 0 && (
                <span
                  style={{
                    marginLeft: '3px',
                    padding: '1px 6px',
                    borderRadius: '10px',
                    background: '#f59e0b',
                    color: '#000',
                    fontSize: '0.7rem',
                    fontWeight: 700,
                  }}
                >
                  {shortlist.length}
                </span>
              )}
            </Link>
          </div>
        </div>

        {/* Global Live Search */}
        <div className="nav-search-wrapper" ref={searchRef}>
          <Search size={16} className="nav-search-icon" />
          <input
            type="text"
            className="nav-search-input"
            placeholder="Search 11,000+ players or clubs..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            onFocus={() => {
              if (results.length > 0) setIsOpen(true);
            }}
            id="global-search-input"
          />

          {isOpen && (
            <div className="search-results-popover" id="search-results-dropdown">
              {loading ? (
                <div style={{ padding: '1rem', textAlign: 'center', color: 'var(--text-dim)', fontSize: '0.85rem' }}>
                  Scanning warehouse...
                </div>
              ) : results.length === 0 ? (
                <div style={{ padding: '1rem', textAlign: 'center', color: 'var(--text-dim)', fontSize: '0.85rem' }}>
                  No players found for &quot;{searchTerm}&quot;
                </div>
              ) : (
                results.map((p) => (
                  <div
                    key={p.player_key}
                    className="search-result-item"
                    onClick={() => handleSelectPlayer(p.player_key)}
                    style={{ cursor: 'pointer' }}
                  >
                    <div>
                      <div style={{ fontWeight: 600, fontSize: '0.9rem', color: '#fff' }}>
                        {p.player_name}
                      </div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                        {p.club_name} • {p.position}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                      <div className="mono-number" style={{ fontSize: '0.85rem', color: '#38bdf8' }}>
                        {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                      </div>
                      {p.is_own_squad === 1 && (
                        <span style={{ fontSize: '0.65rem', color: '#34d399', fontWeight: 600 }}>
                          OWN SQUAD
                        </span>
                      )}
                    </div>
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      </div>
    </nav>
  );
}
