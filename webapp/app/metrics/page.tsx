'use client';

import React, { useState, useMemo } from 'react';
import Link from 'next/link';
import {
  BarChart3,
  BookOpen,
  Search,
  Sparkles,
  Layers,
  ArrowRight,
  TrendingUp,
  Shield,
  Target,
  Zap,
  SlidersHorizontal,
  ScatterChart,
  Users,
  Compass,
  Coins,
  Activity,
  Calculator,
  Info,
  CheckCircle2,
  ChevronRight,
  HelpCircle,
  Eye,
  Sliders,
  Filter
} from 'lucide-react';

interface MetricDefinition {
  id: string;
  name: string;
  code: string;
  category: 'attacking' | 'passing' | 'defending' | 'goalkeeper' | 'valuation' | 'math';
  formula: string;
  shortDefinition: string;
  tacticalMeaning: string;
  moneyballSignificance: string;
  benchmarks: {
    elite: string;
    good?: string;
    average?: string;
    poor?: string;
  };
  fmSource: string;
  tags: string[];
}

interface GraphDefinition {
  id: string;
  title: string;
  subtitle: string;
  usedIn: string[];
  description: string;
  keyFeatures: string[];
  howToInterpret: string;
  proTip: string;
}

export default function MetricsPage() {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<string>('all');
  const [selectedGraphSlice, setSelectedGraphSlice] = useState<number>(0);
  const [pizzaMode, setPizzaMode] = useState<'pizza' | 'radar'>('pizza');

  // Visual Graph Definitions
  const graphs: GraphDefinition[] = [
    {
      id: 'pizza-radar',
      title: 'Opta / StatsBomb Pizza Radar Chart',
      subtitle: 'Multi-metric radial percentile evaluation',
      usedIn: ['Player Dossier (/player/[id])', 'Head-to-Head Compare (/compare)'],
      description:
        'A radial visualization displaying tactical metrics grouped into 4 distinct quadrants. Each slice represents a specific statistical category, and its radial length (distance from the center) represents the player’s percentile rank (0 to 99) against positional peers.',
      keyFeatures: [
        '4 Positional Quadrants: Attacking (Amber), Passing (Cyan), Possession (Purple), and Defending (Rose).',
        'Goalkeeper Mode: Shot Stopping, Handling/Rebounds, Sweeping/Aerial, and Distribution.',
        'Percentile Standard: Evaluated against players within the exact same positional cluster with >= 900 minutes.',
        'Dual View Mode: Toggle between individual Pizza Slices (independent attributes) and Classic Radar Polygon (overall tactical silhouette).',
      ],
      howToInterpret:
        'A full slice reaching the outer perimeter means the player is in the 99th percentile (top 1% globally for that position). A shriveled slice near the center indicates a below-average metric. Look for wide, balanced coverage for all-arounders or sharp, focused spikes for specialist roles.',
      proTip:
        'Hover over any slice in the Player Dossier to reveal the exact raw per-90 count alongside the computed percentile.',
    },
    {
      id: 'fbref-percentiles',
      title: 'FBref Percentile Benchmark Bar Table',
      subtitle: 'Linear statistical progress bars across performance domains',
      usedIn: ['Player Dossier (/player/[id])'],
      description:
        'Inspired by FBref / StatsBomb scouting reports, this table displays raw per-90 values side-by-side with horizontal percentile progress bars, segmented into Attacking, Passing, Defending, and Physical categories.',
      keyFeatures: [
        'Color-Coded Tiers: Elite 80+ (Emerald Green), Above Average 60–79 (Cyan), Good 40–59 (Blue), Average 20–39 (Amber), Below Average <20 (Rose).',
        'Direct Per-90 Display: Compare actual game output (e.g. 0.54 NPxG/90) against relative ranking (e.g. 96th percentile).',
        'Positional Baseline: Eliminates apples-to-oranges comparisons by conditioning ranks on positional clusters.',
      ],
      howToInterpret:
        'The colored fill of the bar visually shows percentile depth. A player with multiple green bars in Defending and blue bars in Passing is an ideal Ball-Playing Defender archetype.',
      proTip:
        'Click column headers to sort between metric name, raw per-90 count, and relative percentile.',
    },
    {
      id: 'scatter-explorer',
      title: '2D Moneyball Scatter Explorer',
      subtitle: 'Dynamic bivariate regression and quadrant discovery',
      usedIn: ['Scatter Explorer (/scatter)'],
      description:
        'An interactive 2-dimensional scatter plot allowing recruiters to map any statistical output metric against market valuation, wages, or physical output. Features automated median crosshairs that split the player pool into four distinct market quadrants.',
      keyFeatures: [
        'Customizable Axes: Plot NPxG+xA/90 against Transfer Valuation (£M), Aerial Duels Won against Heading Acc %, etc.',
        '4-Quadrant Analysis: Automatically identifies the "Golden Moneyball Target Zone" (Upper-Left: High Output, Low Cost).',
        'Interactive Tooltips: Click any bubble to view player demographics and jump straight to their full scouting dossier.',
      ],
      howToInterpret:
        'Focus your transfer budget on the upper-left quadrant (High Output, Low Valuation). Avoid the bottom-right quadrant (Low Output, High Valuation/Wages), which represents distressed or overhyped assets.',
      proTip:
        'Filter by Position and Minutes Played to isolate transfer targets within your specific recruitment tier.',
    },
    {
      id: 'screener-grid',
      title: 'TradingView Quantitative Screener',
      subtitle: 'Filter pill chips, strategy presets, and multi-column sorting',
      usedIn: ['Player Screener (/screener)'],
      description:
        'Modeled after institutional trading terminal screeners, this table provides fast multi-variable filtering across 11,350+ players with active filter chips, valuation sliders, and tactical strategy presets.',
      keyFeatures: [
        'Preset Strategy Filters: Instant 1-click presets including "Wonderkids (U21)", "Moneyball Gems", "Elite Playmakers", and "Shot-Stoppers".',
        'Dual Screener Engine: Dedicated toggles for Outfield Players (10,500+ records) and Goalkeepers (850+ records).',
        'Instant CSV Export: Download your filtered recruitment pool with a single click to import into spreadsheets or shortlist notes.',
      ],
      howToInterpret:
        'Combine high-output performance filters (e.g. NPxG+xA >= 0.40) with tight budget constraints (e.g. Transfer Value <= £10M) to instantly surface hidden gems playing in second-tier leagues.',
      proTip:
        'Use the "In Shortlist" star toggle to isolate players you have bookmarked for upcoming transfer windows.',
    },
    {
      id: 'h2h-differential',
      title: 'Head-to-Head Comparison & Edge Matrix',
      subtitle: 'Side-by-side statistical radar and differential advantage bars',
      usedIn: ['Head-to-Head Compare (/compare)'],
      description:
        'Enables head-to-head evaluation between any two players in the global database. Features an overlaid dual-polygon radar chart alongside a Statistical Differential Matrix showing which player has the tactical edge.',
      keyFeatures: [
        'Overlaid Dual Polygon Radar: Cyan polygon (Player A) vs Gold polygon (Player B).',
        'Edge Matrix: Green and red directional bars highlighting statistical advantages and percentage differentials.',
        'Cross-Role Benchmarking: Benchmark your current starter against an active transfer shortlist candidate.',
      ],
      howToInterpret:
        'If the cyan polygon extends beyond the gold polygon in a specific category, Player A dominates that aspect of play. The edge matrix quantifies the exact percentage delta (e.g. +28% higher progressive passing volume).',
      proTip:
        'Great for pre-match tactical planning: compare your fullbacks directly against an opposing winger.',
    },
    {
      id: 'scd2-timeline',
      title: 'Career Progression Timeline (SCD Type 2)',
      subtitle: 'Multi-season historical development tracking',
      usedIn: ['Player Dossier (/player/[id])'],
      description:
        'Powered by Slowly Changing Dimensions Type 2 (SCD2) in the SQL Server warehouse, this longitudinal timeline displays a player’s statistical development, valuation changes, and role shifts season-over-season without overwriting past history.',
      keyFeatures: [
        'Season Delta Calculations: Measures exact shifts in per-90 metrics (e.g. +0.12 NPxG/90 increase from 2023/24 to 2024/25).',
        'Historical Integrity: Tracks historical club, league tier, and wage growth chronologically.',
        'Development Trajectory: Verifies whether a young prospect is making genuine statistical strides or plateauing.',
      ],
      howToInterpret:
        'Upward green trend arrows indicate statistical growth in offensive involvement or defensive volume; downward red indicators signal attribute regression or tactical misalignment.',
      proTip:
        'Examine historical seasons to see if a player performed well in a stronger league before transferring.',
    },
    {
      id: 'tactical-framework-matrix',
      title: 'Team Tactical Framework & Archetype Matrix',
      subtitle: 'Minutes-weighted style classification across 7 tactical systems',
      usedIn: ['Tactical Archetype Classifier (/archetypes)'],
      description:
        'Evaluates club tactical systems across 7 dominant modern tactical philosophies (Gegenpressing, Tiki-Taka, Vertical Tiki-Taka, Fluid Counter, Direct Counter, Wing Play, Route One, Catenaccio) based on minutes-weighted passing, pressing, crossing, and defensive block events.',
      keyFeatures: [
        'Minutes-Weighted Aggregation: Squad members with higher on-pitch minutes exert proportionally greater weighting on team identity.',
        'Archetype Affinity Scores (0-100): Quantifies each club’s adherence to high-press recovery vs deep-block containment.',
        'Squad Archetype Distribution: Breakdown of players classified into 21 distinct FM tactical archetypes.',
      ],
      howToInterpret:
        'High Gegenpressing scores (>75) indicate relentless high-line counterpressing teams. High Catenaccio scores (>70) indicate deep-block low-risk containment teams.',
      proTip:
        'Scout transfer targets whose tactical archetype matches your team’s dominant tactical framework to minimize tactical adaptation lag.',
    },
    {
      id: 'squad-succession-matrix',
      title: 'Squad Succession Planning & Replacement Matrix',
      subtitle: 'Automated U23 succession candidate mapping for aging starters',
      usedIn: ['Squad Succession Planning (/succession)'],
      description:
        'Proactively identifies upcoming squad vacancies by pairing incumbent starters aged 28+ with top-performing U23 external targets who match their positional profile and tactical responsibilities.',
      keyFeatures: [
        'Succession Risk Scoring: Combines starter age, contract duration, and minute burden to prioritize urgent transfer needs.',
        'Top 3 Candidate Ranking: Surfaces the best young replacement options ranked by Euclidean similarity and performance.',
        'Financial Feasibility: Compares target market valuation and wage demands against your club’s transfer budget.',
      ],
      howToInterpret:
        'Red alerts denote starters aged 31+ with no clear understudy. Green pairings show pre-identified wonderkids ready to step in when the veteran is sold.',
      proTip:
        'Monitor candidates over two consecutive transfer windows using SCD2 progression tracking before triggering their buyout clauses.',
    },
    {
      id: 'squad-diagnosis-dashboard',
      title: 'Departmental Squad Health & Age Profile Matrix',
      subtitle: 'Club-wide positional depth, wage distribution, and tactical risk diagnosis',
      usedIn: ['Squad Diagnosis (/squad-diagnosis)'],
      description:
        'Holistic squad audit breaking down your roster into Goalkeeping, Central Defense, Fullbacks, Midfield, and Attack departments with automated health ratings and recruitment alerts.',
      keyFeatures: [
        'Positional Age Curves: Highlights departments trending above peak athletic age (>28.5 yrs).',
        'Starter vs Depth Delta: Measures performance drop-off when regular starters are rotated or injured.',
        'One-Click Recruitment Shortcut: Jumps directly from a diagnosed squad weakness into pre-filtered screener queries.',
      ],
      howToInterpret:
        'Departments flagged with "Critical" require immediate transfer intervention during the next window to avoid performance cliffs.',
      proTip:
        'Balance high-wage veterans with high-output U21 prospects to optimize your wage-to-points efficiency ratio.',
    },
  ];

  // Comprehensive Metric Definitions (Categorized into 6 core domains)
  const metrics: MetricDefinition[] = [
    // =========================================================================
    // 1. ATTACKING & SHOOTING
    // =========================================================================
    {
      id: 'npxg-per90',
      name: 'Non-Penalty Expected Goals per 90',
      code: 'NPxG/90',
      category: 'attacking',
      formula: '\\text{NPxG/90} = \\frac{\\text{Total Open-Play } xG}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The quality and volume of goalscoring chances a player gets from open play and set pieces (excluding penalty kicks).',
      tacticalMeaning:
        'Measures pure movement, off-the-ball anticipation, and positional poaching ability. Penalties are stripped out because they inflate scoring numbers and do not reflect repeatable open-play goal threat.',
      moneyballSignificance:
        'Penalties cause players to look significantly more prolific than they are, leading to overpaid transfer fees. NPxG isolates true underlying goal threat.',
      benchmarks: {
        elite: '>= 0.50 (World-class primary goalscorer)',
        good: '0.35 - 0.49 (Strong starting striker / inside forward)',
        average: '0.20 - 0.34 (Average wide attacker / attacking midfielder)',
        poor: '< 0.15 (Low direct goal threat)',
      },
      fmSource: 'xG (with penalties subtracted) and Mins from team_players.csv / interested_out_players.csv',
      tags: ['xG', 'Scoring', 'Striker', 'Winger', 'Finishing', 'Attacking'],
    },
    {
      id: 'xg-per90',
      name: 'Expected Goals per 90',
      code: 'xG/90',
      category: 'attacking',
      formula: '\\text{xG/90} = \\frac{\\text{Total Expected Goals}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Total cumulative expected goal probability generated per 90 minutes across all shots including penalties.',
      tacticalMeaning:
        'Reflects total goal expectancy based on historical shot trajectory, distance, angle, and defensive pressure at the exact moment of execution.',
      moneyballSignificance:
        'Establishes baseline finishing volume; comparing xG/90 against actual Goals/90 uncovers variance, hot streaks, or finishing slumps.',
      benchmarks: {
        elite: '>= 0.60 (Elite focal striker / penalty taker)',
        good: '0.40 - 0.59 (Consistent goal threat)',
        average: '0.22 - 0.39 (Standard offensive contributor)',
        poor: '< 0.15 (Minimal goal output)',
      },
      fmSource: 'xG and Mins from FM24 export',
      tags: ['xG', 'Goals', 'Striker', 'Attacking'],
    },
    {
      id: 'goals-per90',
      name: 'Goals Scored per 90',
      code: 'Goals/90',
      category: 'attacking',
      formula: '\\text{Goals/90} = \\frac{\\text{Total Goals Scored}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The actual volume of goals scored per 90 minutes of competitive play.',
      tacticalMeaning:
        'The definitive bottom-line production metric for forwards. When paired with NPxG, it reveals whether high goal tallies are repeatable or luck-driven.',
      moneyballSignificance:
        'Traditional clubs buy on raw Goals/90. Moneyball clubs exploit the market by selling players whose Goals/90 unsustainably exceeds their xG/90.',
      benchmarks: {
        elite: '>= 0.65 (Prolific match-winner / Golden Boot contender)',
        good: '0.45 - 0.64 (High-caliber top-flight forward)',
        average: '0.25 - 0.44 (Standard wide attacker / second striker)',
        poor: '< 0.18 (Low scoring rate)',
      },
      fmSource: 'Gls and Mins from FM24 export',
      tags: ['Goals', 'Finishing', 'Production', 'Attacking'],
    },
    {
      id: 'xg-per-shot',
      name: 'Shot Quality & Selection Index',
      code: 'xG / Shot',
      category: 'attacking',
      formula: '\\text{Shot Quality} = \\frac{\\text{Total } xG}{\\text{Total Shots Attempted}}',
      shortDefinition: 'The average expected goals probability of each shot taken by the player.',
      tacticalMeaning:
        'Indicates shot discipline and decision-making. High xG/shot players take high-percentage shots from inside the 6-yard box or central penalty area; low xG/shot players take low-probability speculative long shots.',
      moneyballSignificance:
        'A player taking 5 long shots per game with 0.04 xG/shot generates 0.20 xG by wasting possessions. A disciplined striker taking 2 shots with 0.22 xG/shot creates 0.44 xG with far less turnover risk.',
      benchmarks: {
        elite: '>= 0.18 (Clinical, high-percentage inside-the-box finisher)',
        good: '0.13 - 0.17 (Good shot selection)',
        average: '0.08 - 0.12 (Standard attacker)',
        poor: '< 0.07 (Wasteful speculative long-range shooter)',
      },
      fmSource: 'xG / shot column from FM24 export',
      tags: ['Shooting', 'Decision Making', 'Efficiency', 'Striker', 'Attacking'],
    },
    {
      id: 'npxg-per-shot',
      name: 'Non-Penalty xG per Shot',
      code: 'NP-xG / Shot',
      category: 'attacking',
      formula: '\\text{NP-xG / Shot} = \\frac{\\text{Non-Penalty } xG}{\\text{Total Shots Attempted}}',
      shortDefinition: 'Average expected goal quality per shot attempt with all penalty kicks removed.',
      tacticalMeaning:
        'Prevents penalty kick conversion (0.76 xG per attempt) from artificially inflating an attacker’s perceived shot selection efficiency.',
      moneyballSignificance:
        'Identifies penalty-box poachers who consistently work the ball into prime scoring positions before releasing shots.',
      benchmarks: {
        elite: '>= 0.16 (Elite close-range chance converter)',
        good: '0.12 - 0.15 (Above-average shot selection)',
        average: '0.08 - 0.11 (Standard outfield average)',
        poor: '< 0.07 (Takes speculative long-range shots)',
      },
      fmSource: 'Derived from NP-xG and Shots from FM24 export',
      tags: ['xG', 'Shot Selection', 'Efficiency', 'Attacking'],
    },
    {
      id: 'shots-per90',
      name: 'Total Shots Attempted per 90',
      code: 'Shots/90',
      category: 'attacking',
      formula: '\\text{Shots/90} = \\frac{\\text{Total Shots Attempted}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The total volume of shooting attempts taken per 90 minutes.',
      tacticalMeaning:
        'Measures offensive assertiveness and shooting gravity. High shot volume forces opposition centerbacks to step out and commit blocks.',
      moneyballSignificance:
        'A forward with elite xG/shot but only 1.0 shot per 90 cannot generate enough volume to carry a team. Elite goalscorers combine >=3.0 shots/90 with good xG/shot.',
      benchmarks: {
        elite: '>= 3.8 (High-volume offensive focal point)',
        good: '2.6 - 3.7 (Active primary forward)',
        average: '1.5 - 2.5 (Standard winger / attacking midfielder)',
        poor: '< 1.2 (Passive attacker)',
      },
      fmSource: 'Shots/90 column from FM24 export',
      tags: ['Shots', 'Volume', 'Attacking', 'Striker'],
    },
    {
      id: 'shot-on-target-rate',
      name: 'Shot on Target Accuracy %',
      code: 'SoT %',
      category: 'attacking',
      formula: '\\text{SoT \\%} = \\frac{\\text{Shots on Target}}{\\text{Total Shots Attempted}} \\times 100',
      shortDefinition: 'The percentage of all shot attempts that hit the target (requiring a goalkeeper save or resulting in a goal).',
      tacticalMeaning:
        'Evaluates shooting technique and composure under defensive pressure. Elite finishers consistently work the goalkeeper rather than spraying shots off-target.',
      moneyballSignificance:
        'High SoT % generates secondary scoring chances (rebounds, corners, deflections) even when the initial shot does not go in.',
      benchmarks: {
        elite: '>= 48.0% (Precise, accurate ball striker)',
        good: '40.0% - 47.9% (Reliable targeting)',
        average: '32.0% - 39.9% (Standard shooting accuracy)',
        poor: '< 30.0% (Erratic shooting)',
      },
      fmSource: 'SoT % from FM24 export',
      tags: ['Accuracy', 'Technique', 'Shooting', 'Attacking'],
    },
    {
      id: 'shot-conv',
      name: 'Shot Conversion Rate %',
      code: 'Conv %',
      category: 'attacking',
      formula: '\\text{Conversion Rate \\%} = \\frac{\\text{Total Goals}}{\\text{Total Shots Attempted}} \\times 100',
      shortDefinition: 'The percentage of all shots taken that result in a goal.',
      tacticalMeaning:
        'Measures pure execution efficiency. While volume is important, a high conversion rate indicates composure in front of goal.',
      moneyballSignificance:
        'Identifies attackers who do not need 10 chances to score, proving crucial for defensive counter-attacking tactics where chances are scarce.',
      benchmarks: {
        elite: '>= 22.0% (Elite clinical execution)',
        good: '16.0% - 21.9% (Reliable forward)',
        average: '10.0% - 15.9% (Average conversion)',
        poor: '< 9.0% (Wasteful in front of goal)',
      },
      fmSource: 'Shot Conv % from FM24 export',
      tags: ['Goals', 'Finishing', 'Striker', 'Attacking'],
    },
    {
      id: 'shrunk-finishing',
      name: 'Shrunk Finishing Delta (Empirical Bayes)',
      code: 'Finishing Index',
      category: 'attacking',
      formula: '\\text{Finishing Delta}_{\\text{shrunk}} = (\\text{Gls/90} - \\text{xG/90}) \\times \\left(\\frac{\\text{Minutes}}{\\text{Minutes} + 900}\\right)',
      shortDefinition: 'Finishing over- or under-performance adjusted by sample size shrinkage to filter out short-term luck.',
      tacticalMeaning:
        'In football, goal overperformance (scoring more than xG) is heavily noise-driven and rapidly regresses to zero over multi-season samples. This metric pulls small-sample hot streaks back to zero.',
      moneyballSignificance:
        'Never buy a player solely because they scored 14 goals from 7 xG in 1,200 minutes: they are experiencing short-term variance that will disappear next season.',
      benchmarks: {
        elite: '> +0.15 (Generational, sustained elite finisher like Kane or Haaland)',
        good: '+0.05 to +0.14 (Above-average finishing touch)',
        average: '-0.05 to +0.05 (Finishing aligns exactly with xG expectation)',
        poor: '< -0.10 (Severe finishing underperformance / confidence crisis)',
      },
      fmSource: 'Derived in SQL warehouse from Goals, xG, and Minutes Played',
      tags: ['Finishing', 'Bayesian', 'Regression', 'Moneyball', 'Attacking'],
    },
    {
      id: 'box-presence-proxy',
      name: 'Penalty Box Presence Proxy',
      code: 'Box Presence',
      category: 'attacking',
      formula: '\\text{Box Presence Proxy} = \\text{Shots/90} \\times \\text{xG/Shot}',
      shortDefinition: 'Composite index measuring how consistently and deeply an attacker penetrates the dangerous central penalty area.',
      tacticalMeaning:
        'Separates perimeter attackers who float on the wings from true target men and inside forwards who regularly take high-value shots from inside the 6-yard and 18-yard boxes.',
      moneyballSignificance:
        'High box presence correlates strongly with multi-season scoring sustainability across different tactical systems.',
      benchmarks: {
        elite: '>= 0.45 (Dominant penalty-box predator)',
        good: '0.30 - 0.44 (Consistent box arrival)',
        average: '0.15 - 0.29 (Peripheral attacker)',
        poor: '< 0.12 (Rarely penetrates penalty area)',
      },
      fmSource: 'Derived in gold mart from Shots/90 and xG/shot',
      tags: ['Box Presence', 'Positioning', 'Striker', 'Attacking'],
    },
    {
      id: 'long-range-goal-share',
      name: 'Long-Range Goal Share %',
      code: 'Long Goal %',
      category: 'attacking',
      formula: '\\text{Long Goal \\%} = \\frac{\\text{Goals Outside Penalty Box}}{\\text{Total Goals Scored}} \\times 100',
      shortDefinition: 'The proportion of a player’s goals scored from outside the 18-yard penalty box.',
      tacticalMeaning:
        'Evaluates long-range shooting capability vs reliance on tap-ins. Useful for identifying midfield long-shot specialists who can unlock low defensive blocks.',
      moneyballSignificance:
        'Beware of attackers with >40% long-range goal share: long shots have low repeatable conversion rates and typically regress sharply the following season.',
      benchmarks: {
        elite: '>= 25.0% (Specialist long-range ball striker)',
        good: '15.0% - 24.9% (Balanced scoring range)',
        average: '5.0% - 14.9% (Standard forward profile)',
        poor: '< 5.0% (Pure inside-the-box finisher)',
      },
      fmSource: 'Derived from Goals Outside Box and Total Goals in FM24 export',
      tags: ['Long Shots', 'Shooting', 'Attacking'],
    },
    {
      id: 'shots-outside-box-ratio',
      name: 'Shots Outside Penalty Box Ratio %',
      code: 'Outside Box %',
      category: 'attacking',
      formula: '\\text{Outside Box \\%} = \\frac{\\text{Shots Outside Box/90}}{\\text{Total Shots/90}} \\times 100',
      shortDefinition: 'The percentage of all shot attempts taken from outside the 18-yard penalty box.',
      tacticalMeaning:
        'Measures shooting discipline. Attackers with high ratios (>60%) often squander promising counter-attacks with speculative, low-probability shots.',
      moneyballSignificance:
        'Recruitment screening flag: high outside-box shooting ratios indicate poor tactical discipline unless coached out of the habit.',
      benchmarks: {
        elite: '<= 25.0% (Disciplined, works high-quality inside-the-box chances)',
        good: '26.0% - 40.0% (Acceptable balance)',
        average: '41.0% - 55.0% (Moderate long-range tendency)',
        poor: '> 60.0% (Wasteful speculative shooter)',
      },
      fmSource: 'Derived from Shots Outside Box/90 and Shots/90',
      tags: ['Decision Making', 'Shooting', 'Discipline', 'Attacking'],
    },
    {
      id: 'goal-threat-share',
      name: 'Goal Threat Share %',
      code: 'Threat Share %',
      category: 'attacking',
      formula: '\\text{Threat Share \\%} = \\frac{\\text{Player NPxG/90}}{\\text{NPxG/90} + \\text{xA/90}} \\times 100',
      shortDefinition: 'The proportion of a player’s personal expected goal involvement driven by direct goalscoring vs creative passing.',
      tacticalMeaning:
        'Classifies attacker role balance: >75% indicates a pure finishing poacher; 40–60% indicates a balanced dual-threat creator-scorer; <30% indicates a primary playmaker.',
      moneyballSignificance:
        'Ensures tactical squad balance: do not pair two 85% goal threat attackers together without a primary playmaker supplying them.',
      benchmarks: {
        elite: '50.0% - 65.0% (Ideal balanced dual-threat forward)',
        good: '66.0% - 80.0% (Pure goalscorer / finisher)',
        average: '35.0% - 49.0% (Creative wide forward / playmaker)',
      },
      fmSource: 'Derived via analytical views in gold schema',
      tags: ['Role', 'Tactics', 'Attacking', 'Striker', 'Playmaker'],
    },
    {
      id: 'npxg-adj-per90',
      name: 'League-Adjusted Non-Penalty xG per 90',
      code: 'Adj NPxG/90',
      category: 'attacking',
      formula: '\\text{Adj NPxG/90} = \\text{NPxG/90} \\times \\text{League Difficulty Coef}',
      shortDefinition: 'Non-penalty expected goals per 90 calibrated against league quality using competition difficulty coefficients.',
      tacticalMeaning:
        'Normalizes goal threat across different European and global tiers, preventing second-division or low-tier league attackers from dominating raw statistical leaderboards.',
      moneyballSignificance:
        'Enables cross-league scouting comparisons: a player producing 0.60 NPxG in a 0.70-rated league projects to 0.42 NPxG in an Apex league.',
      benchmarks: {
        elite: '>= 0.45 (Projects as high-level top-flight scorer)',
        good: '0.32 - 0.44 (Capable starting forward)',
        average: '0.18 - 0.31 (Mid-tier offensive output)',
      },
      fmSource: 'Calculated in gold mart from NP-xG and gold.dim_league coef',
      tags: ['Competition', 'Normalization', 'Scouting', 'Attacking'],
    },
    {
      id: 'npxg-plus-xa-adj',
      name: 'League-Adjusted NP-xGI per 90',
      code: 'Adj NP-xGI/90',
      category: 'attacking',
      formula: '\\text{Adj NP-xGI/90} = (\\text{NPxG/90} + \\text{xA/90}) \\times \\text{League Difficulty Coef}',
      shortDefinition: 'Total expected goal involvement (scoring + creating) normalized by competition difficulty level.',
      tacticalMeaning:
        'The ultimate cross-league recruitment benchmark for offensive potency. Calibrates raw production against defensive quality faced.',
      moneyballSignificance:
        'The primary filter for finding bargain imports playing in Portugal, Belgium, or Championship who can succeed in the Premier League.',
      benchmarks: {
        elite: '>= 0.55 (Elite international recruitment target)',
        good: '0.38 - 0.54 (Strong top-flight contributor)',
        average: '0.22 - 0.37 (Solid squad depth)',
      },
      fmSource: 'Calculated in gold mart from NP-xGI and league difficulty coef',
      tags: ['Moneyball', 'Recruitment', 'Cross-League', 'Attacking'],
    },

    // =========================================================================
    // 2. PASSING & CREATION
    // =========================================================================
    {
      id: 'xa-per90',
      name: 'Expected Assists per 90',
      code: 'xA/90',
      category: 'passing',
      formula: '\\text{xA/90} = \\frac{\\text{Total Expected Assists}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The probability that a pass completed by the player results in a goal, based on the shot quality of the receiver.',
      tacticalMeaning:
        'Isolates the passer’s vision and passing accuracy from the finishing ability of their teammates. Unlike raw assists, which require a teammate to score, xA awards full credit for elite chance creation.',
      moneyballSignificance:
        'A brilliant playmaker playing with terrible strikers might register 2 assists from 9 xA. Traditional scouts look at the 2 assists and pass; Moneyball scouts recognize elite creation at a bargain price.',
      benchmarks: {
        elite: '>= 0.35 (Elite playmaker / world-class creator)',
        good: '0.22 - 0.34 (Starting creator in top leagues)',
        average: '0.12 - 0.21 (Standard attacking midfielder / winger)',
        poor: '< 0.08 (Minimal creative output)',
      },
      fmSource: 'xA/90 column from FM24 export',
      tags: ['Assists', 'Vision', 'Playmaker', 'Passing', 'Creation'],
    },
    {
      id: 'assists-per90',
      name: 'Assists per 90',
      code: 'Ast/90',
      category: 'passing',
      formula: '\\text{Assists/90} = \\frac{\\text{Total Assists}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The actual number of goal-scoring final passes completed per 90 minutes.',
      tacticalMeaning:
        'Measures realized end product. Comparing Ast/90 against xA/90 reveals whether a creator’s teammates are overperforming or squandering open chances.',
      moneyballSignificance:
        'Exploits market inefficiencies: sell creators whose assist tallies are inflated by teammate wonder-strikes, and buy creators with high xA but low raw assists.',
      benchmarks: {
        elite: '>= 0.35 (Prolific primary assist provider)',
        good: '0.22 - 0.34 (Consistent creator)',
        average: '0.12 - 0.21 (Standard midfielder / winger)',
        poor: '< 0.08 (Low direct assist creation)',
      },
      fmSource: 'Ast and Mins from FM24 export',
      tags: ['Assists', 'Playmaker', 'Production', 'Passing'],
    },
    {
      id: 'npxg-xa-per90',
      name: 'Expected Goal Involvement per 90',
      code: 'xGI/90 (NPxG+xA)',
      category: 'passing',
      formula: '\\text{xGI/90} = \\text{NPxG/90} + \\text{xA/90}',
      shortDefinition: 'Combined non-penalty expected goals and expected assists per 90 minutes.',
      tacticalMeaning:
        'The gold standard benchmark for overall offensive firepower. It captures the total expected goal contribution a player delivers from both scoring and creating.',
      moneyballSignificance:
        'The primary numerator in Moneyball recruitment value calculations (e.g. Cost per xGI). Essential for discovering dual-threat attackers.',
      benchmarks: {
        elite: '>= 0.75 (World-class superstar level)',
        good: '0.50 - 0.74 (Upper-echelon European starter)',
        average: '0.30 - 0.49 (Standard midfield / wing contributor)',
        poor: '< 0.20 (Low overall offensive involvement)',
      },
      fmSource: 'Sum of NP-xG/90 and xA/90',
      tags: ['xGI', 'Offense', 'Benchmark', 'Moneyball', 'Passing'],
    },
    {
      id: 'chance-quality',
      name: 'Chance Creation Danger Index',
      code: 'xA / OP-KP',
      category: 'passing',
      formula: '\\text{Chance Quality} = \\frac{\\text{xAssists per 90}}{\\text{Open-Play Key Passes per 90}}',
      shortDefinition: 'The expected goal threat generated per open-play key pass.',
      tacticalMeaning:
        'Separates high-volume crossers who pump hopeful balls into crowded penalty boxes (low xA/OP-KP ~ 0.06) from incisive through-ball artists who set up tap-ins (high xA/OP-KP >= 0.18).',
      moneyballSignificance:
        'Prevents overvaluing wingers whose inflated key pass tallies come from low-percentage crosses.',
      benchmarks: {
        elite: '>= 0.20 (Incisive, high-threat chance creator)',
        good: '0.14 - 0.19 (Solid quality chance provider)',
        average: '0.08 - 0.13 (Average crossing / passing danger)',
        poor: '< 0.07 (Low-threat speculative delivery)',
      },
      fmSource: 'Calculated from xA/90 and OP-KP/90',
      tags: ['Playmaker', 'Quality', 'Passing', 'Decision Making'],
    },
    {
      id: 'open-play-kp-per90',
      name: 'Open-Play Key Passes per 90',
      code: 'OP-KP/90',
      category: 'passing',
      formula: '\\text{OP-KP/90} = \\frac{\\text{Open-Play Key Passes}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Passes executed in open play (excluding set pieces) that lead directly to a teammate shot attempt.',
      tacticalMeaning:
        'Isolates dynamic in-game playmaking from corner kicks and free kicks. Set piece specialists can inflate total key passes without possessing dynamic open-play vision.',
      moneyballSignificance:
        'High OP-KP/90 proves a playmaker can unlock compact defensive blocks without relying on set-piece routines.',
      benchmarks: {
        elite: '>= 2.4 (Elite creative engine in open play)',
        good: '1.6 - 2.3 (Strong attacking contributor)',
        average: '0.8 - 1.5 (Standard midfielder)',
        poor: '< 0.6 (Minimal open-play shot creation)',
      },
      fmSource: 'OPKP/90 from FM24 export',
      tags: ['Playmaker', 'Key Passes', 'Passing', 'Creation'],
    },
    {
      id: 'pass-accuracy',
      name: 'Pass Accuracy %',
      code: 'Pass Acc %',
      category: 'passing',
      formula: '\\text{Pass Accuracy \\%} = \\frac{\\text{Completed Passes}}{\\text{Attempted Passes}} \\times 100',
      shortDefinition: 'The overall percentage of attempted passes that successfully reach a teammate.',
      tacticalMeaning:
        'Indicates technical cleanliness and security. Must always be interpreted alongside progressive passing: sideways centerbacks often have 94% accuracy, while creative #10s operate at 78% due to high-risk through-balls.',
      moneyballSignificance:
        'Crucial for possession-based systems (Tiki-Taka) where maintaining ball control prevents opponent transition counters.',
      benchmarks: {
        elite: '>= 90.0% (Elite ball retention metronome)',
        good: '84.0% - 89.9% (Solid passing accuracy)',
        average: '76.0% - 83.9% (Acceptable for creative wingers)',
        poor: '< 72.0% (High turnover passing risk)',
      },
      fmSource: 'Pas % from FM24 export',
      tags: ['Retention', 'Technique', 'Passing'],
    },
    {
      id: 'completed-passes-per90',
      name: 'Completed Passes per 90',
      code: 'Cmp Passes/90',
      category: 'passing',
      formula: '\\text{Completed Passes/90} = \\text{Passes Attempted/90} \\times \\text{Pass Accuracy}',
      shortDefinition: 'The absolute volume of successful passes delivered per 90 minutes.',
      tacticalMeaning:
        'Quantifies overall build-up involvement and tactical centrality. Deep-lying playmakers (Registas) typically complete 65+ passes per 90.',
      moneyballSignificance:
        'Confirms whether a scouted midfielder has the engine and capacity to handle primary ball-distribution duties.',
      benchmarks: {
        elite: '>= 65.0 (Central midfield tempo controller / Regista)',
        good: '45.0 - 64.9 (Active build-up participant)',
        average: '28.0 - 44.9 (Standard wing / forward involvement)',
        poor: '< 22.0 (Peripheral on-ball involvement)',
      },
      fmSource: 'Derived from Pas A/90 and Pas % in gold mart',
      tags: ['Distribution', 'Volume', 'Midfield', 'Passing'],
    },
    {
      id: 'passes-attempted-per90',
      name: 'Passes Attempted per 90',
      code: 'Pass Att/90',
      category: 'passing',
      formula: '\\text{Pass Att/90} = \\frac{\\text{Total Passes Attempted}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The total volume of passing actions initiated per 90 minutes.',
      tacticalMeaning:
        'Reflects how often teammates look to give the player the ball. High pass attempts indicate trust and central responsibility in possession.',
      moneyballSignificance:
        'High pass attempts combined with low progressive output signals a conservative "sideways recycling" midfielder.',
      benchmarks: {
        elite: '>= 75.0 (High-usage midfield distributor)',
        good: '52.0 - 74.9 (Regular build-up hub)',
        average: '32.0 - 51.9 (Standard outfield involvement)',
      },
      fmSource: 'Pas A/90 from FM24 export',
      tags: ['Involvement', 'Passing', 'Tactics'],
    },
    {
      id: 'prog-passes-per90',
      name: 'Progressive Passes per 90',
      code: 'Pr Passes/90',
      category: 'passing',
      formula: '\\text{Pr Passes/90} = \\frac{\\text{Progressive Passes}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Completed passes that advance the ball substantially (>=10 meters) toward the opponent’s goal.',
      tacticalMeaning:
        'Identifies line-breaking passers who transition their team from defense to attack. Crucial metric for Ball-Playing Defenders, Deep-Lying Playmakers, and Registas.',
      moneyballSignificance:
        'Pass completion percentage is misleading because sideways and backward passes are safe. Progressive passes measure bravery and line-breaking utility.',
      benchmarks: {
        elite: '>= 7.5 (Elite deep-lying distributor / midfield tempo controller)',
        good: '5.0 - 7.4 (Good progression)',
        average: '3.0 - 4.9 (Average progression)',
        poor: '< 2.5 (Conservative, backwards passer)',
      },
      fmSource: 'Pr passes/90 column from FM24 export',
      tags: ['Progressive', 'Midfield', 'Ball-Playing Defender', 'Passing'],
    },
    {
      id: 'prog-actions-per90',
      name: 'Total Progressive Actions per 90',
      code: 'Prog Actions/90',
      category: 'passing',
      formula: '\\text{Prog Actions/90} = \\text{Progressive Passes/90} + \\text{Dribbles Made/90}',
      shortDefinition: 'Total volume of territory-advancing actions executed per 90, combining line-breaking passing and ball-carrying dribbles.',
      tacticalMeaning:
        'Holistic metric capturing the two primary methods of moving the ball upfield. Vital for evaluating dynamic fullbacks and modern #8 box-to-box midfielders.',
      moneyballSignificance:
        'Prevents bias toward pass-only or dribble-only players; surfaces versatile progressors who can adapt to varying defensive presses.',
      benchmarks: {
        elite: '>= 9.5 (Elite dual-threat transition progressor)',
        good: '6.5 - 9.4 (Active progressive engine)',
        average: '4.0 - 6.4 (Standard transition output)',
        poor: '< 3.5 (Static, stationary player)',
      },
      fmSource: 'Derived from Pr passes/90 and Drb/90 in gold mart',
      tags: ['Progression', 'Dribbling', 'Passing', 'Transitions'],
    },
    {
      id: 'prog-rate-per-pass',
      name: 'Progressive Pass Rate %',
      code: 'Prog Pass %',
      category: 'passing',
      formula: '\\text{Prog Rate \\%} = \\frac{\\text{Progressive Passes/90}}{\\text{Passes Attempted/90}} \\times 100',
      shortDefinition: 'The percentage of a player’s attempted passes that successfully advance the ball at least 10 meters toward goal.',
      tacticalMeaning:
        'Measures verticality and passing intent. High rates indicate direct, forward-thinking distributors; low rates indicate sideways possession recyclers.',
      moneyballSignificance:
        'Essential for vertical tiki-taka and direct counter-attacking systems where slow sideways passing kills attacking momentum.',
      benchmarks: {
        elite: '>= 16.0% (Highly penetrative, vertical line-breaker)',
        good: '11.0% - 15.9% (Positive forward intent)',
        average: '7.0% - 10.9% (Balanced possession distributor)',
        poor: '< 6.0% (Risk-averse sideways recycler)',
      },
      fmSource: 'Derived in gold.vw_transfer_targets_outfield',
      tags: ['Verticality', 'Efficiency', 'Passing'],
    },
    {
      id: 'progression-share',
      name: 'Team Progression Share %',
      code: 'Prog Share %',
      category: 'passing',
      formula: '\\text{Progression Share \\%} = \\frac{\\text{Player Pr Passes/90}}{\\text{Team Total Pr Passes/90}} \\times 100',
      shortDefinition: 'The percentage of the entire team’s forward progression executed by this individual player.',
      tacticalMeaning:
        'Reveals whether a midfielder is the irreplaceable metronome of their club’s build-up play.',
      moneyballSignificance:
        'Helps predict whether a player will adapt to taking on primary ball-distribution duties for a top club.',
      benchmarks: {
        elite: '>= 22.0% (Central build-up hub of the entire team)',
        good: '15.0% - 21.9% (Major build-up contributor)',
        average: '< 15.0% (Secondary or peripheral progression role)',
      },
      fmSource: 'Derived in gold schema dimensional views',
      tags: ['Midfield', 'Distribution', 'Tactics', 'Passing'],
    },
    {
      id: 'completed-crosses-per90',
      name: 'Completed Crosses per 90',
      code: 'Crosses Cmp/90',
      category: 'passing',
      formula: '\\text{Completed Crosses/90} = \\text{Crosses Attempted/90} \\times \\text{Cross Accuracy}',
      shortDefinition: 'The volume of successful crosses delivered into the penalty box per 90 minutes.',
      tacticalMeaning:
        'Key performance indicator for traditional wingers and attacking fullbacks/wingbacks in wing-play and target-man tactical frameworks.',
      moneyballSignificance:
        'Look at completed crosses rather than attempted crosses: high attempted crosses with low accuracy indicate wasteful turnover-generating flank play.',
      benchmarks: {
        elite: '>= 1.5 (Dominant wide flank deliverer)',
        good: '0.8 - 1.4 (Consistent crossing threat)',
        average: '0.3 - 0.7 (Standard wide involvement)',
        poor: '< 0.2 (Minimal crossing output)',
      },
      fmSource: 'Derived from Crs A/90 and Crs % in gold mart',
      tags: ['Crosses', 'Wingers', 'Fullback', 'Passing'],
    },
    {
      id: 'op-completed-crosses-per90',
      name: 'Open-Play Completed Crosses per 90',
      code: 'OP-Crosses/90',
      category: 'passing',
      formula: '\\text{OP Completed Crosses} = \\text{Open-Play Crosses Attempted/90} \\times \\text{OP Cross Accuracy}',
      shortDefinition: 'Successful crosses delivered during active open play, filtering out corner kicks and wide set pieces.',
      tacticalMeaning:
        'Measures ability to beat a fullback off the dribble or overlap effectively to deliver whipped crosses while moving at speed.',
      moneyballSignificance:
        'Separates pure dead-ball delivery specialists from genuine dynamic wide creators who create flank superiority in running play.',
      benchmarks: {
        elite: '>= 1.2 (Elite open-play wide creator)',
        good: '0.6 - 1.1 (Solid flank crossing)',
        average: '0.2 - 0.5 (Occasional crosser)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Crosses', 'Open Play', 'Flank', 'Passing'],
    },
    {
      id: 'cross-share-creation',
      name: 'Cross Share of Chance Creation %',
      code: 'Cross Share %',
      category: 'passing',
      formula: '\\text{Cross Share \\%} = \\frac{\\text{Completed Crosses/90}}{\\text{Chances Created/90}} \\times 100',
      shortDefinition: 'The proportion of a player’s total chance creation that originates from aerial or grounded crosses.',
      tacticalMeaning:
        'Identifies wide specialists vs interior playmakers: wingers with >60% rely on crossing; inverted wingers with <25% create via through-balls and cutbacks.',
      moneyballSignificance:
        'Crucial for tactical recruitment: ensure scouted wingers match your tactical philosophy (e.g. Inverted Winger vs Traditional Flank Winger).',
      benchmarks: {
        elite: '>= 55.0% (Pure traditional crossing winger)',
        good: '30.0% - 54.9% (Balanced wide creator)',
        average: '< 30.0% (Interior cutback / through-ball playmaker)',
      },
      fmSource: 'Derived in gold mart from crosses and chances created',
      tags: ['Wing Play', 'Tactics', 'Creation', 'Passing'],
    },
    {
      id: 'chance-creation-pass-pct',
      name: 'Chance Creation per Pass %',
      code: 'Chance/Pass %',
      category: 'passing',
      formula: '\\text{Chance Creation / Pass \\%} = \\frac{\\text{Chances Created/90}}{\\text{Passes Attempted/90}} \\times 100',
      shortDefinition: 'The percentage of attempted passes that directly create a scoring opportunity for a teammate.',
      tacticalMeaning:
        'Measures creative density: how much attacking danger a player packs into their passing distribution.',
      moneyballSignificance:
        'Surfaces hyper-efficient creative super-subs or low-touch playmakers whose creative output per touch is off the charts.',
      benchmarks: {
        elite: '>= 4.5% (High-danger creative incisiveness)',
        good: '2.8% - 4.4% (Solid creative danger)',
        average: '1.4% - 2.7% (Standard midfielder)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Efficiency', 'Playmaker', 'Passing'],
    },
    {
      id: 'assist-luck-shrunk',
      name: 'Shrunk Assist Luck (Empirical Bayes)',
      code: 'Assist Luck',
      category: 'passing',
      formula: '\\text{Assist Luck}_{\\text{shrunk}} = (\\text{Ast/90} - \\text{xA/90}) \\times \\left(\\frac{\\text{Minutes}}{\\text{Minutes} + 900}\\right)',
      shortDefinition: 'The delta between actual assists and expected assists shrunk by minutes played to isolate teammate finishing variance.',
      tacticalMeaning:
        'A positive delta indicates the player’s teammates converted difficult chances at an unsustainable rate; a negative delta indicates teammates squandered great passes.',
      moneyballSignificance:
        'Prevents overpaying for playmakers whose high assist tallies were the product of teammate luck rather than sustainable pass delivery.',
      benchmarks: {
        elite: '+0.00 (Perfect alignment with underlying creation)',
        good: '+0.05 to +0.10 (Benefiting from good finishing)',
        poor: '< -0.10 (Severe teammate finishing underperformance)',
      },
      fmSource: 'Derived in gold schema from Ast/90, xA/90, and Mins',
      tags: ['Bayesian', 'Regression', 'Assists', 'Moneyball', 'Passing'],
    },
    {
      id: 'assists-per-op-kp',
      name: 'Assist Conversion per Open-Play Key Pass',
      code: 'Ast / OP-KP',
      category: 'passing',
      formula: '\\text{Ast / OP-KP} = \\frac{\\text{Assists/90}}{\\text{Open-Play Key Passes/90}}',
      shortDefinition: 'The realized assist conversion rate per open-play key pass delivered.',
      tacticalMeaning:
        'Measures the real-world efficiency of a creator’s chance delivery in turning opportunities into scoreboard goals.',
      moneyballSignificance:
        'Useful for pairing playmakers with clinical strikers: a creator with high xA/OP-KP paired with a clinical finisher maximizes goal yield.',
      benchmarks: {
        elite: '>= 0.22 (Highly converted key pass deliveries)',
        good: '0.14 - 0.21 (Standard conversion)',
        average: '< 0.12 (Low conversion rate)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Conversion', 'Creation', 'Passing'],
    },
    {
      id: 'turnover-rate',
      name: 'Ball Turnover Rate %',
      code: 'Turnover %',
      category: 'passing',
      formula: '\\text{Turnover \\%} = \\frac{\\text{Possession Lost/90}}{\\text{Passes Attempted/90} + \\text{Dribbles/90}} \\times 100',
      shortDefinition: 'The percentage of a player’s on-ball involvements that result in a loss of possession.',
      tacticalMeaning:
        'Measures ball security and retention. Vital for central midfielders and inverted fullbacks who operate in central transition zones where turnovers lead to opponent counterattacks.',
      moneyballSignificance:
        'A high-turnover player is a liability in possession-heavy, high-pressing systems.',
      benchmarks: {
        elite: '<= 11.0% (Press-resistant, elite retention metronome)',
        good: '11.1% - 15.0% (Solid ball security)',
        average: '15.1% - 22.0% (Acceptable for creative wingers)',
        poor: '> 25.0% (High turnover risk)',
      },
      fmSource: 'Derived from Poss Lost/90, Pas A/90, and Drb/90',
      tags: ['Retention', 'Security', 'Turnovers', 'Midfield', 'Passing'],
    },

    // =========================================================================
    // 3. DEFENDING, DUELS & PHYSICAL
    // =========================================================================
    {
      id: 'def-actions-per90',
      name: 'Total Defensive Actions per 90',
      code: 'Def Actions/90',
      category: 'defending',
      formula: '\\text{Def Actions} = (\\text{Tackles} \\times \\text{Tck Acc}) + \\text{Int/90} + \\text{Blocks/90} + \\text{Clearances/90}',
      shortDefinition: 'The total volume of successful defensive interventions completed per 90 minutes.',
      tacticalMeaning:
        'Measures overall defensive workload and activity level. High values are characteristic of high-intensity ball-winners and active defenders.',
      moneyballSignificance:
        'Provides a unified volume count across tackles, interceptions, and clearances rather than evaluating tackles in isolation.',
      benchmarks: {
        elite: '>= 8.5 (High-activity defensive destroyer / active centerback)',
        good: '6.5 - 8.4 (Solid defensive workload)',
        average: '4.5 - 6.4 (Standard output)',
        poor: '< 4.0 (Passive defensive presence)',
      },
      fmSource: 'Derived from Tck A/90, Tck R, Int/90, Blk/90, Clr/90',
      tags: ['Defending', 'Tackles', 'Interceptions', 'Centerback', 'Workload'],
    },
    {
      id: 'non-clearance-def-actions',
      name: 'Non-Clearance Defensive Actions per 90',
      code: 'NC Def Act/90',
      category: 'defending',
      formula: '\\text{NC Def Actions} = (\\text{Tackles} \\times \\text{Tck Acc}) + \\text{Int/90} + \\text{Blocks/90} + \\text{Shots Blocked/90}',
      shortDefinition: 'Defensive intervention volume excluding clearances, creating an unbiased benchmark across centerbacks, fullbacks, and defensive midfielders.',
      tacticalMeaning:
        'Traditional defensive action metrics favor centerbacks who hoof clear balls. Stripping clearances isolates active ball-winning and passing lane interception skill.',
      moneyballSignificance:
        'Essential for scouting modern defensive midfielders (DMs) and inverted fullbacks who defend in midfield zones.',
      benchmarks: {
        elite: '>= 5.5 (Elite ball-winning disruptor)',
        good: '3.8 - 5.4 (High defensive engagement)',
        average: '2.5 - 3.7 (Standard defensive contribution)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Defending', 'Ball Winning', 'Midfield', 'Fullback'],
    },
    {
      id: 'reactive-ratio',
      name: 'Reactive vs Positional Defending Ratio',
      code: 'Reactive Ratio',
      category: 'defending',
      formula: '\\text{Reactive Ratio} = \\frac{\\text{Interceptions/90}}{\\text{Interceptions/90} + (\\text{Tackles} \\times \\text{Tck Acc})}',
      shortDefinition: 'The ratio of interceptions (anticipatory defending) to tackles (reactive recovery challenges).',
      tacticalMeaning:
        'Classifies defensive style: a high ratio (>= 0.55) indicates a cerebral, anticipatory defender (like Maldini or van Dijk) who intercepts passing lanes without diving into challenges; a low ratio (<= 0.40) indicates an aggressive tackler who engages in ground duels.',
      moneyballSignificance:
        'Vital for tactical harmony: pairing an aggressive, reactive "Stopper" with a positional, anticipatory "Cover" centerback.',
      benchmarks: {
        elite: '>= 0.60 (Cerebral positional reader / passing lane cut-off artist)',
        good: '0.45 - 0.59 (Balanced modern centerback)',
        average: '< 0.40 (Aggressive reactive tackler / physical challenge specialist)',
      },
      fmSource: 'Derived from Int/90 and Tackles Won/90',
      tags: ['Style', 'Centerback', 'Tactics', 'Defending'],
    },
    {
      id: 'duels-won-per90',
      name: 'Total Duels Won per 90 (Ground + Aerial)',
      code: 'Duels Won/90',
      category: 'defending',
      formula: '\\text{Duels Won/90} = (\\text{Tackles} \\times \\text{Tck Acc}) + (\\text{Headers} \\times \\text{Heading Acc})',
      shortDefinition: 'The total volume of 1v1 ground challenges and aerial duels won cleanly per 90 minutes.',
      tacticalMeaning:
        'The definitive measure of physical and duel dominance. Centerbacks and defensive midfielders must dominate duels to prevent opponents from establishing territory.',
      moneyballSignificance:
        'Teams with superior duel win volumes control match state and concede fewer high-danger transition counters.',
      benchmarks: {
        elite: '>= 6.5 (Physical powerhouse / dominant duel winner)',
        good: '4.5 - 6.4 (Reliable contest winner)',
        average: '3.0 - 4.4 (Moderate physical presence)',
        poor: '< 2.8 (Vulnerable in 1v1 contests)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Duels', 'Physical', 'Tackles', 'Headers', 'Defending'],
    },
    {
      id: 'tackles-attempted-per90',
      name: 'Tackles Attempted per 90',
      code: 'Tck Att/90',
      category: 'defending',
      formula: '\\text{Tackles Attempted/90} = \\frac{\\text{Tackles Attempted}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The frequency with which a player commits to physical ground tackling challenges per 90 minutes.',
      tacticalMeaning:
        'Measures defensive aggression and challenge volume. Always pair with tackle accuracy to ensure the player isn’t diving in recklessly.',
      moneyballSignificance:
        'High tackle attempts with low win rates cause defensive breakdowns and dangerous free kicks in your defensive third.',
      benchmarks: {
        elite: '>= 3.5 (Aggressive ground ball-winner)',
        good: '2.4 - 3.4 (Active challenge engagement)',
        average: '1.4 - 2.3 (Positional defending)',
      },
      fmSource: 'Tck A/90 from FM24 export',
      tags: ['Tackles', 'Aggression', 'Defending'],
    },
    {
      id: 'tackle-accuracy',
      name: 'Tackle Win Rate %',
      code: 'Tackle Win %',
      category: 'defending',
      formula: '\\text{Tackle Win Rate \\%} = \\frac{\\text{Tackles Won}}{\\text{Tackles Attempted}} \\times 100',
      shortDefinition: 'The percentage of attempted ground tackles resulting in clean possession recovery without committing a foul.',
      tacticalMeaning:
        'Measures tackling timing, technique, and discipline. Defenders with >=75% win rate rarely get beaten off the dribble.',
      moneyballSignificance:
        'A high-volume tackler with 55% tackle accuracy is a defensive liability who gets dribbled past regularly.',
      benchmarks: {
        elite: '>= 80.0% (Elite 1v1 ground stopper)',
        good: '70.0% - 79.9% (Reliable tackler)',
        average: '60.0% - 69.9% (Average duel win rate)',
        poor: '< 55.0% (Frequently beaten / dives in)',
      },
      fmSource: 'Tck R from FM24 export',
      tags: ['Tackles', 'Accuracy', 'Defending'],
    },
    {
      id: 'interceptions-per90',
      name: 'Interceptions per 90',
      code: 'Int/90',
      category: 'defending',
      formula: '\\text{Interceptions/90} = \\frac{\\text{Total Interceptions}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Passes intercepted per 90 minutes by reading opposition passing lanes.',
      tacticalMeaning:
        'Reflects tactical intelligence, anticipation, and positional discipline. The hallmark trait of elite holding midfielders (Kante, Busquets) and reading centerbacks (van Dijk).',
      moneyballSignificance:
        'Interceptions regain possession cleanly without stopping play or risking cards, initiating immediate transition attacks.',
      benchmarks: {
        elite: '>= 2.5 (World-class passing lane reader)',
        good: '1.7 - 2.4 (Strong positional reader)',
        average: '1.0 - 1.6 (Standard outfield contribution)',
        poor: '< 0.8 (Passive lane coverage)',
      },
      fmSource: 'Int/90 from FM24 export',
      tags: ['Interceptions', 'Anticipation', 'IQ', 'Defending'],
    },
    {
      id: 'clearances-per90',
      name: 'Clearances per 90',
      code: 'Clr/90',
      category: 'defending',
      formula: '\\text{Clearances/90} = \\frac{\\text{Total Clearances}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Defensive emergency clearances executed under pressure per 90 minutes.',
      tacticalMeaning:
        'Measures penalty-box protection against crosses, long balls, and set pieces. Characteristic of traditional "no-nonsense" centerbacks in low-block systems.',
      moneyballSignificance:
        'Possession-heavy teams seek centerbacks with moderate clearances and high interceptions to recycle possession rather than kicking the ball out of play.',
      benchmarks: {
        elite: '>= 5.5 (Deep-block aerial clearer / penalty-box anchor)',
        good: '3.5 - 5.4 (Solid defensive relief)',
        average: '2.0 - 3.4 (Standard centerback / fullback)',
      },
      fmSource: 'Clr/90 from FM24 export',
      tags: ['Clearances', 'Centerback', 'Defending'],
    },
    {
      id: 'blocks-per90',
      name: 'Blocked Passes & Crosses per 90',
      code: 'Blocks/90',
      category: 'defending',
      formula: '\\text{Blocks/90} = \\frac{\\text{Total Blocks}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Opposition passes and crosses physically blocked per 90 minutes.',
      tacticalMeaning:
        'Reflects willingness to close down shooting and crossing angles on the perimeter of the box.',
      moneyballSignificance:
        'Fullbacks with high block numbers stifle opponent wingers from generating high-quality deliveries.',
      benchmarks: {
        elite: '>= 1.8 (Dominant angle-closer / active blocker)',
        good: '1.1 - 1.7 (Solid lane obstruction)',
        average: '0.6 - 1.0 (Standard outfield volume)',
      },
      fmSource: 'Blk/90 from FM24 export',
      tags: ['Blocks', 'Fullback', 'Defending'],
    },
    {
      id: 'shots-blocked-per90',
      name: 'Opponent Shots Blocked per 90',
      code: 'Shots Blk/90',
      category: 'defending',
      formula: '\\text{Shots Blocked/90} = \\frac{\\text{Opponent Shots Blocked}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Opponent shot attempts physically blocked by putting the body on the line inside the defensive third.',
      tacticalMeaning:
        'The ultimate measure of last-ditch penalty-box defending and shot suppression. Crucial attribute for centerbacks in Catenaccio and low-block frameworks.',
      moneyballSignificance:
        'Directly shields the goalkeeper by denying shots from ever reaching the goal frame.',
      benchmarks: {
        elite: '>= 1.2 (Brave, last-ditch shot stopper)',
        good: '0.7 - 1.1 (Solid penalty-box shield)',
        average: '< 0.6 (Standard CB presence)',
      },
      fmSource: 'Shots Blk/90 from FM24 export',
      tags: ['Shot Blocking', 'Centerback', 'Defending'],
    },
    {
      id: 'aerial-won-per90',
      name: 'Aerial Dominance & Duel Win Rate',
      code: 'Aer Won/90 & Hdr %',
      category: 'defending',
      formula: '\\text{Aerials Won/90} = \\text{Headers Attempted/90} \\times \\text{Heading Accuracy \\%}',
      shortDefinition: 'The volume and success rate of aerial duels contested per 90 minutes.',
      tacticalMeaning:
        'Measures dominance in the air against opponent long balls, crosses, and set pieces. A centerback must combine high volume with high win percentage.',
      moneyballSignificance:
        'A centerback with 15 Jumping Reach in FM who only wins 52% of aerial duels is an aerial liability. Look for players winning >= 70% in competitive leagues.',
      benchmarks: {
        elite: 'Aer Won >= 4.5 / 90 with Heading Acc >= 72% (Dominant colossus in the air)',
        good: 'Aer Won 3.0 - 4.4 / 90 with Heading Acc 63% - 71%',
        average: 'Aer Won < 2.5 / 90 or Heading Acc < 58%',
        poor: '< 1.8 / 90 (Vulnerable in the air)',
      },
      fmSource: 'Aer A/90 and Hdr % from FM24 export',
      tags: ['Aerial', 'Headers', 'Centerback', 'Striker', 'Defending'],
    },
    {
      id: 'heading-accuracy',
      name: 'Heading Accuracy %',
      code: 'Heading Acc %',
      category: 'defending',
      formula: '\\text{Heading Accuracy \\%} = \\frac{\\text{Aerial Duels Won}}{\\text{Aerial Duels Attempted}} \\times 100',
      shortDefinition: 'The percentage of contested aerial duels won cleanly.',
      tacticalMeaning:
        'Isolates aerial duel success rate from raw volume. Vital for evaluating whether a defender wins their 50/50 aerial battles.',
      moneyballSignificance:
        'Target men and centerbacks below 60% heading accuracy struggle against physical Route One teams.',
      benchmarks: {
        elite: '>= 72.0% (Elite aerial dominance)',
        good: '62.0% - 71.9% (Reliable in the air)',
        average: '50.0% - 61.9% (Average aerial contest)',
        poor: '< 48.0% (Aerial liability)',
      },
      fmSource: 'Hdr % from FM24 export',
      tags: ['Aerial', 'Heading', 'Defending'],
    },
    {
      id: 'dribbles-made-per90',
      name: 'Successful Dribbles Made per 90',
      code: 'Dribbles/90',
      category: 'defending',
      formula: '\\text{Dribbles Made/90} = \\frac{\\text{Successful Dribbles}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Completed 1v1 take-ons and dribbles past opposing defenders per 90 minutes.',
      tacticalMeaning:
        'Measures press-resistance and 1v1 flair. Attackers who complete dribbles collapse defensive structures by pulling second defenders out of position.',
      moneyballSignificance:
        'High dribble volume in wide areas creates numerical overloads and fouls in the attacking third.',
      benchmarks: {
        elite: '>= 3.2 (Elite 1v1 dynamic dribbler)',
        good: '2.0 - 3.1 (Consistent take-on threat)',
        average: '1.0 - 1.9 (Standard wing / midfield carrying)',
        poor: '< 0.8 (Rarely takes on opponents)',
      },
      fmSource: 'Drb/90 from FM24 export',
      tags: ['Dribbling', 'Possession', 'Flair', '1v1'],
    },
    {
      id: 'net-ball-security',
      name: 'Net Ball Security per 90',
      code: 'Ball Security',
      category: 'defending',
      formula: '\\text{Net Ball Security} = \\text{Completed Passes/90} - \\text{Possession Lost/90}',
      shortDefinition: 'Net margin of accurate passes completed over possessions lost per 90 minutes.',
      tacticalMeaning:
        'Evaluates overall on-ball net value. Players with high positive margins keep their team on the front foot; negative margins signal ball-retention liabilities.',
      moneyballSignificance:
        'Crucial for vetting deep midfielders: guarantees the player produces far more completed build-up actions than turnovers.',
      benchmarks: {
        elite: '>= 45.0 (Press-resistant build-up anchor)',
        good: '30.0 - 44.9 (Safe, reliable ball progressor)',
        average: '15.0 - 29.9 (Standard midfield security)',
        poor: '< 10.0 (High turnover deficit)',
      },
      fmSource: 'Derived from Completed Passes/90 and Poss Lost/90',
      tags: ['Security', 'Retention', 'Midfield', 'Turnovers'],
    },
    {
      id: 'possession-won-per90',
      name: 'Possession Won (Ball Recoveries) per 90',
      code: 'Poss Won/90',
      category: 'defending',
      formula: '\\text{Poss Won/90} = \\frac{\\text{Possessions Won}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Loose ball recoveries and possession regains completed per 90 minutes.',
      tacticalMeaning:
        'Measures tenacity and second-ball awareness. Central midfielders who anticipate loose balls regain control before opponents can launch counters.',
      moneyballSignificance:
        'Essential for Gegenpressing: winning loose balls high up the pitch directly creates high-xG scoring chances against disorganized defenses.',
      benchmarks: {
        elite: '>= 8.5 (Relentless ball-recovery engine)',
        good: '6.5 - 8.4 (Active loose-ball winner)',
        average: '4.5 - 6.4 (Standard recovery rate)',
      },
      fmSource: 'Poss Won/90 from FM24 export',
      tags: ['Recoveries', 'Pressing', 'Midfield', 'Defending'],
    },
    {
      id: 'possession-lost-per90',
      name: 'Possession Lost (Dispossessions) per 90',
      code: 'Poss Lost/90',
      category: 'defending',
      formula: '\\text{Poss Lost/90} = \\frac{\\text{Possessions Lost}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The volume of times possession was surrendered through misplaced passes, failed dribbles, or dispossessions per 90.',
      tacticalMeaning:
        'Context-dependent: creative wingers who take high risks can tolerate 15-20 losses; central defensive midfielders losing possession >10 times per game are high liabilities.',
      moneyballSignificance:
        'Identify whether high turnover tallies are justified by commensurate high creative threat (e.g. NPxG+xA).',
      benchmarks: {
        elite: '<= 7.0 (Ultra-secure central metronome - fewer is better)',
        good: '7.1 - 11.0 (Solid ball security)',
        average: '11.1 - 16.0 (Normal for creative attackers)',
        poor: '> 18.0 (Careless in possession)',
      },
      fmSource: 'Poss Lost/90 from FM24 export',
      tags: ['Turnovers', 'Security', 'Risk Management'],
    },
    {
      id: 'possession-differential',
      name: 'Possession Differential per 90',
      code: 'Poss Diff/90',
      category: 'defending',
      formula: '\\text{Possession Differential} = \\text{Possession Won/90} - \\text{Possession Lost/90}',
      shortDefinition: 'Net on-pitch ball recoveries minus turnovers per 90 minutes.',
      tacticalMeaning:
        'Positive differential indicates a net ball winner; negative differential indicates a net ball surrenderer.',
      moneyballSignificance:
        'Holding midfielders must maintain positive possession differentials to stabilize team tactical structure.',
      benchmarks: {
        elite: '>= +3.0 (Dominant net ball recovery engine)',
        good: '+0.5 to +2.9 (Net positive ball-winner)',
        average: '-2.0 to +0.4 (Neutral balance)',
        poor: '< -3.5 (Heavy possession surrenderer)',
      },
      fmSource: 'Derived from Poss Won/90 and Poss Lost/90',
      tags: ['Possession', 'Ball Winning', 'Midfield', 'Defending'],
    },
    {
      id: 'press-efficiency',
      name: 'High-Pressing Workload & Efficiency',
      code: 'Press Eff %',
      category: 'defending',
      formula: '\\text{Press Efficiency} = \\frac{\\text{Pressures Completed/90}}{\\text{Pressures Attempted/90}} \\times 100',
      shortDefinition: 'The success percentage of high-intensity pressing actions that force a turnover within 5 seconds.',
      tacticalMeaning:
        'Crucial for Gegenpressing, high-tempo modern managers. Evaluates whether an attacker or midfielder presses intelligently or runs aimlessly.',
      moneyballSignificance:
        'Eliminates players who cannot execute high-pressing tactical instructions.',
      benchmarks: {
        elite: '>= 32.0% (Elite pressing disruptor)',
        good: '25.0% - 31.9% (Solid press contributor)',
        average: '< 22.0% (Low pressing effectiveness)',
      },
      fmSource: 'Pres A/90 and Pres C/90 from FM24 export',
      tags: ['Pressing', 'Work Rate', 'Gegenpress', 'Midfield', 'Defending'],
    },
    {
      id: 'pressure-accuracy',
      name: 'Press Success Percentage %',
      code: 'Press Acc %',
      category: 'defending',
      formula: '\\text{Press Accuracy \\%} = \\frac{\\text{Pressures Completed}}{\\text{Pressures Attempted}} \\times 100',
      shortDefinition: 'The proportion of pressing interventions that regain possession or force a turnover within 5 seconds.',
      tacticalMeaning:
        'Identifies smart pressers who trap ball carriers into mistakes rather than committing fouls or getting bypassed.',
      moneyballSignificance:
        'A high-pressing forward line with >28% press accuracy creates high-xG counter-pressing chances close to the opposition goal.',
      benchmarks: {
        elite: '>= 30.0% (Clinical pressing execution)',
        good: '24.0% - 29.9% (Effective press participant)',
        average: '18.0% - 23.9% (Standard press accuracy)',
      },
      fmSource: 'Pres C and Pres A from FM24 export',
      tags: ['Pressing', 'Accuracy', 'Defending', 'Gegenpress'],
    },
    {
      id: 'pressures-attempted-per90',
      name: 'Pressures Attempted per 90',
      code: 'Press Att/90',
      category: 'defending',
      formula: '\\text{Pressures Attempted/90} = \\frac{\\text{Total Pressures Attempted}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The frequency with which a player closes down opposing ball carriers per 90 minutes.',
      tacticalMeaning:
        'Measures defensive work-rate and intensity. High values indicate tireless workhorses essential for pressing frameworks.',
      moneyballSignificance:
        'Reveals whether expensive flair players are willing to work off the ball for the team.',
      benchmarks: {
        elite: '>= 22.0 (Relentless high-intensity pressing engine)',
        good: '16.0 - 21.9 (Active pressing participant)',
        average: '10.0 - 15.9 (Moderate pressing involvement)',
        poor: '< 8.0 (Low defensive work rate)',
      },
      fmSource: 'Pres A/90 from FM24 export',
      tags: ['Work Rate', 'Pressing', 'Intensity', 'Defending'],
    },
    {
      id: 'sprints-per-km',
      name: 'Sprints per km (Running Intensity)',
      code: 'Sprints / km',
      category: 'defending',
      formula: '\\text{Sprints per km} = \\frac{\\text{Sprints/90}}{\\text{Distance Covered (km)/90}}',
      shortDefinition: 'Running intensity index measuring the proportion of total running distance executed at high-speed sprint velocity.',
      tacticalMeaning:
        'Separates players who jog or coast (low sprints/km ~ 0.40) from explosive athletes who sprint relentlessly throughout 90 minutes (sprints/km >= 0.75).',
      moneyballSignificance:
        'Raw distance covered is deceptive because slow jogging pads distance. Sprints/km isolates true athletic explosiveness.',
      benchmarks: {
        elite: '>= 0.75 (Explosive, high-intensity athletic engine)',
        good: '0.60 - 0.74 (Solid athletic intensity)',
        average: '0.45 - 0.59 (Standard aerobic runner)',
        poor: '< 0.40 (Low sprint intensity / slow jogger)',
      },
      fmSource: 'Derived from Sprints/90 and Dist/90 in gold mart',
      tags: ['Athleticism', 'Intensity', 'Pace', 'Physical'],
    },
    {
      id: 'high-press-load',
      name: 'High Press Workload Index',
      code: 'Press Load',
      category: 'defending',
      formula: '\\text{High Press Load} = \\text{Pressures Attempted/90} \\times \\text{Sprints per km}',
      shortDefinition: 'Composite work-rate index combining pressing attempt volume with high-intensity sprinting speed.',
      tacticalMeaning:
        'Identifies the hardest-working defensive forwards and midfielders who physically overwhelm opponent build-up lines.',
      moneyballSignificance:
        'Crucial for recruiting front-line pressers in Jurgen Klopp or Marcelo Bielsa tactical systems.',
      benchmarks: {
        elite: '>= 12.0 (Elite Gegenpressing dynamo)',
        good: '8.0 - 11.9 (High-workrate presser)',
        average: '5.0 - 7.9 (Moderate press load)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Gegenpress', 'Workload', 'Physical', 'Defending'],
    },
    {
      id: 'distance-covered-km-per90',
      name: 'Distance Covered per 90 (km)',
      code: 'Dist (km)/90',
      category: 'defending',
      formula: '\\text{Dist (km)/90} = \\frac{\\text{Total Distance Run in km}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The total aerobic running volume in kilometers completed per 90 minutes.',
      tacticalMeaning:
        'Measures stamina and physical durability. Box-to-box midfielders (Carrileros, Mezzalas) typically lead teams with 12.0+ km per match.',
      moneyballSignificance:
        'Ensures players possess the physical conditioning required to sustain performance in congested fixture calendars.',
      benchmarks: {
        elite: '>= 12.2 km (Tireless marathon runner / elite engine)',
        good: '11.2 - 12.1 km (High work capacity)',
        average: '10.0 - 11.1 km (Standard outfield running)',
        poor: '< 9.5 km (Low stamina / limited mobility)',
      },
      fmSource: 'Dist/90 from FM24 export',
      tags: ['Stamina', 'Physical', 'Work Rate', 'Fitness'],
    },
    {
      id: 'sprints-per90',
      name: 'High-Intensity Sprints per 90',
      code: 'Sprints/90',
      category: 'defending',
      formula: '\\text{Sprints/90} = \\frac{\\text{Total Sprints Completed}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'The total volume of explosive, high-velocity sprint bursts executed per 90 minutes.',
      tacticalMeaning:
        'Evaluates pace and willingness to execute overlapping runs, recovery sprints, and channel bursts.',
      moneyballSignificance:
        'Pace and sprint volume are the most reliable predictors of seamless adaptation when stepping up to faster, higher-tier leagues.',
      benchmarks: {
        elite: '>= 9.0 (Dynamic sprinter / rapid recovery pace)',
        good: '6.5 - 8.9 (Active runner)',
        average: '4.0 - 6.4 (Standard sprint volume)',
      },
      fmSource: 'Sprints/90 from FM24 export',
      tags: ['Pace', 'Sprints', 'Athleticism', 'Physical'],
    },
    {
      id: 'fouls-won-net',
      name: 'Net Fouls Drawn per 90',
      code: 'Net Fouls/90',
      category: 'defending',
      formula: '\\text{Net Fouls Drawn} = \\text{Fouls Against/90} - \\text{Fouls Made/90}',
      shortDefinition: 'Net margin of free kicks won for your team versus free kicks conceded per 90 minutes.',
      tacticalMeaning:
        'Measures tactical street-smarts and ball-shielding. Tricky wingers and target men win fouls to relieve defensive pressure and earn set pieces.',
      moneyballSignificance:
        'Positive net fouls generate set-piece scoring opportunities while running down the clock when protecting a lead.',
      benchmarks: {
        elite: '>= +1.5 (Elite foul-drawing magnet)',
        good: '+0.5 to +1.4 (Net positive foul winner)',
        average: '-0.5 to +0.4 (Neutral disciplinary balance)',
        poor: '< -1.2 (Concedes excessive fouls)',
      },
      fmSource: 'Derived from Fouls and Fouled in gold mart',
      tags: ['Discipline', 'Fouls', 'Game Management', 'Physical'],
    },
    {
      id: 'discipline-index',
      name: 'Disciplinary Penalty Index',
      code: 'Discipline Idx',
      category: 'defending',
      formula: '\\text{Discipline Index} = \\text{Fouls/90} + (3.0 \\times \\text{Yellows/90}) + (8.0 \\times \\text{Reds/90})',
      shortDefinition: 'Weighted disciplinary penalty index measuring suspension risk and liability from cards and fouls.',
      tacticalMeaning:
        'Identifies reckless tacklers prone to match-losing red cards and multi-match accumulation suspensions.',
      moneyballSignificance:
        'A high-performing defender who misses 6 games per season through suspensions damages squad reliability and squad chemistry.',
      benchmarks: {
        elite: '<= 1.5 (Clean, disciplined defender - lower is better)',
        good: '1.6 - 2.8 (Normal card exposure)',
        average: '2.9 - 4.5 (Moderate disciplinary caution)',
        poor: '> 5.0 (Severe red-card liability)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Cards', 'Discipline', 'Risk', 'Suspensions'],
    },
    {
      id: 'cards-per-foul',
      name: 'Disciplinary Severity (Cards per Foul)',
      code: 'Cards / Foul',
      category: 'defending',
      formula: '\\text{Cards per Foul} = \\frac{\\text{Yellow Cards/90} + \\text{Red Cards/90}}{\\text{Fouls Made/90}}',
      shortDefinition: 'The ratio of cards received relative to total fouls committed.',
      tacticalMeaning:
        'Separates tactical "smart fouls" that break up counters without receiving cards from reckless, dangerous challenges punished immediately.',
      moneyballSignificance:
        'Smart tactical foulers stop counterattacks without receiving yellow cards early in games.',
      benchmarks: {
        elite: '<= 0.10 (Masters of smart tactical fouls)',
        good: '0.11 - 0.20 (Standard referee caution rate)',
        poor: '> 0.30 (High card severity per foul)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Discipline', 'Fouls', 'Referees'],
    },
    {
      id: 'weighted-def-actions',
      name: 'Weighted Defensive Actions (Centerback Priority)',
      code: 'W-Def Actions',
      category: 'defending',
      formula: '\\text{W-Def Actions} = (\\text{Int} \\times 0.50) + (\\text{Clr} \\times 0.25) + ((\\text{Blk} + \\text{Shots Blk}) \\times 0.25)',
      shortDefinition: 'Positional defense score weighting anticipatory interceptions (50%) above reactive clearances (25%) and blocks (25%).',
      tacticalMeaning:
        'Developed specifically for Central Defender reports to reward defenders who read play over defenders who panic and clear.',
      moneyballSignificance:
        'Isolates elite Ball-Playing Defenders who defend proactively on the front foot.',
      benchmarks: {
        elite: '>= 3.8 (Elite proactive modern centerback)',
        good: '2.6 - 3.7 (Solid defensive weighting)',
        average: '< 2.5 (Standard defensive intervention)',
      },
      fmSource: 'Derived in gold.vw_squad_central_defender_detailed_reports',
      tags: ['Centerback', 'Positional', 'Intelligence', 'Defending'],
    },

    // =========================================================================
    // 4. GOALKEEPER METRICS
    // =========================================================================
    {
      id: 'shot-stopping-delta',
      name: 'Shot-Stopping Delta (Save% - xSave%)',
      code: 'Shot Stop Delta',
      category: 'goalkeeper',
      formula: '\\text{Shot-Stopping Delta} = \\text{Actual Save \\%} - \\text{Expected Save \\% (xSave \\%)}',
      shortDefinition: 'The premier modern metric isolating goalkeeper shot-stopping ability from the quality of the defense in front of them.',
      tacticalMeaning:
        'If a goalkeeper faces high-quality shots and saves 80% when the model expected 72%, their delta is +8.0% (elite shot stopper). A positive delta proves the goalkeeper prevents goals beyond expectation.',
      moneyballSignificance:
        'Goals Conceded per 90 is mostly a reflection of defense strength, not goalkeeper talent. Shot-Stopping Delta isolates true individual shot-stopping talent.',
      benchmarks: {
        elite: '>= +5.0% (World-class match-winning shot stopper)',
        good: '+1.5% to +4.9% (Above-average keeper)',
        average: '-1.5% to +1.4% (Neutral, saves what is expected)',
        poor: '< -3.0% (Shot-stopping liability, conceding saveable goals)',
      },
      fmSource: 'Sv % and xSave % from team_gks.csv / interested_gks.csv',
      tags: ['Goalkeeper', 'Shot Stopping', 'Save %', 'xSave %'],
    },
    {
      id: 'shot-stopping-delta-shrunk',
      name: 'Shrunk Shot-Stopping Delta (Empirical Bayes)',
      code: 'Shot Stop Delta (Shrunk)',
      category: 'goalkeeper',
      formula: '\\text{Delta}_{\\text{shrunk}} = \\frac{\\text{Shots Faced Total} \\times \\text{Shot Stop Delta}}{\\text{Shots Faced Total} + 300.0}',
      shortDefinition: 'Shot-stopping delta regressed toward zero based on sample size of shots faced (prior weight k=300).',
      tacticalMeaning:
        'Goalkeepers with small samples (e.g. backup keepers with 5 appearances) frequently show massive +12% deltas that disappear over a full season. Shrinkage normalizes sample size.',
      moneyballSignificance:
        'Guarantees you do not overpay for backup goalkeepers riding short-term hot streaks.',
      benchmarks: {
        elite: '>= +3.0% (Proven elite shot stopper over large sample)',
        good: '+1.0% to +2.9% (Consistent shot-stopping value)',
        average: '-0.9% to +0.9% (Average keeper)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Bayesian', 'Shrinkage', 'Shot Stopping', 'Goalkeeper'],
    },
    {
      id: 'xg-prevented-per90',
      name: 'Expected Goals Prevented per 90',
      code: 'xG Prev/90',
      category: 'goalkeeper',
      formula: '\\text{xG Prevented/90} = \\text{Post-Shot } xG \\text{ Faced/90} - \\text{Goals Conceded/90}',
      shortDefinition: 'The net number of goals prevented per 90 minutes beyond what an average goalkeeper would have conceded.',
      tacticalMeaning:
        'A goalkeeper with +0.30 xG Prevented/90 saves their team approximately 11 to 12 extra goals over a 38-game league campaign.',
      moneyballSignificance:
        'Quantifies the exact points-per-season contribution of the goalkeeper in the transfer market.',
      benchmarks: {
        elite: '>= +0.25 (Saves ~10+ extra goals per season)',
        good: '+0.10 to +0.24 (Consistent value-add)',
        average: '-0.09 to +0.09 (Average net impact)',
        poor: '< -0.15 (Leaks extra goals)',
      },
      fmSource: 'xG Prevented/90 column from FM24 export',
      tags: ['Goalkeeper', 'Goals Prevented', 'Moneyball'],
    },
    {
      id: 'xgp-per-shot',
      name: 'Goals Prevented per Shot Faced',
      code: 'xGP / Shot',
      category: 'goalkeeper',
      formula: '\\text{xGP / Shot} = \\frac{\\text{xGoals Prevented/90}}{\\text{Shots Faced/90}}',
      shortDefinition: 'The average fraction of a goal prevented on every individual shot on target faced.',
      tacticalMeaning:
        'Normalizes shot prevention by workload. A keeper facing 2 shots per game can now be compared fairly against a keeper facing 7 shots per game.',
      moneyballSignificance:
        'Isolates pure shot-stopping quality regardless of whether the goalkeeper plays for a dominant team or a relegation-battler.',
      benchmarks: {
        elite: '>= +0.06 (Elite shot-stopping efficiency per shot)',
        good: '+0.02 to +0.05 (Above-average keeper)',
        average: '-0.01 to +0.01 (Standard expectancy)',
        poor: '< -0.03 (Underperforming on each shot)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Shot Stopping', 'Efficiency', 'Goalkeeper'],
    },
    {
      id: 'xgp-per-shot-shrunk',
      name: 'Shrunk Goals Prevented per Shot (Bayes k=300)',
      code: 'xGP/Shot (Shrunk)',
      category: 'goalkeeper',
      formula: '\\text{xGP/Shot}_{\\text{shrunk}} = \\frac{\\text{Shots Faced Total} \\times \\text{xGP/Shot} + 300 \\times 0.0}{\\text{Shots Faced Total} + 300}',
      shortDefinition: 'Goalkeeper shot-stopping efficiency shrunk toward zero based on total career/season shot exposure.',
      tacticalMeaning:
        'The gold standard metric used in the goalkeeper screener and radar chart. Provides Bayesian certainty that a high-ranking keeper is genuinely elite.',
      moneyballSignificance:
        'The primary sorting metric for goalkeeper recruitment.',
      benchmarks: {
        elite: '>= +0.035 (Proven world-class shot stopper)',
        good: '+0.015 to +0.034 (Reliable starting keeper)',
        average: '-0.010 to +0.014 (Solid mid-table keeper)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Bayesian', 'Recruitment', 'Shot Stopping', 'Goalkeeper'],
    },
    {
      id: 'goals-vs-xgot-ratio',
      name: 'Goals Conceded to xGOT Faced Ratio',
      code: 'Goals / xGOT',
      category: 'goalkeeper',
      formula: '\\text{Goals / xGOT Ratio} = \\frac{\\text{Goals Conceded/90}}{\\text{xGOT Faced/90}}',
      shortDefinition: 'The ratio of actual goals allowed relative to post-shot expected goals on target faced.',
      tacticalMeaning:
        'Values below 1.0 indicate the keeper allowed fewer goals than shot trajectory warranted (e.g. 0.80 means 20% fewer goals conceded); values above 1.0 indicate concessions above expectation.',
      moneyballSignificance:
        'Intuitive multiplier for scouts: a keeper at 0.75 is outperforming expectation by 25%.',
      benchmarks: {
        elite: '<= 0.82 (Concedes 18%+ fewer goals than expected)',
        good: '0.83 - 0.95 (Positive shot stopping)',
        average: '0.96 - 1.04 (Saves exactly expected goals)',
        poor: '> 1.10 (Concedes 10%+ more goals than expected)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Shot Stopping', 'Ratio', 'Goalkeeper'],
    },
    {
      id: 'xgp-per-season',
      name: 'Projected Goals Saved per Season',
      code: 'xGP / Season',
      category: 'goalkeeper',
      formula: '\\text{xGP / Season} = \\text{xGoals Prevented/90} \\times 38.0',
      shortDefinition: 'The projected cumulative number of goals saved above expectation over a full 38-game league campaign.',
      tacticalMeaning:
        'Translates abstract per-90 rates into real-world league points: saving 8 extra goals over a season typically translates to 6 to 9 additional league table points.',
      moneyballSignificance:
        'Quantifies the return on investment when signing an elite goalkeeper.',
      benchmarks: {
        elite: '>= +8.0 goals (Translates to ~6-9 extra league points)',
        good: '+3.5 to +7.9 goals (Solid value-add)',
        average: '-2.0 to +3.4 goals (Neutral league impact)',
        poor: '< -4.0 goals (Costs points over season)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Season', 'Points', 'Impact', 'Goalkeeper'],
    },
    {
      id: 'saves-percentage',
      name: 'Goalkeeper Save Percentage %',
      code: 'Save %',
      category: 'goalkeeper',
      formula: '\\text{Save \\%} = \\frac{\\text{Saves Made}}{\\text{Shots on Target Faced}} \\times 100',
      shortDefinition: 'The raw percentage of all on-target shots faced that are successfully saved.',
      tacticalMeaning:
        'The classic baseline metric. While raw save % does not adjust for shot quality, elite keepers rarely drop below 72% over multi-season samples.',
      moneyballSignificance:
        'Always evaluate alongside shot difficulty (xGOT/Shot): a keeper with 78% save rate facing easy long shots may be worse than a keeper with 73% facing point-blank tap-ins.',
      benchmarks: {
        elite: '>= 76.0% (Elite shot retention)',
        good: '71.0% - 75.9% (Reliable starting keeper)',
        average: '65.0% - 70.9% (Average top-flight level)',
        poor: '< 64.0% (Low save rate)',
      },
      fmSource: 'Sv % from FM24 export',
      tags: ['Saves', 'Volume', 'Goalkeeper'],
    },
    {
      id: 'shot-difficulty',
      name: 'Shot Difficulty Faced (xGOT / Shot)',
      code: 'Shot Difficulty',
      category: 'goalkeeper',
      formula: '\\text{Shot Difficulty} = \\frac{\\text{xGOT Faced/90}}{\\text{Shots Faced/90}}',
      shortDefinition: 'The average post-shot expected goal difficulty of each shot on target faced by the goalkeeper.',
      tacticalMeaning:
        'Measures how well the defense protects the goalkeeper. High values (>0.36) mean the defense allows opponent strikers into point-blank 1v1 positions; low values (<0.28) mean the defense restricts opponents to harmless long shots.',
      moneyballSignificance:
        'Vital contextual equalizer: excuses keepers playing behind porous, wide-open defensive backlines.',
      benchmarks: {
        elite: '<= 0.28 (Defense forces low-danger shots - lower is easier)',
        average: '0.29 - 0.35 (Standard defensive concessions)',
        poor: '>= 0.36 (Goalkeeper faces high-difficulty point-blank shots)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Workload', 'Context', 'Defense', 'Goalkeeper'],
    },
    {
      id: 'shots-faced-total',
      name: 'Total Shots Faced (Sample Exposure)',
      code: 'Shots Faced Total',
      category: 'goalkeeper',
      formula: '\\text{Shots Faced Total} = \\text{Shots Faced/90} \\times \\left(\\frac{\\text{Minutes}}{90.0}\\right)',
      shortDefinition: 'The cumulative total number of on-target shots faced across the competitive season.',
      tacticalMeaning:
        'Determines sample reliability. In the warehouse, goalkeepers with >=40 shots faced are flagged as `is_eligible = 1` for recruitment consideration.',
      moneyballSignificance:
        'Forms the mathematical denominator for Bayesian shrinkage models ($k=300$).',
      benchmarks: {
        elite: '>= 80 shots (High sample confidence)',
        good: '40 - 79 shots (Qualified for recruitment screening)',
        poor: '< 40 shots (Unreliable low sample size)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Sample Size', 'Reliability', 'Goalkeeper'],
    },
    {
      id: 'shots-faced-per90',
      name: 'Shots Faced per 90',
      code: 'Shots Faced/90',
      category: 'goalkeeper',
      formula: '\\text{Shots Faced/90} = \\text{Saves Made/90} + \\text{Goals Conceded/90}',
      shortDefinition: 'The average volume of on-target shots the goalkeeper must deal with per 90 minutes.',
      tacticalMeaning:
        'Indicates match engagement and defensive workload. Keepers facing 6+ shots per match must maintain acute concentration throughout.',
      moneyballSignificance:
        'Keepers facing high shot volumes can show inflated raw save counts that mask technical flaws.',
      benchmarks: {
        elite: '<= 3.0 (Protected by dominant defense)',
        average: '3.1 - 4.8 (Standard workload)',
        poor: '>= 5.0 (Heavy defensive exposure)',
      },
      fmSource: 'Derived from Saves/90 and Gls Con/90',
      tags: ['Workload', 'Exposure', 'Goalkeeper'],
    },
    {
      id: 'xgot-faced-per90',
      name: 'Post-Shot xG (xGOT) Faced per 90',
      code: 'xGOT Faced/90',
      category: 'goalkeeper',
      formula: '\\text{xGOT Faced/90} = \\text{xGoals Prevented/90} + \\text{Goals Conceded/90}',
      shortDefinition: 'The total post-shot expected goal quality of shots on target faced per 90 minutes.',
      tacticalMeaning:
        'Unlike pre-shot xG, xGOT measures shot placement and speed after leaving the striker’s boot. A shot into the top corner has high xGOT; a shot straight at the keeper has low xGOT.',
      moneyballSignificance:
        'The true metric of shot threat faced by the goalkeeper.',
      benchmarks: {
        elite: '<= 0.90 (Low danger conceded by defense)',
        average: '0.91 - 1.40 (Standard top-flight level)',
        poor: '>= 1.50 (Extreme goal threat faced)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['xGOT', 'Threat', 'Goalkeeper'],
    },
    {
      id: 'parry-rate',
      name: 'Parry Rebound Risk Rate %',
      code: 'Parry Rate %',
      category: 'goalkeeper',
      formula: '\\text{Parry Rate \\%} = \\frac{\\text{Saves Parried/90}}{\\text{Total Saves Made/90}} \\times 100',
      shortDefinition: 'The percentage of saved shots that are parried back into play rather than caught or tipped safely out.',
      tacticalMeaning:
        'Crucial handling risk metric: parrying shots back into the 6-yard box creates chaos and secondary rebound tap-ins for opposing strikers.',
      moneyballSignificance:
        'A goalkeeper with great reflexes but a 65% parry rate will concede preventable secondary rebound goals.',
      benchmarks: {
        elite: '<= 40.0% (Safe handling, rarely concedes rebounds - lower is better)',
        good: '40.1% - 52.0% (Acceptable parry control)',
        average: '52.1% - 62.0% (Moderate rebound risk)',
        poor: '> 65.0% (High rebound risk / parries into danger)',
      },
      fmSource: 'Derived from Saves Parried/90 and Saves/90',
      tags: ['Handling', 'Rebounds', 'Risk', 'Goalkeeper'],
    },
    {
      id: 'hold-rate',
      name: 'Goalkeeper Catch & Hold Rate %',
      code: 'Hold Rate %',
      category: 'goalkeeper',
      formula: '\\text{Hold Rate \\%} = \\frac{\\text{Shots Held/90}}{\\text{Total Saves/90}} \\times 100',
      shortDefinition: 'The percentage of saved shots that are cleanly caught and held rather than parried.',
      tacticalMeaning:
        'Catching the ball terminates the opposition’s attack immediately. High hold rates prevent dangerous secondary rebound tap-ins.',
      moneyballSignificance:
        'Underappreciated skill: a keeper with high saves but low hold rate creates chaos inside their own 6-yard box by giving away rebounds.',
      benchmarks: {
        elite: '>= 48.0% (Elite handling and rebound prevention)',
        good: '38.0% - 47.9% (Reliable hands)',
        average: '28.0% - 37.9% (Standard handling)',
        poor: '< 25.0% (Prone to parrying into danger zones)',
      },
      fmSource: 'Held/90 and Saves/90 from FM24 export',
      tags: ['Handling', 'Rebounds', 'Catching', 'Goalkeeper'],
    },
    {
      id: 'hold-rate-shrunk',
      name: 'Shrunk Hold Rate % (Bayes k=150)',
      code: 'Hold Rate (Shrunk)',
      category: 'goalkeeper',
      formula: '\\text{Hold Rate}_{\\text{shrunk}} = \\frac{\\text{Total Saves} \\times \\text{Hold Rate} + 150 \\times 0.30}{\\text{Total Saves} + 150}',
      shortDefinition: 'Goalkeeper catch and hold percentage regressed toward the global positional baseline (30%) with sample weight k=150.',
      tacticalMeaning:
        'Prevents small-sample handling distortions, ensuring high hold rates are confirmed over extended shot volumes.',
      moneyballSignificance:
        'Eliminates transfer mistakes caused by small-sample handling statistics.',
      benchmarks: {
        elite: '>= 40.0% (Confirmed elite handling over sample)',
        good: '32.0% - 39.9% (Solid catching control)',
        average: '27.0% - 31.9% (Average hands)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Bayesian', 'Handling', 'Goalkeeper'],
    },
    {
      id: 'hold-rate-over-expected',
      name: 'Hold Rate Over Expected (Difficulty-Adjusted)',
      code: 'Hold Rate OE',
      category: 'goalkeeper',
      formula: '\\text{Hold Rate OE} = \\text{Hold Rate} - (0.1143 + 0.5230 \\times \\text{Shot Difficulty})',
      shortDefinition: 'Residual handling efficiency adjusted for the incoming difficulty and velocity of shots faced.',
      tacticalMeaning:
        'Catching a 30-yard speculative shot is expected; catching a 12-yard blast requires world-class technique. Hold Rate OE gives full credit for catching difficult shots.',
      moneyballSignificance:
        'The truest mathematical measure of pure handling skill in modern football analytics.',
      benchmarks: {
        elite: '>= +0.060 (Catches difficult, high-velocity shots)',
        good: '+0.015 to +0.059 (Above-average handling)',
        average: '-0.020 to +0.014 (Expected catching level)',
        poor: '< -0.040 (Parries catchable shots)',
      },
      fmSource: 'Regression model derived in gold.mart_gk_scouting_summary',
      tags: ['Handling', 'Advanced', 'Goalkeeper'],
    },
    {
      id: 'sweeper-actions-per90',
      name: 'Sweeper Actions per 90',
      code: 'Sweeper Act/90',
      category: 'goalkeeper',
      formula: '\\text{Sweeper Actions/90} = \\text{Interceptions/90} + \\text{Clearances/90}',
      shortDefinition: 'Aggressive defensive interventions executed outside the 6-yard box and penalty area per 90 minutes.',
      tacticalMeaning:
        'Crucial for teams playing a high defensive line. Evaluates whether the goalkeeper rushes out to smother through-balls before opponent strikers can reach them.',
      moneyballSignificance:
        'Essential for modern high-line tactical frameworks (Pep Guardiola, Jurgen Klopp, Roberto De Zerbi).',
      benchmarks: {
        elite: '>= 2.5 (Proactive Sweeper Keeper / high-line safety net)',
        good: '1.6 - 2.4 (Active off line)',
        average: '0.8 - 1.5 (Moderate sweeping activity)',
        poor: '< 0.7 (Traditional goal-line keeper)',
      },
      fmSource: 'Derived from Int/90 and Clr/90 in gold mart',
      tags: ['Sweeper Keeper', 'High Line', 'Goalkeeper'],
    },
    {
      id: 'sweeper-share',
      name: 'Proactive Sweeper Share %',
      code: 'Sweeper Share %',
      category: 'goalkeeper',
      formula: '\\text{Sweeper Share \\%} = \\frac{\\text{Interceptions/90}}{\\text{Interceptions/90} + \\text{Clearances/90}} \\times 100',
      shortDefinition: 'The proportion of sweeping actions that are clean proactive interceptions rather than emergency clearances under pressure.',
      tacticalMeaning:
        'High share indicates the goalkeeper reads the flight of the ball early and intercepts comfortably; low share indicates frantic, late rushing.',
      moneyballSignificance:
        'Clean interceptions allow the keeper to immediately distribute the ball forward into a counter-attack.',
      benchmarks: {
        elite: '>= 20.0% (Clean, anticipatory sweeper)',
        good: '10.0% - 19.9% (Active lane interceptor)',
        average: '< 10.0% (Reactive clearance sweeper)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Sweeper Keeper', 'Anticipation', 'Goalkeeper'],
    },
    {
      id: 'gk-completed-passes-per90',
      name: 'Goalkeeper Distribution Completed Passes per 90',
      code: 'GK Cmp Passes/90',
      category: 'goalkeeper',
      formula: '\\text{Completed Passes/90} = \\text{Passes Attempted/90} \\times \\text{Pass Accuracy}',
      shortDefinition: 'The volume of successful passes completed per 90 minutes by the goalkeeper.',
      tacticalMeaning:
        'Measures active involvement as the "11th outfield player" in build-up play. Essential for beating opponent high presses.',
      moneyballSignificance:
        'Recruiters in possession systems require goalkeepers completing >=15 passes per 90.',
      benchmarks: {
        elite: '>= 18.0 (Modern ball-playing goalkeeper / build-up hub)',
        good: '13.0 - 17.9 (Active distributor)',
        average: '8.0 - 12.9 (Traditional distribution)',
        poor: '< 7.0 (Long-ball clearance keeper)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Distribution', 'Buildup', 'Ball-Playing Keeper', 'Goalkeeper'],
    },
    {
      id: 'gk-pass-accuracy',
      name: 'Goalkeeper Pass Accuracy %',
      code: 'GK Pass Acc %',
      category: 'goalkeeper',
      formula: '\\text{Pass Accuracy \\%} = \\frac{\\text{Completed Passes}}{\\text{Attempted Passes}} \\times 100',
      shortDefinition: 'The percentage of goalkeeper distribution passes that successfully reach a teammate.',
      tacticalMeaning:
        'Evaluates distribution composure: keepers playing short ground passes have 75-85% accuracy; long hoofers operate at 45-55%.',
      moneyballSignificance:
        'Misplaced goalkeeper passes inside the defensive third lead to immediate high-xG concessions.',
      benchmarks: {
        elite: '>= 75.0% (Clean, accurate short distributor)',
        good: '65.0% - 74.9% (Solid passing accuracy)',
        average: '50.0% - 64.9% (Mixed long/short distribution)',
        poor: '< 48.0% (Frequent direct turnovers)',
      },
      fmSource: 'Pas % from FM24 export',
      tags: ['Accuracy', 'Distribution', 'Goalkeeper'],
    },
    {
      id: 'giveaways-per90',
      name: 'Giveaways per 90 (Lost Buildup)',
      code: 'Giveaways/90',
      category: 'goalkeeper',
      formula: '\\text{Giveaways/90} = \\text{Passes Attempted/90} \\times (1.0 - \\text{Pass Accuracy})',
      shortDefinition: 'The volume of incomplete passes and surrendered possessions per 90 during goalkeeper distribution.',
      tacticalMeaning:
        'Measures distribution safety. Keepers who repeatedly kick the ball directly to opposing midfielders invite constant pressure.',
      moneyballSignificance:
        'High giveaways directly correlate with high defensive concession pressure in your defensive half.',
      benchmarks: {
        elite: '<= 4.5 (Ultra-secure distribution - fewer is better)',
        good: '4.6 - 7.5 (Solid distribution security)',
        average: '7.6 - 11.0 (Normal long-ball turnover volume)',
        poor: '> 12.0 (High turnover risk in buildup)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Turnovers', 'Security', 'Goalkeeper'],
    },
    {
      id: 'pen-save-shrunk',
      name: 'Shrunk Penalty Save % (Bayes k=20)',
      code: 'Pen Save % (Shrunk)',
      category: 'goalkeeper',
      formula: '\\text{Pen Save \\%}_{\\text{shrunk}} = \\frac{\\text{Pens Faced} \\times \\text{Pen Save \\%} + 20 \\times 0.20}{\\text{Pens Faced} + 20} \\times 100',
      shortDefinition: 'Penalty kick save percentage regressed toward the historical league mean (20%) with Bayesian prior weight k=20.',
      tacticalMeaning:
        'A goalkeeper who saves 2 out of 2 penalties has a 100% raw save rate, but this is a tiny sample. Bayesian shrinkage pulls them to an honest ~27% projection.',
      moneyballSignificance:
        'Isolates genuine penalty-saving specialists for cup competitions and penalty shootouts.',
      benchmarks: {
        elite: '>= 28.0% (Proven penalty specialist)',
        good: '22.0% - 27.9% (Above-average penalty stopper)',
        average: '18.0% - 21.9% (Standard penalty baseline)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Penalties', 'Bayesian', 'Specialist', 'Goalkeeper'],
    },
    {
      id: 'sample-reliability',
      name: 'Goalkeeper Sample Reliability Factor',
      code: 'Sample Reliability',
      category: 'goalkeeper',
      formula: '\\text{Sample Reliability} = \\frac{\\text{Shots Faced Total}}{\\text{Shots Faced Total} + 300.0}',
      shortDefinition: 'Bayesian confidence coefficient (0.0 to 1.0) measuring statistical reliability based on total shots on target faced.',
      tacticalMeaning:
        'Ranges from 0.0 (no confidence, zero sample) to 1.0 (absolute confidence, massive multi-season sample). Used to weight projected performance.',
      moneyballSignificance:
        'Multiplies seasonal goals saved to discount small-sample backup goalkeepers.',
      benchmarks: {
        elite: '>= 0.40 (High-confidence statistical sample)',
        good: '0.25 - 0.39 (Sufficient sample for recruitment)',
        poor: '< 0.20 (Low statistical confidence)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Bayesian', 'Confidence', 'Sample Size', 'Goalkeeper'],
    },
    {
      id: 'gk-errors-leading-to-goal',
      name: 'Errors Leading to Opponent Goal per 90',
      code: 'Errors/90',
      category: 'goalkeeper',
      formula: '\\text{Errors/90} = \\frac{\\text{Mistakes Leading to Goals}}{\\text{Minutes Played} / 90.0}',
      shortDefinition: 'Unforced individual blunders directly resulting in an opponent goal per 90 minutes.',
      tacticalMeaning:
        'Reflects mental composure and error-proneness. Even an elite shot-stopper who commits 3-4 catastrophic blunders per season wipes out their positive goals prevented impact.',
      moneyballSignificance:
        'Disqualifying metric: never invest heavily in error-prone goalkeepers.',
      benchmarks: {
        elite: '0.00 (Flawless, blunder-free campaign)',
        good: '<= 0.03 (Very low error frequency)',
        poor: '>= 0.08 (Error-prone liability)',
      },
      fmSource: 'Derived in gold.mart_gk_scouting_summary',
      tags: ['Errors', 'Composure', 'Risk', 'Goalkeeper'],
    },

    // =========================================================================
    // 5. MONEYBALL & VALUATION
    // =========================================================================
    {
      id: 'cost-per-xgi',
      name: 'Cost per Expected Goal Involvement (£k)',
      code: 'Cost per xGI',
      category: 'valuation',
      formula: '\\text{Cost per xGI} = \\frac{(\\text{Weekly Wage (£k)} \\times 52) + (\\text{Transfer Fee (£k)} / 3)}{\\text{Annual Expected Goal Involvement}}',
      shortDefinition: 'The annualized financial investment (annual salary + amortized fee) required for each unit of expected goal contribution delivered.',
      tacticalMeaning:
        'The centerpiece metric of the Moneyball engine. Identifies undervalued statistical phenoms playing in secondary leagues whose output matches superstars at a fraction of the cost.',
      moneyballSignificance:
        'If Star A costs £15k per xGI and Star B costs £120k per xGI with identical tactical profiles, buying Star A saves millions to reinvest in squad depth.',
      benchmarks: {
        elite: '<= £8k / xGI (Tremendous Moneyball bargain)',
        good: '£9k - £25k / xGI (High-value starter)',
        average: '£26k - £60k / xGI (Standard market price)',
        poor: '> £80k / xGI (Overpriced luxury asset)',
      },
      fmSource: 'Derived from Transfer Value, Wage, and NPxG+xA',
      tags: ['Moneyball', 'Bargains', 'Finance', 'Recruitment', 'Valuation'],
    },
    {
      id: 'xgi-per-cost-m',
      name: 'Expected Goal Involvement Output per £M Annual Cost',
      code: 'xGI / £M',
      category: 'valuation',
      formula: '\\text{xGI / £M} = \\frac{\\text{Annual Expected Goal Involvement}}{\\text{Annualized Wage} + (\\text{Fee} / 3.0)}',
      shortDefinition: 'The units of expected goal involvement delivered per £1,000,000 of annualized club expenditure.',
      tacticalMeaning:
        'The inverse of Cost per xGI. Measures capital efficiency: how much goal threat your budget purchases.',
      moneyballSignificance:
        'Essential for budget-constrained clubs competing in top-flight leagues against oil-funded giants.',
      benchmarks: {
        elite: '>= 60.0 (Massive offensive production per £M spent)',
        good: '35.0 - 59.9 (High-value squad contributor)',
        average: '15.0 - 34.9 (Fair market efficiency)',
        poor: '< 12.0 (Low return on investment)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Capital Efficiency', 'ROI', 'Moneyball', 'Valuation'],
    },
    {
      id: 'value-gap',
      name: 'Value Gap Index (Moneyball Percentile Gap)',
      code: 'Value Gap',
      category: 'valuation',
      formula: '\\text{Value Gap} = \\text{PERCENT\\_RANK}(\\text{Output}) - \\text{PERCENT\\_RANK}(\\text{Transfer Value (£M)})',
      shortDefinition: 'The percentile spread between on-pitch statistical output and market transfer valuation (ranging from -100 to +100).',
      tacticalMeaning:
        'A Value Gap of +50 means the player is in the 85th percentile for performance output but only the 35th percentile for transfer value. They are severely underpriced by the market.',
      moneyballSignificance:
        'The ultimate 1-click filter for uncovering hidden gems playing in undervalued clubs, smaller leagues, or nearing contract expiration.',
      benchmarks: {
        elite: '>= +45.0 (Extreme market inefficiency / immediate buy target)',
        good: '+25.0 to +44.9 (High-value transfer candidate)',
        average: '-15.0 to +24.9 (Fair market valuation)',
        poor: '< -25.0 (Heavily overpriced asset / sell immediately)',
      },
      fmSource: 'Computed across gold schema scouting summary marts',
      tags: ['Value Gap', 'Arbitrage', 'Bargains', 'Moneyball', 'Valuation'],
    },
    {
      id: 'bargain-indices',
      name: 'Attacking & Defensive Bargain Indices',
      code: 'Bargain Index',
      category: 'valuation',
      formula: '\\text{Bargain Index} = \\frac{\\text{Percentile Rank Output}}{\\text{Normalized Wage Tier} \\times \\text{Normalized Value Tier}}',
      shortDefinition: 'Ratio of on-pitch statistical dominance relative to salary and transfer market valuation tier.',
      tacticalMeaning:
        'Automatically surfaced in `gold.vw_bargain_watch` to highlight players in lower divisions or unheralded clubs whose statistical percentiles are elite.',
      moneyballSignificance:
        'Enables mid-table and lower-league clubs with limited budgets to punch above their financial weight class.',
      benchmarks: {
        elite: '>= 2.5 (Extreme value-to-cost asymmetry)',
        good: '1.5 - 2.4 (Good value)',
        average: '< 1.0 (Fair market price or overvalued)',
      },
      fmSource: 'Calculated in gold.vw_bargain_watch',
      tags: ['Bargains', 'Value', 'Scouting', 'Valuation'],
    },
    {
      id: 'transfer-value-m',
      name: 'Market Transfer Valuation (£M)',
      code: 'Valuation (£M)',
      category: 'valuation',
      formula: '\\text{Estimated Transfer Value in Millions (£M)}',
      shortDefinition: 'Estimated financial acquisition cost derived from Football Manager market valuation ranges.',
      tacticalMeaning:
        'Reflects reputation, league stature, age, and contract length rather than pure statistical output.',
      moneyballSignificance:
        'The denominator in all Moneyball valuation models.',
      benchmarks: {
        elite: '<= £15.0M for elite output (High-value recruitment zone)',
        good: '£15.1M - £35.0M (Competitive market price)',
        poor: '> £60.0M (Superstar premium / low efficiency)',
      },
      fmSource: 'Parsed from Value string in bronze to silver and gold.fact_players_value',
      tags: ['Valuation', 'Transfer Fee', 'Finance', 'Valuation'],
    },
    {
      id: 'wage-per-week-k',
      name: 'Contractual Weekly Wage (£k)',
      code: 'Wage (£k/wk)',
      category: 'valuation',
      formula: '\\text{Contractual Basic Weekly Wage in Thousands (£k/wk)}',
      shortDefinition: 'The ongoing weekly payroll obligation committed to the player.',
      tacticalMeaning:
        'Directly impacts club financial health and squad wage hierarchy structure.',
      moneyballSignificance:
        'Annual wages typically exceed amortized transfer fees over a 4-year contract; controlling wage inflation is vital for long-term sustainability.',
      benchmarks: {
        elite: '<= £20k/wk for starting contributor (Efficient payroll)',
        good: '£21k - £50k/wk (Standard top-flight starter)',
        poor: '> £120k/wk (High financial burden)',
      },
      fmSource: 'Parsed from Wage string in silver to gold schema',
      tags: ['Wage', 'Salary', 'Payroll', 'Finance', 'Valuation'],
    },
    {
      id: 'on-pitch-goal-diff-per90',
      name: 'On-Pitch Net Goal Differential per 90',
      code: 'On-Pitch GD/90',
      category: 'valuation',
      formula: '\\text{On-Pitch GD/90} = \\text{Team Goals Scored/90} - \\text{Team Goals Conceded/90}',
      shortDefinition: 'The net goal differential achieved by the team while this specific player was on the pitch.',
      tacticalMeaning:
        'Football’s equivalent to basketball’s "Plus/Minus (+/-)". Measures team performance when the player is playing.',
      moneyballSignificance:
        'Highlights subtle glue players whose presence dramatically improves team balance even if their personal scoring stats are modest.',
      benchmarks: {
        elite: '>= +1.20 / 90 (Dominant positive team net impact)',
        good: '+0.40 to +1.19 / 90 (Positive team impact)',
        average: '-0.20 to +0.39 / 90 (Neutral team differential)',
        poor: '< -0.50 / 90 (Negative team differential)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Plus-Minus', 'Team Impact', 'Valuation'],
    },
    {
      id: 'on-pitch-gd-shrunk',
      name: 'Shrunk On-Pitch Goal Differential',
      code: 'On-Pitch GD (Shrunk)',
      category: 'valuation',
      formula: '\\text{GD}_{\\text{shrunk}} = (\\text{Team Goals Scored} - \\text{Team Goals Conceded}) \\times \\left(\\frac{\\text{Minutes}}{\\text{Minutes} + 900}\\right)',
      shortDefinition: 'On-pitch goal differential regressed toward zero based on minute sample size.',
      tacticalMeaning:
        'Filters out short-term sample distortions (e.g. substitutes who played 100 minutes during an 8-0 blowout win).',
      moneyballSignificance:
        'Provides statistically robust plus-minus evaluation for recruitment analysis.',
      benchmarks: {
        elite: '>= +0.80 (Proven positive impact over large sample)',
        good: '+0.25 to +0.79 (Consistent winning contributor)',
        poor: '< -0.30 (Negative on-pitch drag)',
      },
      fmSource: 'Derived in gold.mart_outfield_scouting_summary',
      tags: ['Bayesian', 'Plus-Minus', 'Sample Size', 'Valuation'],
    },

    // =========================================================================
    // 6. SIMILARITY, COMPETITION & MATHEMATICAL MODELING
    // =========================================================================
    {
      id: 'euclidean-similarity',
      name: 'Scale-Weighted Euclidean Distance Matcher',
      code: 'Similarity Distance (D)',
      category: 'math',
      formula: 'D(\\mathbf{Target}, \\mathbf{Incumbent}) = \\sqrt{\\sum_{i=1}^n w_i \\cdot \\left(\\text{Target}_i - \\text{Incumbent}_i\\right)^2}',
      shortDefinition: 'Mathematical vector distance matching scouted external targets against your team’s primary starter.',
      tacticalMeaning:
        'Rather than searching for arbitrary attributes, the warehouse takes your incumbent starter’s per-90 statistical fingerprint and searches 11,000+ players for the closest statistical twin.',
      moneyballSignificance:
        'Guarantees seamless tactical replacement when selling an aging star: the new signing will perform the exact same tactical functions in your match engine.',
      benchmarks: {
        elite: 'D <= 1.50 (Statistical twin / near-identical tactical clone)',
        good: 'D 1.51 - 2.80 (Strong profile match)',
        average: 'D 2.81 - 4.50 (Moderate similarity)',
        poor: 'D > 5.00 (Fundamentally different player profile)',
      },
      fmSource: '8 Positional views in gold schema (gold.vw_similar_*)',
      tags: ['Euclidean', 'Similarity', 'Replacement', 'Math', 'Modeling'],
    },
    {
      id: 'profile-match-pct',
      name: 'Tactical Profile Match Percentage',
      code: 'Profile Match %',
      category: 'math',
      formula: '\\text{Profile Match \\%} = \\max\\left(0, 100 - \\frac{D}{D_{\\text{threshold}}} \\times 100\\right)',
      shortDefinition: 'Intuitive 0% to 100% tactical compatibility score derived from Euclidean similarity distance.',
      tacticalMeaning:
        'Translates complex multi-dimensional geometric distance into an intuitive scout match score.',
      moneyballSignificance:
        'Provides instant recruitment shortlist clarity for managers who prefer percentages over mathematical vector distances.',
      benchmarks: {
        elite: '>= 90.0% (Plug-and-play tactical clone)',
        good: '80.0% - 89.9% (Very close fit)',
        average: '70.0% - 79.9% (Usable alternative with minor tactical adaptations)',
        poor: '< 65.0% (Poor tactical alignment)',
      },
      fmSource: 'Computed across all gold.vw_similar_* views',
      tags: ['Match %', 'Similarity', 'Scouting', 'Math'],
    },
    {
      id: 'scd2-architecture',
      name: 'Slowly Changing Dimensions Type 2 (SCD2)',
      code: 'SCD Type 2',
      category: 'math',
      formula: '\\text{Validity: } \\text{dwh\\_current\\_validity} = 1, \\quad [\\text{dwh\\_create\\_date}, \\text{dwh\\_cd\\_valid\\_till}]',
      shortDefinition: 'Enterprise data engineering pattern that preserves historical season snapshots rather than overwriting past records.',
      tacticalMeaning:
        'When you export a new season from FM24, the pipeline retires the old season (`dwh_current_validity = 0`) and inserts the new season as active (`dwh_current_validity = 1`). Past form is never deleted.',
      moneyballSignificance:
        'Powers true longitudinal player development tracking, winter transfer isolation, and multi-season regression models.',
      benchmarks: {
        elite: 'Guaranteed 1 active row per player with complete chronological audit history',
        good: 'Multi-season growth tracking and delta comparisons',
        average: 'Historical season queries via Point-in-Time dimension joins',
      },
      fmSource: 'Built into gold.load_gold ETL procedure and dimension tables',
      tags: ['SCD2', 'Database', 'Engineering', 'History', 'Math'],
    },
    {
      id: 'league-difficulty-coef',
      name: 'League Difficulty Calibration Multiplier',
      code: 'League Coef (x)',
      category: 'math',
      formula: '\\text{Adjusted Metric} = \\text{Raw Metric} \\times \\text{League Difficulty Coef}',
      shortDefinition: 'Competition quality calibration factor scaling raw player statistics according to domestic league tier.',
      tacticalMeaning:
        'Calibrates player output against standard top-flight difficulty: Apex leagues (Premier League, La Liga, Serie A, Bundesliga) = 1.00; SL1 (Championship, Portugal, Netherlands) = 0.85-0.90; SL2 (League One, Belgium, Scotland) = 0.75-0.80; lower tiers scale down to 0.50.',
      moneyballSignificance:
        'Eliminates the common scouting trap of signing a striker who scored 25 goals in a weak league and flops in a premier competition.',
      benchmarks: {
        elite: '1.00x (Apex competition: Premier League, La Liga, Serie A)',
        good: '0.80x - 0.90x (High-caliber tier: Championship, Eredivisie, Liga Portugal)',
        average: '0.65x - 0.75x (Mid-tier developmental leagues)',
        poor: '<= 0.60x (Lower-division competitions)',
      },
      fmSource: 'gold.dim_league reference table in SQL Server',
      tags: ['Competition', 'Normalization', 'Calibration', 'Math'],
    },
    {
      id: 'relative-translation-coef',
      name: 'Relative Translation Multiplier (Squad Calibrated)',
      code: 'Rel Translation (x)',
      category: 'math',
      formula: '\\text{Relative Translation Coef} = \\frac{\\text{Target League Coef}}{\\text{User Club League Coef}}',
      shortDefinition: 'Personalized competition multiplier scaling a scouted player’s output directly relative to the user’s managed club baseline.',
      tacticalMeaning:
        'If managing a League One club (coef 0.70) and scouting a player in the Scottish Premiership (coef 0.75), their stats are translated by (0.75 / 0.70) = 1.07x. Powers the "Adjust to My League" toggle and Ghost Radar outline.',
      moneyballSignificance:
        'Provides tailor-made performance projections specifically calibrated for your club’s exact division.',
      benchmarks: {
        elite: '> 1.10x (Stepping down from a stronger league - expected performance boost)',
        good: '0.95x - 1.05x (Direct lateral league transfer)',
        average: '< 0.90x (Stepping up to a tougher league - expected performance compression)',
      },
      fmSource: 'Computed dynamically in webapp player dossier and screener',
      tags: ['Custom', 'Translation', 'Ghost Radar', 'Math'],
    },
    {
      id: 'dim-team-tactical-archetypes',
      name: 'Team Tactical Archetype Affinity Index (0-100)',
      code: 'Archetype Score',
      category: 'math',
      formula: '\\text{Score} = \\sum_{j=1}^m w_j \\cdot \\text{PERCENT\\_RANK}\\left(\\text{Event Metric}_j\\right) \\times 100',
      shortDefinition: 'Minutes-weighted composite scoring classifying football clubs across 7 distinct tactical philosophies.',
      tacticalMeaning:
        'Aggregates pressures attempted, passes attempted, progressive passing, crossing volume, aerial headers, and defensive blocks into 0-100 affinity scores for Gegenpressing, Tiki-Taka, Vertical Tiki-Taka, Fluid Counter, Direct Counter, Wing Play, and Catenaccio.',
      moneyballSignificance:
        'Ensures strategic recruitment alignment: prevents signing players whose tactical strengths contradict the manager’s playing style.',
      benchmarks: {
        elite: '>= 80 (Undisputed tactical framework purity)',
        good: '65 - 79 (Strong tactical system affinity)',
        average: '45 - 64 (Flexible hybrid tactical system)',
      },
      fmSource: 'gold.dim_team_tactical_archetypes table',
      tags: ['Tactics', 'Archetypes', 'Gegenpress', 'Tiki-Taka', 'Math'],
    },
    {
      id: 'empirical-bayes-shrinkage',
      name: 'Empirical Bayes Multi-Tier Shrinkage Framework',
      code: 'Bayes Shrinkage',
      category: 'math',
      formula: '\\hat{\\theta}_{\\text{shrunk}} = \\frac{n \\cdot \\bar{x} + k \\cdot \\mu_0}{n + k}',
      shortDefinition: 'Universal Bayesian shrinkage framework applied across the warehouse to pull small-sample metrics toward established population priors.',
      tacticalMeaning:
        'Applied across multiple domains: Finishing Delta (prior 0.0, k=900 mins), Assist Luck (prior 0.0, k=900 mins), Goalkeeper xGP/Shot (prior 0.0, k=300 shots), Hold Rate (prior 30%, k=150 saves), Penalty Save % (prior 20%, k=20 pens), Error Rate (prior 0.022, k=5000 mins).',
      moneyballSignificance:
        'The mathematical backbone preventing transfer mistakes caused by small sample sizes and short-term variance.',
      benchmarks: {
        elite: 'Sample n >= 3 * k (Uncompromised signal dominates shrinkage)',
        good: 'Sample n >= k (50%+ signal retention)',
        average: 'Sample n < k (Heavily shrunk toward prior for safety)',
      },
      fmSource: 'Built into SQL Server procedures and gold mart views',
      tags: ['Bayesian', 'Statistics', 'Shrinkage', 'Architecture', 'Math'],
    },
  ];

  // Dynamic Category Configuration
  const categories = useMemo(() => {
    return [
      { id: 'all', label: 'All Knowledge', icon: Layers, count: metrics.length + graphs.length },
      { id: 'graphs', label: 'Visual Graphs & Charts', icon: BarChart3, count: graphs.length },
      { id: 'attacking', label: 'Attacking & Shooting', icon: Target, count: metrics.filter((m) => m.category === 'attacking').length },
      { id: 'passing', label: 'Passing & Creation', icon: Zap, count: metrics.filter((m) => m.category === 'passing').length },
      { id: 'defending', label: 'Defending & Duels', icon: Shield, count: metrics.filter((m) => m.category === 'defending').length },
      { id: 'goalkeeper', label: 'Goalkeeping', icon: Activity, count: metrics.filter((m) => m.category === 'goalkeeper').length },
      { id: 'valuation', label: 'Moneyball & Valuation', icon: Coins, count: metrics.filter((m) => m.category === 'valuation').length },
      { id: 'math', label: 'Similarity & Modeling', icon: Calculator, count: metrics.filter((m) => m.category === 'math').length },
    ];
  }, [metrics, graphs]);

  // Filtered lists
  const filteredMetrics = useMemo(() => {
    return metrics.filter((m) => {
      const matchesCategory = selectedCategory === 'all' || m.category === selectedCategory;
      const q = searchQuery.toLowerCase().trim();
      const matchesSearch =
        !q ||
        m.name.toLowerCase().includes(q) ||
        m.code.toLowerCase().includes(q) ||
        m.shortDefinition.toLowerCase().includes(q) ||
        m.tacticalMeaning.toLowerCase().includes(q) ||
        m.tags.some((t) => t.toLowerCase().includes(q));
      return matchesCategory && matchesSearch;
    });
  }, [metrics, selectedCategory, searchQuery]);

  const filteredGraphs = useMemo(() => {
    if (selectedCategory !== 'all' && selectedCategory !== 'graphs') return [];
    const q = searchQuery.toLowerCase().trim();
    if (!q) return graphs;
    return graphs.filter(
      (g) =>
        g.title.toLowerCase().includes(q) ||
        g.subtitle.toLowerCase().includes(q) ||
        g.description.toLowerCase().includes(q) ||
        g.keyFeatures.some((f) => f.toLowerCase().includes(q))
    );
  }, [graphs, selectedCategory, searchQuery]);

  // Demo Pizza Slices for Interactive Preview
  const demoSlices = [
    { label: 'NPxG/90', category: 'attacking', pct: 92, color: 'var(--accent-attacking)' },
    { label: 'xG/Shot', category: 'attacking', pct: 86, color: 'var(--accent-attacking)' },
    { label: 'Shots/90', category: 'attacking', pct: 78, color: 'var(--accent-attacking)' },
    { label: 'xA/90', category: 'passing', pct: 84, color: 'var(--accent-passing)' },
    { label: 'OP-KP/90', category: 'passing', pct: 91, color: 'var(--accent-passing)' },
    { label: 'Pr Passes', category: 'passing', pct: 72, color: 'var(--accent-passing)' },
    { label: 'Dribbles/90', category: 'possession', pct: 88, color: 'var(--accent-possession)' },
    { label: 'Retain %', category: 'possession', pct: 64, color: 'var(--accent-possession)' },
    { label: 'Turnover %', category: 'possession', pct: 75, color: 'var(--accent-possession)' },
    { label: 'Def Act/90', category: 'defending', pct: 45, color: 'var(--accent-defending)' },
    { label: 'Pressures', category: 'defending', pct: 82, color: 'var(--accent-defending)' },
    { label: 'Aerial Won', category: 'defending', pct: 68, color: 'var(--accent-defending)' },
  ];

  return (
    <div style={{ maxWidth: '1400px', margin: '0 auto', paddingBottom: '4rem' }}>
      {/* Hero Header */}
      <div
        style={{
          background: 'linear-gradient(180deg, rgba(14, 165, 233, 0.12) 0%, rgba(10, 13, 20, 0) 100%)',
          borderRadius: '24px',
          padding: '3rem 2.5rem',
          border: '1px solid var(--border-subtle)',
          marginBottom: '2.5rem',
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        <div style={{ position: 'relative', zIndex: 1, maxWidth: '900px' }}>
          <div
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.4rem 0.85rem',
              borderRadius: '20px',
              background: 'rgba(14, 165, 233, 0.15)',
              border: '1px solid rgba(14, 165, 233, 0.3)',
              color: '#38bdf8',
              fontSize: '0.82rem',
              fontWeight: 600,
              marginBottom: '1rem',
            }}
          >
            <BookOpen size={14} />
            <span>Opta & FBref Analytics Dictionary</span>
          </div>

          <h1
            style={{
              fontSize: '2.75rem',
              fontWeight: 800,
              lineHeight: 1.15,
              color: 'var(--text-main)',
              marginBottom: '1rem',
              letterSpacing: '-0.02em',
            }}
          >
            Metrics, Mathematics & Visual Graph Guide
          </h1>

          <p
            style={{
              fontSize: '1.1rem',
              color: 'var(--text-muted)',
              lineHeight: 1.6,
              marginBottom: '2rem',
            }}
          >
            A comprehensive reference explaining every graph, percentile benchmark, mathematical model, and advanced
            Moneyball metric across the entire Football Manager Analytics platform.
          </p>

          {/* Quick Search */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '0.75rem',
              background: 'var(--bg-elevated)',
              border: '1px solid var(--border-highlight)',
              borderRadius: '14px',
              padding: '0.85rem 1.25rem',
              boxShadow: 'var(--glass-shadow)',
              maxWidth: '650px',
            }}
          >
            <Search size={18} color="var(--text-dim)" />
            <input
              type="text"
              placeholder="Search metrics, formulas, or graphs (e.g., 'NPxG', 'Pizza', 'Hold Rate', 'Euclidean')..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              style={{
                background: 'transparent',
                border: 'none',
                outline: 'none',
                color: 'var(--text-main)',
                fontSize: '0.95rem',
                width: '100%',
              }}
              id="metrics-search-input"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery('')}
                style={{
                  color: 'var(--text-dim)',
                  fontSize: '0.8rem',
                  padding: '2px 8px',
                  borderRadius: '6px',
                  background: 'var(--bg-card)',
                }}
              >
                Clear
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Category Tabs */}
      <div
        style={{
          display: 'flex',
          gap: '0.6rem',
          flexWrap: 'wrap',
          marginBottom: '2.5rem',
        }}
        id="category-tabs"
      >
        {categories.map((cat) => {
          const Icon = cat.icon;
          const isSelected = selectedCategory === cat.id;
          return (
            <button
              key={cat.id}
              onClick={() => setSelectedCategory(cat.id)}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.5rem',
                padding: '0.65rem 1.15rem',
                borderRadius: '12px',
                fontSize: '0.88rem',
                fontWeight: 600,
                background: isSelected ? 'var(--accent-primary)' : 'var(--bg-card)',
                color: isSelected ? '#fff' : 'var(--text-muted)',
                border: isSelected ? '1px solid var(--accent-primary)' : '1px solid var(--border-subtle)',
                transition: 'all 0.15s ease',
              }}
              id={`tab-${cat.id}`}
            >
              <Icon size={16} />
              <span>{cat.label}</span>
              <span
                style={{
                  fontSize: '0.72rem',
                  padding: '2px 6px',
                  borderRadius: '8px',
                  background: isSelected ? 'rgba(255,255,255,0.2)' : 'rgba(255,255,255,0.06)',
                  color: isSelected ? '#fff' : 'var(--text-dim)',
                }}
              >
                {cat.count}
              </span>
            </button>
          );
        })}
      </div>

      {/* SECTION 1: VISUAL GRAPHS & CHARTS */}
      {(selectedCategory === 'all' || selectedCategory === 'graphs') && filteredGraphs.length > 0 && (
        <div style={{ marginBottom: '4rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.5rem' }}>
            <BarChart3 size={24} color="#06b6d4" />
            <h2 style={{ fontSize: '1.8rem', fontWeight: 700, color: 'var(--text-main)' }}>
              Visual Graphs & Charts on the Web App
            </h2>
          </div>

          {/* Interactive Graph Showcase: Pizza Radar Demo */}
          <div
            style={{
              background: 'var(--bg-card)',
              border: '1px solid var(--border-subtle)',
              borderRadius: '20px',
              padding: '2rem',
              marginBottom: '2rem',
              display: 'grid',
              gridTemplateColumns: 'minmax(320px, 480px) 1fr',
              gap: '2.5rem',
              alignItems: 'center',
            }}
          >
            {/* Interactive Pizza Visual SVG */}
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
              <div style={{ marginBottom: '1rem', display: 'flex', gap: '0.5rem' }}>
                <button
                  onClick={() => setPizzaMode('pizza')}
                  style={{
                    padding: '0.4rem 0.85rem',
                    borderRadius: '8px',
                    fontSize: '0.8rem',
                    fontWeight: 600,
                    background: pizzaMode === 'pizza' ? 'var(--accent-primary)' : 'var(--bg-elevated)',
                    color: pizzaMode === 'pizza' ? '#fff' : 'var(--text-muted)',
                    border: '1px solid var(--border-subtle)',
                  }}
                >
                  Pizza Slices Mode
                </button>
                <button
                  onClick={() => setPizzaMode('radar')}
                  style={{
                    padding: '0.4rem 0.85rem',
                    borderRadius: '8px',
                    fontSize: '0.8rem',
                    fontWeight: 600,
                    background: pizzaMode === 'radar' ? 'var(--accent-primary)' : 'var(--bg-elevated)',
                    color: pizzaMode === 'radar' ? '#fff' : 'var(--text-muted)',
                    border: '1px solid var(--border-subtle)',
                  }}
                >
                  Polygon Radar Mode
                </button>
              </div>

              <svg width="340" height="340" viewBox="0 0 340 340">
                <defs>
                  <radialGradient id="radarCenterGlow" cx="50%" cy="50%" r="50%">
                    <stop offset="0%" stopColor="rgba(6, 182, 212, 0.2)" />
                    <stop offset="100%" stopColor="transparent" />
                  </radialGradient>
                </defs>

                <circle cx="170" cy="170" r="140" fill="url(#radarCenterGlow)" />

                {/* Concentric Percentile Rings */}
                {[0.25, 0.5, 0.75, 1.0].map((ring, idx) => (
                  <circle
                    key={idx}
                    cx="170"
                    cy="170"
                    r={140 * ring}
                    fill="none"
                    stroke="rgba(255, 255, 255, 0.08)"
                    strokeDasharray={ring === 1.0 ? 'none' : '4 4'}
                  />
                ))}

                {/* Radial Grid Lines */}
                {demoSlices.map((_, i) => {
                  const angle = (i * 2 * Math.PI) / demoSlices.length - Math.PI / 2;
                  const x = 170 + 140 * Math.cos(angle);
                  const y = 170 + 140 * Math.sin(angle);
                  return (
                    <line
                      key={i}
                      x1="170"
                      y1="170"
                      x2={x}
                      y2={y}
                      stroke="rgba(255, 255, 255, 0.06)"
                    />
                  );
                })}

                {/* Slices or Polygon */}
                {pizzaMode === 'pizza' ? (
                  demoSlices.map((slice, i) => {
                    const step = (2 * Math.PI) / demoSlices.length;
                    const startAngle = i * step - Math.PI / 2 + 0.03;
                    const endAngle = (i + 1) * step - Math.PI / 2 - 0.03;
                    const r = Math.max(15, (slice.pct / 100) * 140);

                    const x1 = 170 + r * Math.cos(startAngle);
                    const y1 = 170 + r * Math.sin(startAngle);
                    const x2 = 170 + r * Math.cos(endAngle);
                    const y2 = 170 + r * Math.sin(endAngle);

                    const isHovered = selectedGraphSlice === i;
                    const path = `M 170 170 L ${x1} ${y1} A ${r} ${r} 0 0 1 ${x2} ${y2} Z`;

                    return (
                      <path
                        key={i}
                        d={path}
                        fill={slice.color}
                        fillOpacity={isHovered ? 0.75 : 0.4}
                        stroke={slice.color}
                        strokeWidth={isHovered ? 2.5 : 1.2}
                        onMouseEnter={() => setSelectedGraphSlice(i)}
                        style={{ cursor: 'pointer', transition: 'all 0.2s ease' }}
                      />
                    );
                  })
                ) : (
                  <polygon
                    points={demoSlices
                      .map((slice, i) => {
                        const angle = (i * 2 * Math.PI) / demoSlices.length - Math.PI / 2;
                        const r = (slice.pct / 100) * 140;
                        return `${170 + r * Math.cos(angle)},${170 + r * Math.sin(angle)}`;
                      })
                      .join(' ')}
                    fill="rgba(6, 182, 212, 0.35)"
                    stroke="#06b6d4"
                    strokeWidth="2.5"
                  />
                )}

                {/* Center Core */}
                <circle cx="170" cy="170" r="16" fill="var(--bg-elevated)" stroke="var(--border-highlight)" />
                <text
                  x="170"
                  y="174"
                  textAnchor="middle"
                  fill="var(--text-main)"
                  fontSize="10"
                  fontWeight="bold"
                >
                  99%
                </text>
              </svg>

              <div
                style={{
                  marginTop: '0.75rem',
                  fontSize: '0.85rem',
                  color: 'var(--text-muted)',
                  textAlign: 'center',
                }}
              >
                Hover slice: <strong style={{ color: demoSlices[selectedGraphSlice].color }}>{demoSlices[selectedGraphSlice].label}</strong> ({demoSlices[selectedGraphSlice].pct}th percentile)
              </div>
            </div>

            {/* Explanation of Opta Pizza Chart */}
            <div>
              <div
                style={{
                  fontSize: '0.8rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  letterSpacing: '0.05em',
                  color: '#06b6d4',
                  marginBottom: '0.5rem',
                }}
              >
                Interactive Chart Anatomy
              </div>
              <h3 style={{ fontSize: '1.4rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.85rem' }}>
                How to Read the Opta / StatsBomb Pizza Chart
              </h3>
              <p style={{ color: 'var(--text-muted)', lineHeight: 1.6, marginBottom: '1.25rem' }}>
                The Pizza Radar eliminates the visual distortion of standard attribute spider graphs. Instead of showing
                raw numbers (e.g. 1–20), it shows <strong>percentile rank (0 to 99)</strong> calculated specifically within the player’s
                positional peer group.
              </p>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '0.75rem', marginBottom: '1.25rem' }}>
                <div style={{ background: 'var(--bg-elevated)', padding: '0.75rem 1rem', borderRadius: '10px', borderLeft: '3px solid #f59e0b' }}>
                  <div style={{ fontWeight: 600, color: '#f59e0b', fontSize: '0.85rem' }}>Attacking Quadrant</div>
                  <div style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>NPxG, Shot Quality, Shots on Target</div>
                </div>
                <div style={{ background: 'var(--bg-elevated)', padding: '0.75rem 1rem', borderRadius: '10px', borderLeft: '3px solid #06b6d4' }}>
                  <div style={{ fontWeight: 600, color: '#06b6d4', fontSize: '0.85rem' }}>Passing Quadrant</div>
                  <div style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>xA, Open-Play Key Passes, Pr Passes</div>
                </div>
                <div style={{ background: 'var(--bg-elevated)', padding: '0.75rem 1rem', borderRadius: '10px', borderLeft: '3px solid #8b5cf6' }}>
                  <div style={{ fontWeight: 600, color: '#8b5cf6', fontSize: '0.85rem' }}>Possession Quadrant</div>
                  <div style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>Dribbles, Retention %, Progression Share</div>
                </div>
                <div style={{ background: 'var(--bg-elevated)', padding: '0.75rem 1rem', borderRadius: '10px', borderLeft: '3px solid #f43f5e' }}>
                  <div style={{ fontWeight: 600, color: '#f43f5e', fontSize: '0.85rem' }}>Defending Quadrant</div>
                  <div style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>Def Actions, Pressures, Aerial Duels</div>
                </div>
              </div>

              <div
                style={{
                  background: 'rgba(14, 165, 233, 0.08)',
                  border: '1px solid rgba(14, 165, 233, 0.2)',
                  borderRadius: '10px',
                  padding: '0.85rem 1.15rem',
                  fontSize: '0.85rem',
                  color: '#38bdf8',
                  lineHeight: 1.5,
                }}
              >
                💡 <strong>Scout’s Rule of Thumb:</strong> Slices reaching the outer dashed boundary mean the player is among
                the top 1% in the world for that specific output. A balanced circular polygon indicates an elite all-arounder.
              </div>
            </div>
          </div>

          {/* Cards for All Graph Types */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(420px, 1fr))', gap: '1.5rem' }}>
            {filteredGraphs.map((g) => (
              <div
                key={g.id}
                style={{
                  background: 'var(--bg-card)',
                  border: '1px solid var(--border-subtle)',
                  borderRadius: '16px',
                  padding: '1.75rem',
                  display: 'flex',
                  flexDirection: 'column',
                  justifyContent: 'space-between',
                  transition: 'all 0.2s ease',
                }}
                className="graph-card"
              >
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.75rem' }}>
                    <div>
                      <h3 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.2rem' }}>
                        {g.title}
                      </h3>
                      <div style={{ fontSize: '0.82rem', color: 'var(--text-dim)' }}>{g.subtitle}</div>
                    </div>
                  </div>

                  <p style={{ fontSize: '0.9rem', color: 'var(--text-muted)', lineHeight: 1.55, marginBottom: '1.25rem' }}>
                    {g.description}
                  </p>

                  <div style={{ marginBottom: '1.25rem' }}>
                    <div style={{ fontSize: '0.8rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.5rem' }}>
                      Key Features:
                    </div>
                    <ul style={{ paddingLeft: '1.2rem', fontSize: '0.83rem', color: 'var(--text-muted)', lineHeight: 1.6 }}>
                      {g.keyFeatures.map((f, i) => (
                        <li key={i}>{f}</li>
                      ))}
                    </ul>
                  </div>

                  <div
                    style={{
                      background: 'var(--bg-elevated)',
                      border: '1px solid var(--border-subtle)',
                      borderRadius: '10px',
                      padding: '0.85rem 1rem',
                      marginBottom: '1rem',
                    }}
                  >
                    <div style={{ fontSize: '0.78rem', fontWeight: 700, color: '#38bdf8', marginBottom: '0.25rem' }}>
                      HOW TO INTERPRET:
                    </div>
                    <div style={{ fontSize: '0.83rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
                      {g.howToInterpret}
                    </div>
                  </div>
                </div>

                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    paddingTop: '1rem',
                    borderTop: '1px solid var(--border-subtle)',
                    fontSize: '0.78rem',
                    color: 'var(--text-dim)',
                  }}
                >
                  <div>
                    Used in: <strong style={{ color: 'var(--text-muted)' }}>{g.usedIn.join(', ')}</strong>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* SECTION 2: METRIC CATALOG */}
      {filteredMetrics.length > 0 && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
              <Calculator size={24} color="#10b981" />
              <h2 style={{ fontSize: '1.8rem', fontWeight: 700, color: 'var(--text-main)' }}>
                Detailed Metric Catalog & Formulas
              </h2>
            </div>
            <div style={{ fontSize: '0.88rem', color: 'var(--text-dim)' }}>
              Showing <strong>{filteredMetrics.length}</strong> defined metrics
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(440px, 1fr))', gap: '1.5rem' }}>
            {filteredMetrics.map((m) => {
              const categoryColor =
                m.category === 'attacking'
                  ? 'var(--accent-attacking)'
                  : m.category === 'passing'
                  ? 'var(--accent-passing)'
                  : m.category === 'defending'
                  ? 'var(--accent-defending)'
                  : m.category === 'goalkeeper'
                  ? '#38bdf8'
                  : m.category === 'valuation'
                  ? 'var(--accent-emerald)'
                  : '#a855f7';

              return (
                <div
                  key={m.id}
                  style={{
                    background: 'var(--bg-card)',
                    border: '1px solid var(--border-subtle)',
                    borderRadius: '16px',
                    padding: '1.75rem',
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'space-between',
                  }}
                  id={`metric-${m.id}`}
                >
                  <div>
                    {/* Header */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.75rem' }}>
                      <div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', marginBottom: '0.25rem' }}>
                          <span
                            style={{
                              fontFamily: 'JetBrains Mono, monospace',
                              fontSize: '0.9rem',
                              fontWeight: 700,
                              color: categoryColor,
                              background: 'rgba(255,255,255,0.06)',
                              padding: '2px 8px',
                              borderRadius: '6px',
                            }}
                          >
                            {m.code}
                          </span>
                          <span
                            style={{
                              fontSize: '0.72rem',
                              fontWeight: 600,
                              textTransform: 'uppercase',
                              letterSpacing: '0.04em',
                              padding: '2px 8px',
                              borderRadius: '10px',
                              background: 'var(--bg-elevated)',
                              color: 'var(--text-dim)',
                            }}
                          >
                            {m.category}
                          </span>
                        </div>
                        <h3 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)' }}>
                          {m.name}
                        </h3>
                      </div>
                    </div>

                    {/* Short Def */}
                    <p style={{ fontSize: '0.9rem', color: 'var(--text-muted)', lineHeight: 1.5, marginBottom: '1rem' }}>
                      {m.shortDefinition}
                    </p>

                    {/* Formula Box */}
                    <div
                      style={{
                        background: 'var(--bg-primary)',
                        border: '1px solid var(--border-subtle)',
                        borderRadius: '10px',
                        padding: '0.85rem 1.15rem',
                        marginBottom: '1rem',
                        fontFamily: 'JetBrains Mono, monospace',
                        fontSize: '0.82rem',
                        color: '#38bdf8',
                        overflowX: 'auto',
                      }}
                    >
                      {m.formula}
                    </div>

                    {/* Tactical Meaning */}
                    <div style={{ marginBottom: '1rem' }}>
                      <div style={{ fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-dim)', marginBottom: '0.25rem' }}>
                        Tactical Interpretation:
                      </div>
                      <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', lineHeight: 1.55 }}>
                        {m.tacticalMeaning}
                      </p>
                    </div>

                    {/* Moneyball Significance */}
                    <div style={{ marginBottom: '1.25rem' }}>
                      <div style={{ fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--accent-emerald)', marginBottom: '0.25rem' }}>
                        Moneyball Value:
                      </div>
                      <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', lineHeight: 1.55 }}>
                        {m.moneyballSignificance}
                      </p>
                    </div>

                    {/* Benchmarks Grid */}
                    <div
                      style={{
                        background: 'var(--bg-elevated)',
                        borderRadius: '10px',
                        padding: '0.85rem 1rem',
                        marginBottom: '1rem',
                        fontSize: '0.8rem',
                      }}
                    >
                      <div style={{ fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.4rem' }}>
                        Performance Benchmarks:
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem', color: 'var(--text-muted)' }}>
                        {m.benchmarks.elite && <div>🟢 <strong style={{ color: 'var(--accent-emerald)' }}>Elite:</strong> {m.benchmarks.elite}</div>}
                        {m.benchmarks.good && <div>🔵 <strong style={{ color: '#38bdf8' }}>Good:</strong> {m.benchmarks.good}</div>}
                        {m.benchmarks.average && <div>🟡 <strong style={{ color: '#fbbf24' }}>Average:</strong> {m.benchmarks.average}</div>}
                        {m.benchmarks.poor && <div>🔴 <strong style={{ color: '#f43f5e' }}>Below Avg:</strong> {m.benchmarks.poor}</div>}
                      </div>
                    </div>
                  </div>

                  {/* FM Source Footer */}
                  <div
                    style={{
                      borderTop: '1px solid var(--border-subtle)',
                      paddingTop: '0.85rem',
                      fontSize: '0.75rem',
                      color: 'var(--text-dim)',
                    }}
                  >
                    🎮 <strong>FM24 Source:</strong> {m.fmSource}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Empty State */}
      {filteredMetrics.length === 0 && filteredGraphs.length === 0 && (
        <div
          style={{
            textAlign: 'center',
            padding: '4rem 2rem',
            background: 'var(--bg-card)',
            borderRadius: '20px',
            border: '1px solid var(--border-subtle)',
          }}
        >
          <HelpCircle size={48} color="var(--text-dim)" style={{ marginBottom: '1rem' }} />
          <h3 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '0.5rem' }}>
            No metrics or graphs match &quot;{searchQuery}&quot;
          </h3>
          <p style={{ color: 'var(--text-muted)', marginBottom: '1.5rem' }}>
            Try searching for terms like &quot;xG&quot;, &quot;Shrinkage&quot;, &quot;Radar&quot;, &quot;Hold Rate&quot;, or click a category tab above.
          </p>
          <button
            onClick={() => {
              setSearchQuery('');
              setSelectedCategory('all');
            }}
            style={{
              padding: '0.65rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--accent-primary)',
              color: '#fff',
              fontWeight: 600,
            }}
          >
            Reset Filters
          </button>
        </div>
      )}

      {/* Floating Action Links to Webapp Tools */}
      <div
        style={{
          marginTop: '4rem',
          background: 'linear-gradient(135deg, rgba(59, 130, 246, 0.1), rgba(139, 92, 246, 0.1))',
          borderRadius: '20px',
          border: '1px solid var(--border-highlight)',
          padding: '2.5rem',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: '1.5rem',
        }}
      >
        <div>
          <h3 style={{ fontSize: '1.5rem', fontWeight: 800, color: 'var(--text-main)', marginBottom: '0.5rem' }}>
            Ready to Put the Metrics into Action?
          </h3>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.95rem' }}>
            Test out these metrics across our live database of 11,350+ players and goalkeepers.
          </p>
        </div>

        <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
          <Link
            href="/screener"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.75rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--accent-primary)',
              color: '#fff',
              fontWeight: 600,
              fontSize: '0.9rem',
            }}
          >
            <SlidersHorizontal size={16} />
            <span>Launch Screener</span>
          </Link>
          <Link
            href="/scatter"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.75rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--bg-elevated)',
              border: '1px solid var(--border-highlight)',
              color: 'var(--text-main)',
              fontWeight: 600,
              fontSize: '0.9rem',
            }}
          >
            <ScatterChart size={16} />
            <span>Open Scatter Explorer</span>
          </Link>
          <Link
            href="/"
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.5rem',
              padding: '0.75rem 1.25rem',
              borderRadius: '10px',
              background: 'var(--bg-elevated)',
              border: '1px solid var(--border-highlight)',
              color: 'var(--text-main)',
              fontWeight: 600,
              fontSize: '0.9rem',
            }}
          >
            <BarChart3 size={16} />
            <span>Scouting Hub</span>
          </Link>
        </div>
      </div>
    </div>
  );
}
