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

  // Category Configuration
  const categories = [
    { id: 'all', label: 'All Knowledge', icon: Layers, count: 32 },
    { id: 'graphs', label: 'Visual Graphs & Charts', icon: BarChart3, count: 6 },
    { id: 'attacking', label: 'Attacking & Shooting', icon: Target, count: 7 },
    { id: 'passing', label: 'Passing & Creation', icon: Zap, count: 7 },
    { id: 'defending', label: 'Defending & Duels', icon: Shield, count: 6 },
    { id: 'goalkeeper', label: 'Goalkeeping', icon: Activity, count: 5 },
    { id: 'valuation', label: 'Moneyball & Valuation', icon: Coins, count: 4 },
    { id: 'math', label: 'Similarity & Modeling', icon: Calculator, count: 3 },
  ];

  // Visual Graph Definitions
  const graphs: GraphDefinition[] = [
    {
      id: 'pizza-radar',
      title: 'Opta / StatsBomb Pizza Radar Chart',
      subtitle: 'Multi-metric radial percentile evaluation',
      usedIn: ['Player Dossier (/player/[id])', 'Head-to-Head Compare (/compare)'],
      description:
        'A radial visualization displaying up to 14 tactical metrics grouped into 4 distinct quadrants. Each slice represents a specific statistical category, and its radial length (distance from the center) represents the player’s percentile rank (0 to 99) against positional peers.',
      keyFeatures: [
        '4 Positional Quadrants: Attacking (Amber), Passing (Cyan), Possession (Purple), and Defending (Rose).',
        'Goalkeeper Mode: Shot Stopping, Handling/Rebounds, Sweeping/Aerial, and Distribution.',
        'Percentile Standard: Evaluated against players within the exact same positional cluster (e.g., Striker vs Strikers) with >= 900 minutes.',
        'Dual View Mode: Toggle between individual Pizza Slices (independent attributes) and Classic Radar Polygon (overall tactical silhouette).',
      ],
      howToInterpret:
        'A full slice reaching the outer perimeter means the player is in the 99th percentile (top 1% globally for that position). A shriveled slice near the center indicates a below-average metric. Look for wide, balanced coverage for all-arounders or sharp, focused spikes for specialist roles (e.g. Fox in the Box vs Playmaking Winger).',
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
        'Color-Coded Tiers: Elite 90+ (Emerald Green), Above Average 75–89 (Cyan), Average 50–74 (Amber), Below Average <50 (Slate).',
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
  ];

  // Comprehensive Metric Definitions
  const metrics: MetricDefinition[] = [
    // --- ATTACKING ---
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
      tags: ['xG', 'Scoring', 'Striker', 'Winger', 'Finishing'],
    },
    {
      id: 'xg-per-shot',
      name: 'Shot Quality & Selection Index',
      code: 'xG / Shot',
      category: 'attacking',
      formula: '\\text{Shot Quality} = \\frac{\\text{Total } xG}{\\text{Shots Attempted}}',
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
      tags: ['Shooting', 'Decision Making', 'Efficiency', 'Striker'],
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
      tags: ['Finishing', 'Bayesian', 'Regression', 'Moneyball'],
    },
    {
      id: 'shot-conv',
      name: 'Shot Conversion Rate',
      code: 'Conv %',
      category: 'attacking',
      formula: '\\text{Conversion Rate} = \\frac{\\text{Total Goals}}{\\text{Total Shots Attempted}} \\times 100',
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
      tags: ['Goals', 'Finishing', 'Striker'],
    },
    {
      id: 'goal-threat-share',
      name: 'Goal Threat Share %',
      code: 'Threat Share',
      category: 'attacking',
      formula: '\\text{Threat Share} = \\frac{\\text{Player } NPxG}{\\text{Estimated Team Total } NPxG} \\times 100',
      shortDefinition: 'The proportion of a club’s total open-play attacking threat concentrated in this single player.',
      tacticalMeaning:
        'Measures tactical focalization. A striker with 35% Goal Threat Share is the undisputed focal point of their team’s entire attacking system.',
      moneyballSignificance:
        'Helps assess whether a player will maintain their output if transferred to a team with other high-usage stars.',
      benchmarks: {
        elite: '>= 30.0% (Primary offensive focal point of team)',
        good: '20.0% - 29.9% (Major offensive contributor)',
        average: '12.0% - 19.9% (Complementary attacker)',
      },
      fmSource: 'Derived via analytical views in gold schema',
      tags: ['Role', 'Tactics', 'Attacking'],
    },

    // --- PASSING & CREATION ---
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
      tags: ['Assists', 'Vision', 'Playmaker', 'Passing'],
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
      tags: ['xGI', 'Offense', 'Benchmark', 'Moneyball'],
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
      tags: ['Playmaker', 'Quality', 'Passing'],
    },
    {
      id: 'prog-passes-per90',
      name: 'Progressive Passes per 90',
      code: 'Pr Passes/90',
      category: 'passing',
      formula: '\\text{Pr passes/90} = \\frac{\\text{Progressive Passes}}{\\text{Minutes} / 90.0}',
      shortDefinition: 'Completed passes that advance the ball substantially toward the opponent’s goal.',
      tacticalMeaning:
        'Identifies line-breaking passers who transition their team from defense to attack. Crucial metric for Ball-Playing Defenders, Deep-Lying Playmakers, and Registas.',
      moneyballSignificance:
        'Pass completion percentage is misleading because sideways and backward passes are safe. Progressive passes measure bravery and line-breaking utility.',
      benchmarks: {
        elite: '>= 7.5 (Elite deep-lying distributor / midfield tempo controller)',
        good: '5.0 - 7.4 (Good progression)',
        average: '3.0 - 4.9 (Average progression)',
      },
      fmSource: 'Pr passes/90 column from FM24 export',
      tags: ['Progressive', 'Midfield', 'Ball-Playing Defender', 'Passing'],
    },
    {
      id: 'progression-share',
      name: 'Team Progression Share %',
      code: 'Prog Share %',
      category: 'passing',
      formula: '\\text{Progression Share} = \\frac{\\text{Player Pr Passes/90}}{\\text{Team Total Pr Passes/90}} \\times 100',
      shortDefinition: 'The percentage of the entire team’s forward progression executed by this player.',
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
      tags: ['Midfield', 'Distribution', 'Tactics'],
    },
    {
      id: 'turnover-rate',
      name: 'Ball Turnover Rate %',
      code: 'Turnover %',
      category: 'passing',
      formula: '\\text{Turnover \\%} = \\frac{\\text{Possession Lost/90}}{\\text{Passes Attempted/90} + \\text{Dribbles/90} + \\text{Crosses/90}} \\times 100',
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
      fmSource: 'Derived from Poss Lost/90, Pas A/90, Drb/90, and Crs A/90',
      tags: ['Retention', 'Security', 'Turnovers', 'Midfield'],
    },

    // --- DEFENDING & DUELS ---
    {
      id: 'def-actions-per90',
      name: 'Total Defensive Actions per 90',
      code: 'Def Actions/90',
      category: 'defending',
      formula: '\\text{Def Actions} = \\text{Tackles Won/90} + \\text{Interceptions/90} + \\text{Blocks/90} + \\text{Clearances/90}',
      shortDefinition: 'The total volume of defensive interventions completed per 90 minutes.',
      tacticalMeaning:
        'Measures overall defensive workload and activity level. High values are characteristic of high-intensity ball-winners and active defenders.',
      moneyballSignificance:
        'Provides a unified volume count across tackles, interceptions, and clearances rather than evaluating tackles in isolation.',
      benchmarks: {
        elite: '>= 8.5 (High-activity defensive destroyer / active centerback)',
        good: '6.5 - 8.4 (Solid defensive workload)',
        average: '4.5 - 6.4 (Standard output)',
      },
      fmSource: 'Derived from Tck A/90, Tck R, Int/90, Blk/90, Clr/90',
      tags: ['Defending', 'Tackles', 'Interceptions', 'Centerback'],
    },
    {
      id: 'reactive-ratio',
      name: 'Reactive vs Positional Defending Ratio',
      code: 'Reactive Ratio',
      category: 'defending',
      formula: '\\text{Reactive Ratio} = \\frac{\\text{Interceptions/90}}{\\text{Interceptions/90} + \\text{Tackles Won/90}}',
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
      },
      fmSource: 'Aer A/90 and Hdr % from FM24 export',
      tags: ['Aerial', 'Headers', 'Centerback', 'Striker'],
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
      tags: ['Pressing', 'Work Rate', 'Gegenpress', 'Midfield'],
    },

    // --- GOALKEEPER METRICS ---
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
      id: 'hold-rate',
      name: 'Goalkeeper Hold Rate % (Rebound Prevention)',
      code: 'Hold Rate %',
      category: 'goalkeeper',
      formula: '\\text{Hold Rate \\%} = \\frac{\\text{Shots Held/90}}{\\text{Total Saves/90}} \\times 100',
      shortDefinition: 'The percentage of saved shots that are cleanly caught and held rather than parried back into danger.',
      tacticalMeaning:
        'Catching the ball terminates the opposition’s attack immediately. High hold rates prevent dangerous secondary rebound tap-ins.',
      moneyballSignificance:
        'Underappreciated skill: a keeper with high saves but low hold rate creates chaos inside their own 6-yard box by giving away rebounds.',
      benchmarks: {
        elite: '>= 48.0% (Elite handling and rebound prevention)',
        good: '38.0% - 47.9% (Reliable hands)',
        average: '< 34.0% (Prone to parrying into danger zones)',
      },
      fmSource: 'Held/90 and Saves/90 from FM24 export',
      tags: ['Handling', 'Rebounds', 'Goalkeeper'],
    },
    {
      id: 'sweeper-index',
      name: 'Sweeper Keeper Index per 90',
      code: 'Sweeper Index',
      category: 'goalkeeper',
      formula: '\\text{Sweeper Index} = \\text{Interceptions/90} + \\text{Clearances/90} + \\text{Possession Won/90}',
      shortDefinition: 'Aggressive defensive interventions executed outside the 6-yard box and penalty area.',
      tacticalMeaning:
        'Crucial for teams playing a high defensive line. Evaluates whether the goalkeeper rushes out to smother through-balls before opponent strikers can reach them.',
      moneyballSignificance:
        'Essential for modern Pep Guardiola / Klopp / De Zerbi style high-line tactical frameworks.',
      benchmarks: {
        elite: '>= 2.5 (Proactive Sweeper Keeper / high-line safety net)',
        good: '1.6 - 2.4 (Active off line)',
        average: '< 1.2 (Traditional penalty-box line keeper)',
      },
      fmSource: 'Int/90, Clr/90, Poss Won/90 from FM24 export',
      tags: ['Sweeper Keeper', 'High Line', 'Goalkeeper'],
    },

    // --- MONEYBALL & VALUATION ---
    {
      id: 'cost-per-xgi',
      name: 'Cost per Expected Goal Involvement (£k)',
      code: 'Cost per xGI',
      category: 'valuation',
      formula: '\\text{Cost per xGI} = \\frac{\\text{Transfer Value (£M)} \\times 1000 + (\\text{Weekly Wage (£k)} \\times 52)}{\\text{NPxG} + \\text{xA}}',
      shortDefinition: 'The total financial investment (transfer fee + annual salary) required for each unit of expected goal contribution delivered.',
      tacticalMeaning:
        'The centerpiece metric of the Moneyball engine. Identifies undervalued statistical phenoms playing in secondary leagues whose output matches superstars at a fraction of the cost.',
      moneyballSignificance:
        'If Star A costs £250k per xGI and Star B costs £1,200k per xGI with identical tactical profiles, buying Star A saves millions to reinvest in squad depth.',
      benchmarks: {
        elite: '<= £150k / xGI (Tremendous Moneyball bargain)',
        good: '£151k - £350k / xGI (High-value starter)',
        average: '£351k - £800k / xGI (Standard market price)',
        poor: '> £1,000k / xGI (Overpriced luxury asset)',
      },
      fmSource: 'Derived from Transfer Value, Wage, and NPxG+xA',
      tags: ['Moneyball', 'Bargains', 'Finance', 'Recruitment'],
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
      tags: ['Bargains', 'Value', 'Scouting'],
    },

    // --- MATHEMATICAL & SIMILARITY MODELS ---
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
      tags: ['Euclidean', 'Similarity', 'Replacement', 'Math'],
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
      },
      fmSource: 'Computed across all gold.vw_similar_* views',
      tags: ['Match %', 'Similarity', 'Scouting'],
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
      tags: ['SCD2', 'Database', 'Engineering', 'History'],
    },
  ];

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
            Moneyball metric across the FM Analytics scouting platform.
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
                        <div>🟢 <strong style={{ color: 'var(--accent-emerald)' }}>Elite:</strong> {m.benchmarks.elite}</div>
                        <div>🔵 <strong style={{ color: '#38bdf8' }}>Good:</strong> {m.benchmarks.good}</div>
                        <div>🟡 <strong style={{ color: '#fbbf24' }}>Average:</strong> {m.benchmarks.average}</div>
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
