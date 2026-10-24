# 📐 Metrics & Similarity Mathematics

This guide documents the theoretical foundations, mathematical formulations, feature weightings, and tactical rules powering the analytics and similarity matching engines in `DW`.

---

## 1. Moneyball in Football Manager 24

Traditional scouting in Football Manager relies heavily on visible attribute numbers (1–20) and scout star ratings. However, these suffer from critical blind spots:
1. **Reputation Bias:** Scout star ratings are heavily skewed by player reputation and league prestige rather than actual on-pitch tactical efficiency.
2. **Tactical Mismatch:** A player with 16 Passing might achieve poor progressive output in an unsuitable tactical system.
3. **Attribute Inflation:** High-attribute players command exorbitant transfer fees and wages despite failing to deliver elite per-90 value.

By ingesting granular performance statistics (expected goals, expected assists, progressive passing volume, pressure completions, and aerial duels) into `DW`, recruitment decisions are driven by **actual on-pitch output per 90 minutes**, finding undervalued gems who fit the club's tactical needs.

---

## 2. Per-90 Normalization & Standardizations

Raw match event counts are severely distorted by playing time. All event totals are normalized to per-90 minute equivalents:

$$\text{Metric Per 90} = \frac{\text{Total Metric Count}}{\text{Minutes Played} / 90.0}$$

### Season Counts Converted to Per-90:
* **Goals & Assists:** $\text{Gls}/90$, $\text{Ast}/90$.
* **Defensive Interventions:** Tackles Won/90, Tackles Attempted/90, Mistake Leading to Goals/90.
* **Discipline:** Fouls Conceded/90, Fouls Won/90, Yellow Cards/90, Red Cards/90.

---

## 3. Advanced Derived Metrics

### 3.1. Quality of Chance Creation
Rather than raw key pass volume, which includes low-danger passes into crowded boxes, the platform measures the expected danger generated per key pass:

$$\text{Chance Creation Quality} = \frac{\text{xAssists per 90}}{\text{Open-Play Key Passes per 90}}$$

* **Tactical Interpretation:** Measures whether a playmaker delivers high-probability tap-ins ($xA/\text{OP-KP} \ge 0.20$) or low-threat speculative crosses.

---

### 3.2. Shrunk Finishing Delta & Assist Luck
In football analytics, finishing overperformance ($\text{Goals} - xG$) is notoriously noisy and heavily regresses toward the mean over multi-season horizons. To prevent overpaying for players experiencing short-term hot streaks over limited minutes, we apply **Empirical Bayes shrinkage toward zero on the per-90 delta**:

$$\text{Finishing Delta}_{\text{shrunk}} = (\text{Goals/90} - \text{xGoals/90}) \times \left(\frac{\text{Minutes Played}}{\text{Minutes Played} + 900}\right)$$

Similarly, teammate finishing noise on expected assists ($\text{Ast} - xA$) is shrunk to avoid small-sample distortions:

$$\text{Assist Luck}_{\text{shrunk}} = (\text{Assists/90} - \text{xAssists/90}) \times \left(\frac{\text{Minutes Played}}{\text{Minutes Played} + 900}\right)$$

* At $900$ minutes (10 matches), the observed delta is shrunk by $50\%$.
* At $2,700$ minutes (30 matches), the shrinkage factor reaches $75\%$, placing higher confidence in persistent underlying performance.

---

### 3.3. Aerial Dominance
$$\text{Aerial Duels Won per 90} = \text{Headers Attempted per 90} \times \text{Heading Accuracy \%}$$

Both volume ($\text{Aer Won}/90$) and efficiency ($\text{Heading Accuracy \%}$) are evaluated independently: a centerback winning 3 aerial duels at 75% accuracy is tactically distinct from a target man contesting 10 aerial duels at 45% accuracy.

---

### 3.4. Crossing Threat Index
$$\text{Completed Crosses per 90} = \text{Crosses Attempted per 90} \times \text{Cross Accuracy \%}$$
$$\text{Open-Play Completed Crosses per 90} = \text{Open-Play Crosses Attempted per 90} \times \text{Open-Play Cross Accuracy \%}$$

Separating completed volume from raw attempts prevents high-volume, low-efficiency fullbacks from inflating their tactical score.

---

### 3.5. Total Defensive Actions & Reactive Defending Ratio
$$\text{Defensive Actions per 90} = (\text{Tackles Attempted}/90 \times \text{Tackle Accuracy}) + \text{Interceptions}/90 + \text{Blocks}/90 + \text{Shots Blocked}/90 + \text{Clearances}/90$$

$$\text{Non-Clearance Defensive Actions per 90} = (\text{Tackles Attempted}/90 \times \text{Tackle Accuracy}) + \text{Interceptions}/90 + \text{Blocks}/90 + \text{Shots Blocked}/90$$

$$\text{Reactive vs Positional Ratio} = \frac{\text{Interceptions}/90}{\text{Interceptions}/90 + (\text{Tackles Attempted}/90 \times \text{Tackle Accuracy})}$$

* **High Ratio ($\ge 0.55$):** Positional reader and anticipator who steps up into passing lanes without going to ground.
* **Low Ratio ($\le 0.40$):** Aggressive, physical tackler whose primary interventions come through ground challenges.

---

### 3.6. Ball Retention & Turnover Rate
$$\text{Turnover Rate} = \frac{\text{Possession Lost per 90}}{\text{Passes Attempted}/90 + \text{Dribbles}/90 + \text{Crosses}/90}$$

* Measures ball security relative to total on-ball touches. Critical for deep-lying playmakers and inverted fullbacks where possession loss is catastrophic.

---

### 3.7. Goalkeeper Performance & Bayesian Shrinkage Framework

Goalkeeper evaluation suffers from severe small-sample noise: with ~3.5 shots on target faced per 90, 900 minutes represents only ~35 shots, yielding a binomial sampling standard error of $\pm 7$ percentage points on save percentage when true talent variance between professional keepers is merely $2\text{--}3$ points. To isolate genuine talent from noise, all key metrics are anchored to **shots faced exposure ($n$)** rather than minutes played, applying Empirical Bayes shrinkage:

$$\hat{\theta}_{\text{shrunk}} = \frac{n \cdot x + k \cdot \mu}{n + k}$$

#### 3.7.1. Workload & Shot Difficulty Foundation
* **Shots Faced per 90:**
  $$\text{Shots Faced}/90 = \text{Saves Made}/90 + \text{Goals Conceded}/90$$
* **Shots Faced Total ($n$):**
  $$\text{Shots Faced Total} = \text{Shots Faced}/90 \times \left(\frac{\text{Minutes Played}}{90}\right)$$
* **Post-Shot Expected Goals Faced on Target ($\text{xGOT}/90$):**
  $$\text{xGOT Faced}/90 = \text{xGoals Prevented}/90 + \text{Goals Conceded}/90$$
* **Shot Difficulty (Defense Concession Quality):**
  $$\text{Shot Difficulty} = \frac{\text{xGOT Faced}/90}{\text{Shots Faced}/90}$$
  *Evaluates the quality of shooting conceded by the outfield defense. A keeper behind a porous defense faces harder shots (higher xGOT per shot).*

#### 3.7.2. Primary Shot-Stopping Metrics
* **xGP per Shot (Primary Shot-Stopping Metric):**
  $$\text{xGP per Shot} = \frac{\text{xGoals Prevented}/90}{\text{Shots Faced}/90}$$
  *Strips out team defensive workload, isolating the exact goals prevented per shot faced.*
* **xGP per Shot Shrunk ($k=300$, Prior $\mu = 0.0$):**
  $$\text{xGP per Shot}_{\text{shrunk}} = \frac{\text{Shots Faced Total} \times \text{xGP per Shot}}{\text{Shots Faced Total} + 300}$$
  *Derived from binomial variance ($p \approx 0.70 \implies \sigma^2 \approx 0.21$; true skill $\text{SD} \approx 0.025 \implies k = 0.21 / 0.025^2 \approx 340 \approx 300$).*
* **Goals vs xGOT Ratio:**
  $$\text{Goals vs xGOT Ratio} = \frac{\text{Goals Conceded}/90}{\text{xGOT Faced}/90}$$
  *Intuitive scouting ratio: values $< 1.00$ indicate goal denial overperformance.*
* **Projected Season Goals Prevented:**
  $$\text{xGP per Season} = \text{xGoals Prevented}/90 \times 38$$
* **Squad Replacement Upgrade Delta:**
  $$\Delta \text{Goals Saved} = (\text{Target xGP}_{\text{shrunk}} - \text{Starter xGP}_{\text{shrunk}}) \times 38$$
  *The core decision metric comparing candidate recruitment targets directly against the incumbent starter.*

#### 3.7.3. Save Handling & Rebound Risk
* **Parry Rate (Rebound Vulnerability):**
  $$\text{Parry Rate} = \frac{\text{Saves Parried}/90}{\text{Saves Made}/90}$$
* **Tip Rate (Neutral Deflection):**
  $$\text{Tip Rate} = \frac{\text{Saves Tipped}/90}{\text{Saves Made}/90}$$
* **Hold Rate Shrunk ($k=150$, Prior $\mu = 0.30$):**
  $$\text{Hold Rate}_{\text{shrunk}} = \frac{\text{Saves Total} \times \text{Hold Rate} + 150 \times 0.30}{\text{Saves Total} + 150}$$
* **Hold Rate Over Expected:**
  $$\text{Hold Rate Over Expected} = \text{Hold Rate} - (0.1143 + 0.5230 \times \text{Shot Difficulty})$$
  *Residual handling performance adjusted for shot difficulty (facing difficult dipping/swerving shots reduces hold probability).*

#### 3.7.4. Sweeping Actions & Buildup Distribution
* **Sweeper Actions per 90:**
  $$\text{Sweeper Actions}/90 = \text{Interceptions}/90 + \text{Clearances}/90$$
* **Proactive Sweeper Share:**
  $$\text{Sweeper Share} = \frac{\text{Interceptions}/90}{\text{Interceptions}/90 + \text{Clearances}/90}$$
  *Separates proactive anticipation (intercepting through-balls) from reactive crisis defending (clearances).*
* **Completed Passes per 90 & Giveaways:**
  $$\text{Completed Passes}/90 = \text{Passes Attempted}/90 \times \text{Pass Accuracy}$$
  $$\text{Giveaways}/90 = \text{Passes Attempted}/90 \times (1 - \text{Pass Accuracy})$$

#### 3.7.5. Reliability & Penalty/Error Shrinkage
* **Sample Reliability Factor:**
  $$R_{\text{sample}} = \frac{\text{Shots Faced Total}}{\text{Shots Faced Total} + 300}$$
* **Eligibility Cutoff:** $\text{Shots Faced Total} \ge 40$ (or $\text{Minutes Played} \ge 1350$).
* **Heavily Shrunk Penalties ($k=20$, Prior $\mu = 0.20$):**
  $$\text{Pen Save}_{\text{shrunk}} = \frac{\text{Pens Faced Total} \times \text{Pen Save \%} + 20 \times 0.20}{\text{Pens Faced Total} + 20}$$
* **Regressed Unforced Errors ($k=5000$ mins, Prior $\mu = 0.022$):**
  $$\text{Errors per } 90_{\text{shrunk}} = \frac{\text{Minutes} \times \text{Mistakes}/90 + 5000 \times 0.022}{\text{Minutes} + 5000}$$

---

### 3.8. Weighted Defensive Impact Score ($W_{\text{def}}$)
Used in `gold.vw_squad_central_defender_detailed_reports` to separate high-value anticipatory defending from last-ditch desperation clearances:

$$W_{\text{def}} = (\text{Interceptions}/90 \times 0.50) + (\text{Clearances}/90 \times 0.25) + ((\text{Blocks}/90 + \text{Shots Blocked}/90) \times 0.25)$$

* **Tactical Interpretation:** Prioritizes intelligence and positioning (interceptions carry 2x the weight of standard clearances).

---

### 3.9. Key Passes per Progressive Pass ($kP/\text{ProgP}$)
$$\text{kP per ProgP} = \frac{\text{Open-Play Key Passes per 90}}{\text{Progressive Passes per 90}}$$

* **Tactical Interpretation:** Evaluates whether a defender's forward passing translates directly into final-third shooting opportunities or merely recycles possession into midfield.

---

### 3.10. Moneyball "Value Gap" Index (Market Inefficiency Finder)

Traditional cost-efficiency ratios (such as $\text{cost per xGI}$) blow up or invert when performance is negative or near zero (especially for defensive players and goalkeepers). The **Value Gap Index** resolves this through non-parametric percentile ranking:

$$\text{Value Gap} = \text{Percentile}(\text{Tactical Output}) - \text{Percentile}(\text{Transfer Value})$$

* **Outfield Output:** $\text{Percentile}(\text{Non-Penalty xGoals} + \text{Expected Assists})$.
* **Goalkeeper Output:** $\text{Percentile}(\text{xGP per Shot}_{\text{shrunk}})$.
* **Cost Percentile:** $\text{Percentile}(\text{Transfer Value (£M)})$.
* **Scale:** Ranges from $-100.0$ to $+100.0$.
* **Scouting Interpretation:**
  * **$\text{Value Gap} \ge +50.0$ (💎 Elite Moneyball Bargain):** Player operates in the upper percentiles of performance but is priced in the lower tiers of market valuation (e.g. overlooked talent in South America or Eastern Europe).
  * **$\text{Value Gap} \le -30.0$ (⚠️ Premium Overpriced Risk):** Transfer fee or hype vastly outstrips underlying per-90 statistical contribution.

## 4. Scale-Weighted Euclidean Similarity Engine

### 4.1. Mathematical Formula

To find replacements for an existing starting player (e.g. an aging star or departing starter), the engine matches every scouted target in the database against the incumbent using a **scale-weighted Euclidean distance**:

$$D(\mathbf{Target}, \mathbf{Incumbent}) = \sqrt{\sum_{i=1}^{k} w_i \cdot \left(\text{Target}_i - \text{Incumbent}_i\right)^2}$$

Where:
* $w_i$ is the positional importance weight assigned to tactical dimension $i$.
* $\text{Target}_i$ is the target player's value for metric $i$.
* $\text{Incumbent}_i$ is the starting player's baseline value for metric $i$.

A similarity distance of $0.00$ indicates an identical statistical clone. The lower the distance, the closer the profile match.

### 4.2. Null-Safety & Baseline Imputation
In SQL Server, any expression containing `NULL` evaluates to `NULL`. To prevent players with missing stats from dropping out of similarity calculations, all metrics are guarded with `COALESCE` using neutral tactical baselines:

```sql
COALESCE(target_metric, baseline_value)
```

| Metric | Neutral Tactical Baseline |
| :--- | :--- |
| `shot_accuracy` | `0.40` (40% on target) |
| `conversion_rate` | `0.10` (10% conversion) |
| `pass_accuracy` | `0.80` (80% completion) |
| `open_play_cross_accuracy` | `0.20` (20% cross completion) |
| `tackle_accuracy` | `0.75` (75% tackle win rate) |
| `heading_accuracy` | `0.60` (60% aerial win rate) |
| `pressure_efficiency` | `0.20` (20% press win rate) |

---

## 5. Positional Metric Weight Allocations

### 5.1. Striker (ST) — `gold.vw_similar_strikers`
Matches attacking output, box presence, aerial ability, and clinical conversion:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `non_penalty_xGoals_per90` | $30.0$ | Core goal threat without penalty inflation. |
| `shots_on_target_per90` | $10.0$ | Volume of dangerous attempts on goal. |
| `shot_accuracy` | $15.0$ | Ball striking precision. |
| `conversion_rate` | $15.0$ | Clinical finishing efficiency. |
| `xGoals_per_shot` | $15.0$ | Shot selection quality (avoiding speculative efforts). |
| `aerial_duels_won_per90` | $10.0$ | Aerial target presence in the box. |
| `heading_accuracy` | $10.0$ | Aerial conversion rate. |
| `goal_threat_share` | $10.0$ | Proportion of player's xG relative to total team xG. |

---

### 5.2. Central Defender (CD) — `gold.vw_similar_central_defenders`
Matches aerial dominance, ball progression, positional discipline, and tackling:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `aerial_duels_won_per90` | $25.0$ | First-line box defense and set-piece defense. |
| `heading_accuracy` | $20.0$ | Aerial duel success rate. |
| `progressive_passes_per90` | $15.0$ | Ability to break the lines from deep. |
| `pass_accuracy` | $10.0$ | Reliability under pressure. |
| `tackle_accuracy` | $10.0$ | Ground duel efficiency. |
| `reactive_vs_positional` | $10.0$ | Tactical style matching (stepping up vs sweeping). |
| `discipline_index` | $5.0$ | Foul and card control to prevent costly penalties. |

---

### 5.3. Flank Defender / Fullback — `gold.vw_similar_flank_defenders`
Matches crossing threat, athletic work rate, ball progression, and ball security:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `completed_crosses_per90` | $25.0$ | Volume of successful deliveries into the box. |
| `open_play_cross_accuracy` | $15.0$ | Delivery efficiency. |
| `prog_rate_per_pass` | $20.0$ | Forward ball progression per pass attempted. |
| `turnover_rate` | $15.0$ | Ball retention and press resistance on the flank. |
| `tackle_accuracy` | $10.0$ | 1v1 defensive isolation stability. |
| `sprints_per90` | $15.0$ | High-intensity overlapping and recovery running. |

---

### 5.4. Defensive Midfielder (DM) — `gold.vw_similar_defensive_midfielders`
Matches ball-winning volume, aerial coverage, defensive pressure, and distribution:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `defensive_actions_per90` | $25.0$ | Total volume of defensive interventions. |
| `tackle_accuracy` | $15.0$ | Duel efficiency in defensive transition. |
| `reactive_vs_positional` | $15.0$ | Pressing vs shielding tendencies. |
| `aerial_duels_won_per90` | $15.0$ | Second-ball aerial dominance in the middle third. |
| `pass_accuracy` | $15.0$ | Security during buildup phases. |
| `pressure_efficiency` | $15.0$ | Success rate when pressing opposition midfielders. |

---

### 5.5. Central Midfielder (CM) — `gold.vw_similar_central_midfielders`
Matches progressive ball movement, creative share, athletic engine, and dual-phase involvement:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `progressive_passes_per90` | $25.0$ | Line-breaking pass delivery. |
| `progression_share` | $20.0$ | Share of team progression flowing through player. |
| `passes_attempted_per90` | $15.0$ | Touch volume and tempo dictation. |
| `pass_accuracy` | $15.0$ | Retention under pressure. |
| `npxg_plus_xa_per90` | $15.0$ | Dual-phase attacking threat from deep. |
| `pressure_efficiency` | $10.0$ | Counter-pressing effectiveness. |

---

### 5.6. Winger / Wide Attacker — `gold.vw_similar_wingers`
Matches 1v1 dribbling, box deliveries, chance creation quality, and direct scoring:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `npxg_plus_xa_per90` | $25.0$ | Combined direct goal involvement threat. |
| `dribbles_made_per90` | $20.0$ | 1v1 isolation ball-carrying ability. |
| `completed_crosses_per90` | $15.0$ | Quality of deliveries into the 18-yard box. |
| `chance_quality` ($xA/\text{OP-KP}$) | $15.0$ | Precision and danger of final passes. |
| `goal_threat_share` | $15.0$ | Proportion of attacking responsibility carried. |
| `sprints_per90` | $10.0$ | Off-the-ball running behind defensive lines. |

---

### 5.7. Attacking Midfielder (CAM) — `gold.vw_similar_attacking_midfielders`
Matches central chance creation, key pass efficiency, creative burden, and press resistance:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `npxg_plus_xa_per90` | $25.0$ | Direct goal contribution rate. |
| `open_play_key_passes_per90` | $20.0$ | Open-play defense-splitting pass volume. |
| `chance_quality` ($xA/\text{OP-KP}$) | $20.0$ | Danger of opportunities provided to strikers. |
| `progression_share` | $15.0$ | Central ball progression responsibility. |
| `turnover_rate` | $10.0$ | Ball retention in crowded attacking zones. |
| `pressure_efficiency` | $10.0$ | First-line pressing contribution. |

---

### 5.8. Goalkeeper (GK) — `gold.vw_similar_goalkeepers`
Matches shot-stopping delta, catching ability, sweep actions, and distribution:

| Metric | Weight ($w_i$) | Tactical Justification |
| :--- | :--- | :--- |
| `shot_stopping_delta` | $35.0$ | Goals prevented above expectation ($\text{Save \%} - \text{xSave \%}$). |
| `xGoals_prevented_per90` | $25.0$ | Absolute goals prevented per 90. |
| `hold_rate` | $15.0$ | Rebound control and secure handling. |
| `sweeper_index` | $15.0$ | Interceptions and clearances outside the 6-yard box. |
| `pass_accuracy` | $10.0$ | Buildup distribution precision. |

---

## 6. Tactical Archetypes Classification Engine

`gold.vw_tactical_archetypes` evaluates outfield metrics to automatically classify players into specific Football Manager tactical roles:

```mermaid
graph TD
    A[Outfield Player] --> B{Position}
    B -->|Striker| S1{Aerial Duels $\ge 4$ & Hdr Acc $\ge 55\%$?}
    S1 -->|Yes| TargetForward[Target Forward]
    S1 -->|No| S2{NPxG $\ge 0.35$ & Conv $\ge 15\%$?}
    S2 -->|Yes| Poacher[Poacher / Finisher]
    S2 -->|No| S3{xA $\ge 0.12$ & OP-KP $\ge 0.70$?}
    S3 -->|Yes| DLF[Complete / Deep-Lying Forward]
    S3 -->|No| PressingForward[Pressing / Advanced Forward]

    B -->|Central Defender| D1{Prog Rate $\ge 5.0$ & Prog Passes $\ge 4.0$?}
    D1 -->|Yes| BPD[Ball-Playing Defender]
    D1 -->|No| D2{Aerials $\ge 3.5$ & Hdr Acc $\ge 65\%$?}
    D2 -->|Yes| Stopper[Stopper / No-Nonsense]
    D2 -->|No| D3{Reactive Ratio $\ge 0.55$?}
    D3 -->|Yes| Cover[Cover / Interceptor]
    D3 -->|No| TradCD[Traditional Central Defender]

    B -->|Midfielder| M1{Passes $\ge 55$, Acc $\ge 88\%$, Prog $\ge 4.5$?}
    M1 -->|Yes| DLP[Deep-Lying Playmaker]
    M1 -->|No| M2{Def Actions $\ge 5.5$ & Press Eff $\ge 25\%$?}
    M2 -->|Yes| BWM[Ball-Winning Midfielder]
    M2 -->|No| M3{Dist $\ge 13\text{km}$ & Sprints $\ge 10$ & NPxG+xA $\ge 0.15$?}
    M3 -->|Yes| BBM[Box-to-Box Midfielder]
    M3 -->|No| Mezzala[Roaming Playmaker / Mezzala]
```

---

## 7. Financial Recruitment: Cost per xGI & Bargain Watch

To uncover market inefficiencies, `gold.vw_bargain_watch` evaluates player production against total amortized financial commitments through three distinct metrics:

### 7.1. Cost per xGI
Measures the total capital expenditure (acquisition fee plus annual wage) required to acquire one non-penalty expected goal involvement ($NPxG + xA$):

$$\text{Cost per xGI} = \frac{\text{Transfer Value (£M)} \times 1,000 + (\text{Weekly Wage (£k)} \times 52.0)}{\text{NPxG} + \text{xA}}$$

### 7.2. Attacking Bargain Index
Quantifies direct attacking output delivered per £1M of total annual financial cost:

$$\text{Attacking Bargain Index} = \frac{(\text{NPxG} + \text{xA}) \times 100.0}{\text{Transfer Value (£M)} + (\text{Weekly Wage (£k)} \times 0.052)}$$

* Cast as `DECIMAL(10,2)` to support extreme values without arithmetic overflow when acquiring high-performing youth or lower-league players on nominal wages (£20–£80/week).

### 7.3. Defensive Bargain Index
Quantifies defensive interventions delivered per £1M of total annual financial cost:

$$\text{Defensive Bargain Index} = \frac{\text{Defensive Actions per 90} \times 10.0}{\text{Transfer Value (£M)} + (\text{Weekly Wage (£k)} \times 0.052)}$$

* Cast as `DECIMAL(10,2)` to provide scale headroom for low-cost defensive stoppers.

### 7.4. Bargain Watch Qualification Criteria
* $Mins \ge 900$ (statistically valid competitive sample size).
* $is\_own\_squad = 0$ (external recruitment targets only).
* Budget Qualification: $\text{Transfer Value} \le £15.0\text{M}$ **OR** $\text{Weekly Wage} \le £30.0\text{k}$.
* Performance Qualification: $NPxG + xA \ge 0.30$ **OR** $\text{Defensive Actions}/90 \ge 5.00$.

* **Strategic Outcome:** Identifies players generating elite top-tier output playing for lower-reputation clubs or expiring contracts whose total cost per goal creation or defensive contribution is a fraction of market value.

---

## 8. Multi-Season Growth & Development Tracking

Using SCD Type 2 history across successive game seasons, `gold.vw_player_growth_tracking` computes exact trajectory deltas:

$$\Delta(\text{Metric}) = \text{Metric}_{\text{Season } t} - \text{Metric}_{\text{Season } t-1}$$

Tracked metrics include:
* $\Delta(NPxG + xA)$: Attacking development trajectory.
* $\Delta(\text{Progressive Passes})$: Line-breaking passing maturation.
* $\Delta(\text{Defensive Actions})$: Physical and defensive work rate trajectory.
* $\Delta(\text{Transfer Value})$: Asset appreciation/depreciation.

---

## 9. Advanced Recruitment & Squad Planning Engines

### 9.1. Interactive 4-Quadrant Scatter Matrix Explorer (`/scatter`)
Provides a dynamic visual analysis canvas modeled on modern analytics firms (Opta, FBref, StatsBomb):
* **2D Normalized Coordinate Scaling:** Normalizes any two selected metrics across qualified cohorts with statistical median and mean crosshair dividers.
* **Opta Quadrants:**
  * **Q1 (Top-Right):** High X / High Y (Elite output & volume leaders).
  * **Q2 (Top-Left):** High Y / Lower X (Specialist overperformers or high-efficiency technicians).
  * **Q3 (Bottom-Left):** Below median on both dimensions.
  * **Q4 (Bottom-Right):** High X / Lower Y (High volume / underperforming output).
* **Curated Presets:** Instant access to classic recruitment trade-offs (e.g. *Finishing Overperformance*, *Creation vs Threat*, *Progression vs Security*, *GK Shot Stopping vs Sweeping*, and *Moneyball Value Gap*).
* **Integrated Shortlisting & Search:** Interactive SVG dots with live search pulsing rings and 1-click ⭐ shortlisting.

---

### 9.2. Squad Succession & Replacement Engine (`/succession`)
Solves the strategic challenge of replacing aging starters (e.g. players aged 29–35+) before physical decline impacts league results:
* **Incumbent Starter Selection:** Select any player from Macará (or any warehouse club) as the replacement baseline.
* **Multidimensional Profile Similarity:** Computes normalized Euclidean similarity across key tactical pillars (progression, threat, ball retention, defending, and goalkeeping).
* **38-Game Season Upgrade Delta:**
  * **Outfield:** $\text{Impact} = (\text{Candidate } \text{NP-xGI/90} - \text{Incumbent } \text{NP-xGI/90}) \times 38$ (projected full-season goal contribution difference).
  * **Goalkeeper:** $\text{Impact} = (\text{Candidate } \text{xGP/90} - \text{Incumbent } \text{xGP/90}) \times 38$ (projected full-season goals prevented difference).
* **Financial Delta:** Computes annual wage difference and market value differentials.

---

### 9.3. Squad Tactical Gap & Depth Diagnosis (`/squad-diagnosis`)
Automated team-wide audit across 5 core departments:
1. **Goalkeeping (GK)**
2. **Central Defense (CB)**
3. **Fullbacks & Wingbacks (FB)**
4. **Central & Defensive Midfield (MID)**
5. **Attack & Wingers (ATT)**

* **Benchmarked Against League Medians:** Compares Macará's squad averages against LIGA MX medians.
* **Automated Risk Flags:** Generates 🟢 Healthy, 🟡 Monitor/Succession, and 🔴 Action Required diagnostic alerts.
* **One-Click Solution Routing:** Every diagnosed gap provides a direct "Scout Department Solutions" button linking to `/screener` or `/succession` with pre-filled filters.

---

### 9.4. Custom Shortlists & FM Export Engine (`/shortlist`)
* **Local Persistence:** Bookmarked targets persist across browser sessions with zero server lag.
* **FM-Ready CSV Export:** Generates standardized CSV files containing player keys, names, positions, clubs, valuations, wages, and scouting notes ready for Football Manager import or spreadsheet manipulation.
* **Clipboard Player ID Copying:** Copies comma-separated player keys with a single click for instant in-game searches.

---

## 10. Team-Level Tactical Frameworks & Outliers Engine

While individual player archetype profiling (`gold.vw_tactical_archetypes`) classifies player roles in isolation, football outputs are heavily conditioned by the team's overarching tactical system. Mapping tactical frameworks at the club level provides essential context to evaluate whether a player's statistics are elevated or suppressed by team instructions, and allows scouts to detect **system outliers**.

### 10.1. Minutes-Weighted Team Metric Aggregation

To avoid giving equal weight to fringe squad members with brief appearances, team-level per-90 rates are calculated as a **playing-time weighted average**:

$$\bar{M}_{\text{team}} = \frac{\sum_{i=1}^{n} M_i \times \text{Minutes}_i}{\sum_{i=1}^{n} \text{Minutes}_i}$$

Where $M_i$ represents player $i$'s per-90 rate and $\text{Minutes}_i$ represents their competitive playing time ($\ge 180$ mins).

### 10.2. Categorization of 7 Team Tactical Frameworks

| Framework | Analytical Context | Expected Metric Spikes (↑) | Expected Metric Drops (↓) |
| :--- | :--- | :--- | :--- |
| **1. High-Intensity Pressing** (*Gegenpressing*) | Designed to bypass defensive transition by hunting the ball in advanced areas immediately upon turnover. | `pressures_attempted_per90`, `pressures_completed_per90`, `possession_won_per90`, `interceptions_per90`, `sprints_per90`, `distance_covered_km_per90`, `fouls_made_per90` | None inherently; `possession_lost_per90` rises due to verticality. |
| **2. Possession & Control** (*Tiki-Taka / Control Possession*) | Risk-averse, patient horizontal probing to monopolize the ball and dictate tempo. Defending is depressed as the opponent lacks the ball. | `passes_attempted_per90`, `pass_accuracy`, `xGoals_per_shot` | `possession_lost_per90`, `tackles_attempted_per90`, `interceptions_per90`, `clearances_per90` |
| **3. Penetrative Possession** (*Vertical Tiki-Taka*) | Blends possession dominance with aggressive, line-breaking passes through central channels. | `passes_attempted_per90`, `progressive_passes_per90`, `open_play_key_passes_per90`, `chances_created_per90` | `crosses_attempted_per90` (focus is almost entirely central) |
| **4. Transition & Space Exploitation** (*Counter-Attacking*) | • **Fluid Counter:** Absorbs pressure deep; mobile forwards carry and combine quickly.<br>• **Direct Counter:** Bypasses midfield upon winning ball, launching forward to runners. | • *Fluid:* `dribbles_made_per90`, `sprints_per90`, `blocks_per90`<br>• *Direct:* `progressive_passes_per90`, `clearances_per90` | • *Direct:* `pass_accuracy`, `passes_attempted_per90` |
| **5. Horizontal Stretching** (*Wing Play*) | Bypasses central congestion by overloading flanks, isolating wide players, and creating crossing opportunities. | `open_play_crosses_attempted_per90`, `crosses_attempted_per90`, `dribbles_made_per90`, `headers_attempted_per90`, `key_headers_per90` | None inherently. |
| **6. Aerial & Vertical Attrition** (*Route One / Long Ball*) | Forgoes midfield buildup entirely, relying on defensive solidity, aerial combat, and fighting for second balls. | `headers_attempted_per90`, `heading_accuracy`, `clearances_per90`, `possession_won_per90` | `passes_attempted_per90`, `pass_accuracy` |
| **7. Deep Block Defending** (*Catenaccio / Low Block*) | Risk-averse survival tactics focused on protecting the penalty box, frustrating opposition, and nullifying space. | `blocks_per90`, `shots_blocked_per90`, `clearances_per90`, `key_tackles_per90` | `pressures_attempted_per90` (passive engagement), `shots_per90`, `xGoals_per90`, `progressive_passes_per90` |

### 10.3. Directional Outlier Heuristics

`gold.vw_player_system_outliers` programmatically identifies players whose performance contradicts their team's statistical signature:

1. **High-Precision Striker in Direct System:** A striker maintaining $\ge 84\%$ pass accuracy and $\ge 12.0$ passes/90 in a Route One or Direct Counter team. Indicates elite technical link-up play that thrives even when starved of possession.
2. **High-Clearance Defender in Tiki-Taka:** A defender logging $\ge 1.45$ clearances/90 in a Tiki-Taka or Vertical Tiki-Taka side. Identifies a resilient stopper capable of executing emergency box defense even in a possession-heavy team.
3. **Attacking Talisman in Deep Block:** An attacker or midfielder producing $NPxG + xA \ge 0.35$/90 in a Catenaccio / Deep Block team. Identifies high-efficiency match winners delivering top-tier goal threat despite low system volume.
4. **Lone Pressing Engine:** A forward or attacking midfielder executing $\ge 13.0$ pressures/90 in a passive, low-intensity team ($\le 7.2$ team pressures/90).
5. **Dynamic Ball Carrier in Direct System:** A player recording $\ge 1.80$ dribbles/90 in a Route One system, bypassing aerial attrition through individual ground carries.
6. **Central Incision Playmaker in Flank System:** An attacking midfielder logging $\ge 1.30$ open-play key passes/90 with $\le 25\%$ crossing share in a Wing Play system.
7. **Ball-Playing Quarterback in Direct System:** A central defender completing $\ge 4.50$ progressive passes/90 with $\ge 86\%$ pass accuracy in a direct or Route One side.
8. **Ball Retention Hub in Gegenpress:** A central or defensive midfielder maintaining $\ge 89\%$ pass accuracy and $\le 14\%$ turnover rate in a high-chaos Gegenpressing system.

