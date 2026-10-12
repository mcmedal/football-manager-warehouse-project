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

### 3.2. Shrunk Finishing Delta
In football analytics, finishing overperformance ($\text{Goals} - xG$) is notoriously noisy and heavily regresses toward the mean over multi-season horizons. To prevent overpaying for strikers experiencing short-term hot streaks over limited minutes, we apply **Empirical Bayes shrinkage toward zero**:

$$\text{Finishing Delta}_{\text{shrunk}} = (\text{xG-OP}) \times \left(\frac{\text{Minutes Played}}{\text{Minutes Played} + 900}\right)$$

* At $900$ minutes (10 matches), the observed overperformance is shrunk by $50\%$.
* At $2,700$ minutes (30 matches), the shrinkage factor stabilizes at $75\%$, placing higher confidence in persistent finishing skill.

---

### 3.3. Aerial Dominance
$$\text{Aerial Duels Won per 90} = \text{Headers Attempted per 90} \times \text{Heading Accuracy \%}$$

Both volume ($\text{Aer Won}/90$) and efficiency ($\text{Heading Accuracy \%}$) are evaluated independently: a centerback winning 3 aerial duels at 75% accuracy is tactically distinct from a target man contesting 10 aerial duels at 45% accuracy.

---

### 3.4. Crossing Threat Index
$$\text{Completed Crosses per 90} = \text{Crosses Attempted per 90} \times \text{Cross Accuracy \%}$$

Separating completed volume from raw attempts prevents high-volume, low-efficiency fullbacks from inflating their tactical score.

---

### 3.5. Total Defensive Actions & Reactive Defending Ratio
$$\text{Defensive Actions per 90} = (\text{Tackles Attempted}/90 \times \text{Tackle Accuracy}) + \text{Interceptions}/90 + \text{Clearances}/90 + \text{Blocks}/90$$

$$\text{Reactive vs Positional Ratio} = \frac{\text{Tackles Won}/90 + \text{Interceptions}/90}{\text{Defensive Actions}/90}$$

* **High Ratio ($\ge 0.55$):** Aggressive, reactive stopper or interceptor who steps up to challenge ball-carriers.
* **Low Ratio ($\le 0.40$):** Positional, low-block defender whose primary outputs are box clearances and shot blocks.

---

### 3.6. Ball Retention & Turnover Rate
$$\text{Turnover Rate} = \frac{\text{Possession Lost per 90}}{\text{Passes Attempted}/90 + \text{Dribbles}/90 + \text{Crosses}/90}$$

* Measures ball security relative to total on-ball touches. Critical for deep-lying playmakers and inverted fullbacks where possession loss is catastrophic.

---

### 3.7. Goalkeeper Performance Metrics
* **Shot Stopping Delta:**
  $$\Delta_{\text{Shot Stopping}} = \text{Save \%} - \text{Expected Save (xSave) \%}$$
* **Hold Rate %:**
  $$\text{Hold Rate} = \frac{\text{Saves Held per 90}}{\text{Saves Made per 90}}$$
* **Sweeper Keeper Index:**
  $$\text{Sweeper Index} = \text{Clearances per 90} + \text{Distance Covered (km) per 90}$$
  *(In longitudinal growth tracking, calculated as $\text{Clearances}/90 + \text{Interceptions}/90$)*
* **Distribution Value:**
  $$\text{Distribution Value} = \text{Passes Attempted per 90} \times \text{Pass Accuracy \%}$$

---

### 3.8. Weighted Defensive Impact Score ($W_{\text{def}}$)
Used in `gold.vw_squad_central_defender_detailed_reports` to separate high-value anticipatory defending from last-ditch desperation clearances:

$$W_{\text{def}} = (\text{Interceptions}/90 \times 0.50) + (\text{Clearances}/90 \times 0.25) + ((\text{Blocks}/90 + \text{Shots Blocked}/90) \times 0.25)$$

* **Tactical Interpretation:** Prioritizes intelligence and positioning (interceptions carry 2x the weight of standard clearances).

---

### 3.9. Key Passes per Progressive Pass ($kP/\text{ProgP}$)
$$\text{kP per ProgP} = \frac{\text{Open-Play Key Passes per 90}}{\text{Progressive Passes per 90}}$$

* **Tactical Interpretation:** Evaluates whether a defender's forward passing translates directly into final-third shooting opportunities or merely recycles possession into midfield.

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

$$\text{Cost per xGI} = \frac{\text{Transfer Value (€M)} \times 1,000 + (\text{Weekly Wage (€k)} \times 52.0)}{\text{NPxG} + \text{xA}}$$

### 7.2. Attacking Bargain Index
Quantifies direct attacking output delivered per €1M of total annual financial cost:

$$\text{Attacking Bargain Index} = \frac{(\text{NPxG} + \text{xA}) \times 100.0}{\text{Transfer Value (€M)} + (\text{Weekly Wage (€k)} \times 0.052)}$$

* Cast as `DECIMAL(10,2)` to support extreme values without arithmetic overflow when acquiring high-performing youth or lower-league players on nominal wages (£20–£80/week).

### 7.3. Defensive Bargain Index
Quantifies defensive interventions delivered per €1M of total annual financial cost:

$$\text{Defensive Bargain Index} = \frac{\text{Defensive Actions per 90} \times 10.0}{\text{Transfer Value (€M)} + (\text{Weekly Wage (€k)} \times 0.052)}$$

* Cast as `DECIMAL(10,2)` to provide scale headroom for low-cost defensive stoppers.

### 7.4. Bargain Watch Qualification Criteria
* $Mins \ge 900$ (statistically valid competitive sample size).
* $is\_own\_squad = 0$ (external recruitment targets only).
* Budget Qualification: $\text{Transfer Value} \le €15.0\text{M}$ **OR** $\text{Weekly Wage} \le €30.0\text{k}$.
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
