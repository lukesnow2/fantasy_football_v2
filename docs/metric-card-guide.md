# Writing metric cards

Every stat with an ⓘ opens the same card, and every card reads from one row of
`meta_data.metric_definitions`. The card shows the name, the category, the
description, Calculation and How to read it. The Data Dictionary adds the range
and band tiles, a worked example and Limitations. `detailed_description` isn't
shown anywhere yet, so nothing a reader needs can live only there.

These rules come from a research pass in November 2026 (NN/g, Carbon, W3C,
FanGraphs, FiveThirtyEight, Spiegelhalter's risk-communication group and the
plain-language research behind them). The short version: answer the reader's
question, not the builder's, and keep the machinery one click away.

## The card

**Length.** The whole card stays around 60-80 words. People read about a fifth
of what's on a screen, so assume most readers stop after the description.

**Name.** Use the label the page shows, so the card matches what was clicked.
"Playoff odds", not "Playoff Probability". Sentence case.

**Description** (`short_description`). One sentence, 20 words or fewer, that
answers "what question does this number answer?" Don't lean on the metric's own
name ("Luck: how lucky a team was" tells you nothing). Model outputs open with
what they are: "The chance that...", "An estimate of...", "The wins a team's
scores would have earned...".

**Calculation** (`calculation_formula`). It prints in a monospace box, so it's a
word equation, never prose: fan words joined by ÷ × − +.
`Wins + half of ties − expected wins`. No exponents, Greek letters, column names
or full sentences. Name every kind of input. A composite gives its weights here
and nowhere else on the card. A simulation says what's counted over what:
`Seasons finished in a playoff spot ÷ 10,000 simulated seasons`.

**How to read it** (`interpretation_guide`). At most three sentences, none over
25 words.

1. Start with the anchor: what average or zero is, which direction is better,
   and the unit. Spell out any sign that runs against intuition every time.
2. Translate into something fans already count, like wins, points a week or
   seasons out of 10. "+1.6 is about one and a half more wins than the scoring
   earned." "0.84 is a score out of 1, not a chance of winning."
3. Add a "what it isn't" clause wherever a misreading is likely: expected wins
   aren't a forecast, a 0-1 score isn't a probability, odds aren't a ranking.
4. When a number can disagree with what readers expect, give the cause.
5. At most one limitation, and only one that changes how most readers read the
   number. The rest go in the Dictionary.

## Odds and other chances

- Odds show to one decimal, and never as a rounded 100% or 0% unless the
  outcome is settled ("Clinched", "Eliminated").
- Name the miss case as a whole-number frequency out of 10 (or out of 100 for
  small chances): "A team at 70% still misses about 3 seasons in 10." Never
  repeat the tenths in prose.
- No "likely" or "good chance" without the number beside it. People read those
  words all over the map.
- Accuracy is a calibration sentence with counts ("teams given 85 to 95% made
  it 90% of the time"), led by a plain comparison. A Brier score only ever
  appears in parentheses after that.

## The Data Dictionary

**Range and bands.** The Range tile is this league's observed range. Good and
Excellent are only for performance scales, and they come from this league's own
history (say, top third and top tenth of team-seasons), never from "industry
benchmarks". Chances and zero-sum numbers like luck get no bands: leave both
thresholds empty.

**Worked example** (`example_calculation`). One realistic team, every step
shown, ending on the exact value in the format the page prints it. Fake-precise
weightings that the code doesn't run are worse than no example.

**Limitations.** Concrete and checkable: what the number can't see, what sample
it needs, how well it's held up. Say whether it looks back or forward, and
whether it counts the season in progress. Vague hedges ("results may vary")
cost trust; specific ones don't.

**Jargon.** Describe the thing, not the method's name. "Played out 10,000
times", not "Monte Carlo". "Pulled toward the league average early on", not
"shrinkage". "The record your points would usually produce", not
"Pythagorean". A technical name can appear once in the Dictionary, after the
plain description.

## House rules that still apply

- Copy is evergreen: it describes the number, never this season's story.
- Career cards count the live season in totals; season records, best and worst
  seasons, and outcomes wait for a season to finish.
- No em or en dashes, contractions in prose, American spelling. Run
  `~/.claude/skills/luke-voice/scripts/lint_copy.py` on new copy.
- Every number in the copy has to match what the code computes. When the code
  changes, the card changes in the same PR.

## Before it ships

Someone who didn't build the metric reads the card cold and answers three
questions: what does this number answer, is this value good, and what would
make it wrong? A miss on any of them means a rewrite.
