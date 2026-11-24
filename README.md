# pump-fun-paper-sprint

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/TUHIN729/pump-fun-paper-sprint/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/TUHIN729/pump-fun-paper-sprint/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A small-stakes, wide-trail trader for the pump.fun paper sprint: it buys every coin at its first buyable moment — a few trades in, real reserves built enough for a minimum-bar order, buyers still outspending sellers — with a fixed 0.03 SOL stake, then holds with a wide trailing exit so a real runner can pay for the losers and a hard stop plus a time stop for stale positions. It rotates across as many coins as the five-slot cap allows, to clear the breadth bar with room to spare, and a portfolio-level brake halts new entries if equity ever sags, keeping maximum drawdown far inside the 50% guardrail. The shape comes from the dev window itself: at one decision per slot, directional rules (momentum, breakouts, dips) all lose to the ~2.5% round-trip fees, while buying young coins and holding them averages +11% to +36% per 6 hours, driven by a few coins that run 2x–13x — so the bot plays the cross-section, not the tape. Fully deterministic: every decision is a pure function of the replay stream, so two runs on the same data emit byte-identical orders.

## Run it

```sh
node main.ts
```

Built for the [make.fun](https://make.fun/?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) bounty "Pump.fun Paper Sprint: Any Bot Can Win".
