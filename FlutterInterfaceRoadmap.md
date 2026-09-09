# Flutter Interface Roadmap (MVP)

## MVP Target
Create a **standalone Flutter UI** inside this backend repo under a single top-level folder: `HMI/`.

The app is **single-user**, single-device, no auth, no persistence. Backend integration is deferred: the rhyme suffix will be fixed for now.

## Required Behavior
1. **Idle screen**
   - Text field labeled `[Player]`
   - Primary button: `Jokatu`

2. **On `Jokatu`**
   - Show a centered reverse countdown: `3`, then `2`, then `1! Aurrera` (1 second each)
   - During this countdown: the header is **NOT visible**
   - Immediately after `Aurrera`, the 60s game starts

3. **Running screen (60 → 0)**
   - Top header (visible for the entire 60s run):
     - Text: `Eguneko errima...`
     - Suffix: fixed for MVP (example: `-ina`), but **designed to come from backend later**
     - Suffix styling: **bigger** and **bold**
   - Circular border/progress ring that is shaded and **decreases** with time
   - Big timer in the center going `60` down to `0`
   - While running, the user can type words
   - Pressing Enter submits the current word and it appears in a **horizontal list**
   - Keep the input focused after submitting

4. **Finished screen**
   - When time ends: show `Denbora!`
   - Keep header `Eguneko errima... -ina` visible (suffix still bigger/bold)
   - Keep submitted words visible
   - Disable word input

## Non-Goals (MVP)
- No backend calls yet (suffix is fixed)
- No scoring, persistence, or multiplayer
- No complex navigation

## Folder/Layout Plan (Standalone `HMI/`)
Create a Flutter project rooted at `HMI/` so it stays isolated from the backend Python/services.

Minimal structure:
- `HMI/pubspec.yaml`
- `HMI/lib/main.dart` (app entry)
- `HMI/lib/game_screen.dart` (UI + state)
- `HMI/lib/widgets/timer_ring.dart` (progress ring)
- `HMI/lib/widgets/word_strip.dart` (horizontal list)

## UX Layout Spec (Clean Interface)
- Single page (`Scaffold` + `SafeArea`)
- Layout zones:
  - Top: header (only in Running and Finished)
  - Middle: ring + timer (Running/Finished)
  - Bottom: word input + horizontal list

Header styling:
- `Eguneko errima...` regular
- Suffix (e.g. `-ina`) larger font (e.g. 28–34) and bold

Words list:
- Each word rendered as a compact `Chip`
- Horizontally scrollable

## State Machine
Define:
- `enum GamePhase { idle, countdown, running, finished }`

State:
- `playerName: String`
- `phase: GamePhase`
- `countdownValue: int` (3..1)
- `rhymeSuffix: String` (MVP fixed: `-ina`)
- `words: List<String>`

## Timing (Recommended)
- Countdown: `Timer.periodic(1s)` showing `3 → 2 → 1! Aurrera`, then transition to Running
- Running: `AnimationController(duration: 60s)` from 1.0 down to 0.0
  - Ring progress = controller value
  - Remaining seconds = `(controller.value * 60).ceil()`
  - On complete: transition to Finished

## Parallel Workstreams (Merge-Friendly)
1. UI Skeleton + Styling
2. Countdown + Phase Transitions
3. Timer Ring + Big Timer
4. Word Entry + Horizontal List
5. Polish (focus management, re-entry protection)

## Acceptance Criteria (MVP)
- `Jokatu` triggers `3`, `2`, `1! Aurrera` with 1-second cadence
- Header appears only after countdown completes and stays through Finished
- Timer starts at 60 and reaches 0
- Ring visibly decreases over 60 seconds
- Enter submits words during the run into a horizontal list
- `Denbora!` appears at the end; input disabled
