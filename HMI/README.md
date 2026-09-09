# HMI (Flutter MVP)

This folder contains a minimal Flutter UI MVP for the BertsonalTrainer project.

## What is implemented
- `[Player]` input + `Jokatu` start button
- Pre-start countdown: `3`, `2`, `1! Aurrera`
- 60s round with:
  - Header: `Eguneko errima...` + suffix (MVP fixed: `-ina`, bigger + bold)
  - Decreasing circular progress ring
  - Big timer `60` → `0`
  - Word entry; Enter submits into a horizontal list
- End state: `Denbora!` (header remains visible; input disabled)

## Running it
This repository environment may not have Flutter installed.

On a machine with Flutter installed:
1. Open a terminal in `HMI/`
2. Run `flutter pub get`
3. Run `flutter run`

If you need platform scaffolding (android/ios/windows/etc.) generated, you can run:
- `flutter create .`

This should preserve the existing `lib/` code.
