import 'dart:async';
import 'dart:math' as math;

import 'package:flutter/material.dart';

import 'widgets/orbit_words.dart';
import 'widgets/timer_ring.dart';
import 'widgets/word_strip.dart';

enum GamePhase {
  idle,
  countdown,
  running,
  finished,
}

class GameScreen extends StatefulWidget {
  const GameScreen({super.key});

  @override
  State<GameScreen> createState() => _GameScreenState();
}

class _GameScreenState extends State<GameScreen> with SingleTickerProviderStateMixin {
  static const int _gameSeconds = 60;
  static const String _rhymePrefix = 'Egunerko errima...';
  static const String _rhymeSuffixMvp = '-ina';

  final TextEditingController _playerController = TextEditingController();
  final TextEditingController _wordController = TextEditingController();
  final FocusNode _wordFocusNode = FocusNode();

  GamePhase _phase = GamePhase.idle;
  Timer? _countdownTimer;
  int _countdownValue = 3;

  late final AnimationController _roundController;
  final List<String> _words = <String>[];

  @override
  void initState() {
    super.initState();
    _playerController.addListener(() {
      // Enables/disables the Jokatu button.
      if (!mounted) return;
      setState(() {});
    });
    _roundController = AnimationController(
      vsync: this,
      duration: const Duration(seconds: _gameSeconds),
    )..addStatusListener((status) {
        if (status == AnimationStatus.dismissed) {
          // Completed reverse(1->0)
          _finishRound();
        }
      });
  }

  @override
  void dispose() {
    _countdownTimer?.cancel();
    _playerController.dispose();
    _wordController.dispose();
    _wordFocusNode.dispose();
    _roundController.dispose();
    super.dispose();
  }

  void _startCountdown() {
    if (_phase != GamePhase.idle) return;

    setState(() {
      _words.clear();
      _wordController.clear();
      _countdownValue = 3;
      _phase = GamePhase.countdown;
    });

    _countdownTimer?.cancel();
    _countdownTimer = Timer.periodic(const Duration(seconds: 1), (t) {
      if (_countdownValue == 3) {
        setState(() {
          _countdownValue = 2;
        });
        return;
      }
      if (_countdownValue == 2) {
        setState(() {
          _countdownValue = 1;
        });
        return;
      }

      // _countdownValue == 1: keep "1! Aurrera" visible for this second, then start.
      t.cancel();
      _startRound();
    });
  }

  void _startRound() {
    _countdownTimer?.cancel();
    setState(() {
      _phase = GamePhase.running;
    });

    // Start a 60s round. Controller runs from 1.0 down to 0.0.
    _roundController.stop();
    _roundController.value = 1.0;
    _roundController.reverse(from: 1.0);

    // Ensure the user can type immediately.
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (!mounted) return;
      _wordFocusNode.requestFocus();
    });
  }

  void _finishRound() {
    if (_phase != GamePhase.running) return;
    setState(() {
      _phase = GamePhase.finished;
    });
    _wordFocusNode.unfocus();
  }

  void _resetToIdle() {
    _countdownTimer?.cancel();
    _roundController.stop();
    _roundController.value = 1.0;
    setState(() {
      _phase = GamePhase.idle;
      _countdownValue = 3;
      _words.clear();
      _wordController.clear();
    });
  }

  void _submitWord(String raw) {
    if (_phase != GamePhase.running) return;
    final String word = raw.trim();
    if (word.isEmpty) return;

    setState(() {
      _words.add(word);
    });
    _wordController.clear();

    // Keep focus so the user can keep typing fast.
    _wordFocusNode.requestFocus();
  }

  String _countdownText() {
    if (_countdownValue <= 1) return '1! Aurrera';
    return '$_countdownValue';
  }

  int _remainingSeconds() {
    // Controller is 1.0 -> 0.0. Use ceil so we start at 60.
    return math.max(0, (_roundController.value * _gameSeconds).ceil());
  }

  @override
  Widget build(BuildContext context) {
    final ThemeData theme = Theme.of(context);

    return Scaffold(
      body: SafeArea(
        child: AnimatedBuilder(
          animation: _roundController,
          builder: (context, _) {
            final bool showHeader =
                _phase == GamePhase.running || _phase == GamePhase.finished;
            final bool showPlayerName = _phase != GamePhase.idle;
            final int remaining = _remainingSeconds();
            final double progress = _roundController.value;
            final String playerName = _playerController.text.trim();

            return Padding(
              padding: const EdgeInsets.all(20),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  // Header
                  SizedBox(
                    height: 120,
                    child: Stack(
                      children: [
                        if (showHeader)
                          const Align(
                            alignment: Alignment.center,
                            child: _RhymeHeader(
                              prefix: _rhymePrefix,
                              suffix: _rhymeSuffixMvp,
                            ),
                          ),
                        if (showPlayerName && playerName.isNotEmpty)
                          Align(
                            alignment: Alignment.topRight,
                            child: Container(
                              padding: const EdgeInsets.symmetric(
                                horizontal: 10,
                                vertical: 6,
                              ),
                              decoration: BoxDecoration(
                                color: theme.colorScheme.surfaceContainerHighest,
                                borderRadius: BorderRadius.circular(999),
                              ),
                              child: Text(
                                playerName,
                                style: theme.textTheme.labelLarge?.copyWith(
                                  fontWeight: FontWeight.w700,
                                ),
                                overflow: TextOverflow.ellipsis,
                              ),
                            ),
                          ),
                      ],
                    ),
                  ),
                  const SizedBox(height: 16),

                  // Main area
                  Expanded(
                    child: Center(
                      child: _buildMainArea(
                        theme: theme,
                        remaining: remaining,
                        progress: progress,
                      ),
                    ),
                  ),

                  const SizedBox(height: 16),

                  // Bottom input + words
                  if (_phase == GamePhase.idle) ...[
                    TextField(
                      controller: _playerController,
                      decoration: const InputDecoration(
                        labelText: '[Player]',
                        border: OutlineInputBorder(),
                      ),
                      textInputAction: TextInputAction.done,
                    ),
                    const SizedBox(height: 12),
                    SizedBox(
                      height: 48,
                      child: ElevatedButton(
                        onPressed: playerName.isEmpty ? null : _startCountdown,
                        child: const Text('Jokatu'),
                      ),
                    ),
                  ] else ...[
                    TextField(
                      controller: _wordController,
                      focusNode: _wordFocusNode,
                      enabled: _phase == GamePhase.running,
                      decoration: const InputDecoration(
                        hintText: 'Idatzi hitza eta sakatu Enter',
                        border: OutlineInputBorder(),
                      ),
                      textInputAction: TextInputAction.done,
                      onSubmitted: _submitWord,
                    ),
                    const SizedBox(height: 12),
                    WordStrip(words: _words),
                    if (_phase == GamePhase.finished) ...[
                      const SizedBox(height: 12),
                      SizedBox(
                        height: 48,
                        child: OutlinedButton(
                          onPressed: _resetToIdle,
                          child: const Text('Hasierara joan'),
                        ),
                      ),
                    ],
                  ],
                ],
              ),
            );
          },
        ),
      ),
    );
  }

  Widget _buildMainArea({
    required ThemeData theme,
    required int remaining,
    required double progress,
  }) {
    if (_phase == GamePhase.countdown) {
      return Text(
        _countdownText(),
        textAlign: TextAlign.center,
        style: theme.textTheme.displayMedium?.copyWith(
          fontWeight: FontWeight.w700,
        ),
      );
    }

    if (_phase == GamePhase.idle) {
      return Text(
        'Prest?',
        textAlign: TextAlign.center,
        style: theme.textTheme.headlineMedium?.copyWith(
          fontWeight: FontWeight.w600,
        ),
      );
    }

    // running or finished
    final bool finished = _phase == GamePhase.finished;
    return Column(
      mainAxisSize: MainAxisSize.min,
      children: [
        Stack(
          alignment: Alignment.center,
          children: [
            SizedBox(
              width: 220,
              height: 220,
              child: TimerRing(
                progress: finished ? 0.0 : progress,
                backgroundColor: theme.colorScheme.surfaceContainerHighest,
                foregroundColor: theme.colorScheme.primary,
                strokeWidth: 14,
              ),
            ),
            OrbitWords(
              words: _words,
              radius: 140,
            ),
            Text(
              '${finished ? 0 : remaining}',
              style: theme.textTheme.displayLarge?.copyWith(
                fontWeight: FontWeight.w800,
                letterSpacing: -1,
              ),
            ),
          ],
        ),
        const SizedBox(height: 16),
        if (finished)
          Text(
            'Denbora!',
            style: theme.textTheme.headlineMedium?.copyWith(
              fontWeight: FontWeight.w700,
              color: theme.colorScheme.error,
            ),
          ),
      ],
    );
  }
}

class _RhymeHeader extends StatelessWidget {
  const _RhymeHeader({
    required this.prefix,
    required this.suffix,
  });

  final String prefix;
  final String suffix;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Column(
      mainAxisSize: MainAxisSize.min,
      children: [
        Text(
          prefix,
          textAlign: TextAlign.center,
          style: theme.textTheme.headlineMedium?.copyWith(
            fontWeight: FontWeight.w800,
          ),
        ),
        const SizedBox(height: 8),
        Text(
          suffix,
          textAlign: TextAlign.center,
          style: theme.textTheme.displaySmall?.copyWith(
            fontWeight: FontWeight.w900,
            decoration: TextDecoration.underline,
          ),
        ),
      ],
    );
  }
}
