import 'dart:math' as math;

import 'package:flutter/material.dart';

class OrbitWords extends StatelessWidget {
  const OrbitWords({
    super.key,
    required this.words,
    required this.radius,
    this.maxWords = 24,
  });

  final List<String> words;
  final double radius;
  final int maxWords;

  @override
  Widget build(BuildContext context) {
    if (words.isEmpty) return const SizedBox.shrink();

    // Keep the UI readable: orbit the most recent words only.
    final List<String> orbitWords =
        words.length <= maxWords ? words : words.sublist(words.length - maxWords);

    return IgnorePointer(
      child: LayoutBuilder(
        builder: (context, constraints) {
          final double w = constraints.maxWidth;
          final double h = constraints.maxHeight;
          final Offset center = Offset(w / 2, h / 2);
          final int n = orbitWords.length;

          final List<Widget> positioned = <Widget>[];
          for (int i = 0; i < n; i++) {
            // Start at top and proceed clockwise.
            final double angle = (-math.pi / 2) + ((2 * math.pi) * (i / n));
            final Offset p = center + Offset(
              radius * math.cos(angle),
              radius * math.sin(angle),
            );

            final Color bg = _chipColor(i);
            positioned.add(
              Positioned(
                left: p.dx,
                top: p.dy,
                child: FractionalTranslation(
                  translation: const Offset(-0.5, -0.5),
                  child: Container(
                    constraints: const BoxConstraints(maxWidth: 88),
                    padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                    decoration: BoxDecoration(
                      color: bg,
                      borderRadius: BorderRadius.circular(999),
                      border: Border.all(
                        color: Colors.black.withValues(alpha: 0.06),
                      ),
                    ),
                    child: Text(
                      orbitWords[i],
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: Theme.of(context).textTheme.labelMedium?.copyWith(
                            fontWeight: FontWeight.w700,
                          ),
                    ),
                  ),
                )
              ),
            );
          }

          return Stack(children: positioned);
        },
      ),
    );
  }

  Color _chipColor(int index) {
    // Distinct, soft shading per word.
    final double hue = (index * 47) % 360;
    final hsl = HSLColor.fromAHSL(1.0, hue, 0.55, 0.80);
    return hsl.toColor().withValues(alpha: 0.85);
  }
}
