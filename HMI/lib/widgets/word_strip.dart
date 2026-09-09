import 'package:flutter/material.dart';

class WordStrip extends StatelessWidget {
  const WordStrip({
    super.key,
    required this.words,
  });

  final List<String> words;

  @override
  Widget build(BuildContext context) {
    if (words.isEmpty) {
      return const SizedBox(
        height: 44,
        child: Align(
          alignment: Alignment.centerLeft,
          child: Text(''),
        ),
      );
    }

    return SizedBox(
      height: 44,
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: Row(
          children: [
            for (int i = 0; i < words.length; i++) ...[
              Chip(
                label: Text(words[i]),
                backgroundColor: _chipColor(i),
                visualDensity: VisualDensity.compact,
              ),
              const SizedBox(width: 8),
            ],
          ],
        ),
      ),
    );
  }

  Color _chipColor(int index) {
    final double hue = (index * 47) % 360;
    final hsl = HSLColor.fromAHSL(1.0, hue, 0.55, 0.82);
    return hsl.toColor().withValues(alpha: 0.85);
  }
}
