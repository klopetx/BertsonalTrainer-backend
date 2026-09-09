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
            for (final w in words) ...[
              Chip(
                label: Text(w),
                visualDensity: VisualDensity.compact,
              ),
              const SizedBox(width: 8),
            ],
          ],
        ),
      ),
    );
  }
}
