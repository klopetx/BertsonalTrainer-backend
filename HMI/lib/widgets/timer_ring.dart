import 'dart:math' as math;

import 'package:flutter/material.dart';

class TimerRing extends StatelessWidget {
  const TimerRing({
    super.key,
    required this.progress,
    required this.backgroundColor,
    required this.foregroundColor,
    this.strokeWidth = 12,
  });

  /// Remaining fraction in range [0, 1].
  final double progress;
  final Color backgroundColor;
  final Color foregroundColor;
  final double strokeWidth;

  @override
  Widget build(BuildContext context) {
    return CustomPaint(
      painter: _TimerRingPainter(
        progress: progress.clamp(0.0, 1.0),
        backgroundColor: backgroundColor,
        foregroundColor: foregroundColor,
        strokeWidth: strokeWidth,
      ),
    );
  }
}

class _TimerRingPainter extends CustomPainter {
  _TimerRingPainter({
    required this.progress,
    required this.backgroundColor,
    required this.foregroundColor,
    required this.strokeWidth,
  });

  final double progress;
  final Color backgroundColor;
  final Color foregroundColor;
  final double strokeWidth;

  @override
  void paint(Canvas canvas, Size size) {
    final Offset center = size.center(Offset.zero);
    final double radius = (math.min(size.width, size.height) / 2) - (strokeWidth / 2);

    final Paint bg = Paint()
      ..color = backgroundColor
      ..style = PaintingStyle.stroke
      ..strokeWidth = strokeWidth;

    final Paint fg = Paint()
      ..color = foregroundColor.withOpacity(0.85)
      ..style = PaintingStyle.stroke
      ..strokeWidth = strokeWidth
      ..strokeCap = StrokeCap.round;

    // Background ring
    canvas.drawCircle(center, radius, bg);

    // Foreground remaining arc (shaded) decreasing over time.
    final Rect rect = Rect.fromCircle(center: center, radius: radius);
    final double startAngle = -math.pi / 2;
    final double sweep = (2 * math.pi) * progress;
    canvas.drawArc(rect, startAngle, sweep, false, fg);
  }

  @override
  bool shouldRepaint(covariant _TimerRingPainter oldDelegate) {
    return oldDelegate.progress != progress ||
        oldDelegate.backgroundColor != backgroundColor ||
        oldDelegate.foregroundColor != foregroundColor ||
        oldDelegate.strokeWidth != strokeWidth;
  }
}
