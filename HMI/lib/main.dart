import 'package:flutter/material.dart';

import 'game_screen.dart';

void main() {
  runApp(const HmiApp());
}

class HmiApp extends StatelessWidget {
  const HmiApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'BertsonalTrainer HMI',
      theme: ThemeData(
        useMaterial3: true,
        colorSchemeSeed: const Color(0xFF1B5E20),
      ),
      home: const GameScreen(),
    );
  }
}
