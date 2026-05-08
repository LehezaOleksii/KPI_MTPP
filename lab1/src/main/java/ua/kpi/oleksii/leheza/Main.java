package ua.kpi.oleksii.leheza;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== Меню ===");
            System.out.println("1. CPU-bound (π Монте-Карло)");
            System.out.println("2. CPU-bound (Факторизація)");
            System.out.println("3. CPU-bound (Прості числа)");
            System.out.println("4. Memory-bound (Транспонування)");
            System.out.println("5. I/O-bound (Підрахунок слів)");
            System.out.println("0. Вихід");

            System.out.print("\nОберіть: ");
            int choice = scanner.nextInt();

            if (choice == 0) break;

            switch (choice) {
                case 1:
                    runPiTask();
                    break;
                case 2:
                    runFactorizationTask();
                    break;
                case 3:
                    runPrimesTask();
                    break;
                case 4:
                    runMatrixTask();
                    break;
                case 5:
                    runFileTask();
                    break;
                default:
                    System.out.println("Невірний вибір");
            }
        }

        scanner.close();
    }

    private static void runPiTask() {
        System.out.print("Кількість ітерацій (млн): ");
        long iterations = new Scanner(System.in).nextLong() * 1_000_000;

        System.out.println("\n--- Послідовно ---");
        long start = System.nanoTime();
        double pi = PiMonteCarlo.calculateSequential(iterations);
        long time = System.nanoTime() - start;
        System.out.printf("π: %.6f\n", pi);
        System.out.printf("Час: %.3f с\n", time / 1e9);

        for (int threads : new int[]{2, 4, 8, 16}) {
            System.out.println("\n--- " + threads + " потоків ---");
            start = System.nanoTime();
            pi = PiMonteCarlo.calculateParallel(iterations, threads);
            time = System.nanoTime() - start;
            System.out.printf("π: %.6f\n", pi);
            System.out.printf("Час: %.3f с\n", time / 1e9);
        }
    }

    private static void runFactorizationTask() {
        System.out.print("Число для факторизації: ");
        long number = new Scanner(System.in).nextLong();

        System.out.println("\n--- Послідовно ---");
        long start = System.nanoTime();
        var factors = Factorization.factorizeSequential(number);
        long time = System.nanoTime() - start;
        System.out.println("Множники: " + factors);
        System.out.printf("Час: %.6f с\n", time / 1e9);

        for (int threads : new int[]{2, 4, 8}) {
            System.out.println("\n--- " + threads + " потоків ---");
            start = System.nanoTime();
            factors = Factorization.factorizeParallel(number, threads);
            time = System.nanoTime() - start;
            System.out.println("Множники: " + factors);
            System.out.printf("Час: %.6f с\n", time / 1e9);
        }
    }

    private static void runPrimesTask() {
        System.out.print("Діапазон до: ");
        int limit = new Scanner(System.in).nextInt();

        System.out.println("\n--- Послідовно ---");
        long start = System.nanoTime();
        int count = PrimeNumbers.countSequential(limit);
        long time = System.nanoTime() - start;
        System.out.println("Простих: " + count);
        System.out.printf("Час: %.3f с\n", time / 1e9);

        for (int threads : new int[]{2, 4, 8, 16}) {
            System.out.println("\n--- " + threads + " потоків ---");
            start = System.nanoTime();
            count = PrimeNumbers.countParallel(limit, threads);
            time = System.nanoTime() - start;
            System.out.println("Простих: " + count);
            System.out.printf("Час: %.3f с\n", time / 1e9);
        }
    }

    private static void runMatrixTask() {
        System.out.print("Розмір матриці: ");
        int size = new Scanner(System.in).nextInt();

        System.out.println("Генерація...");
        int[][] matrix = MatrixTranspose.generate(size);

        System.out.println("\n--- Послідовно ---");
        long start = System.nanoTime();
        int[][] result = MatrixTranspose.transposeSequential(matrix);
        long time = System.nanoTime() - start;
        System.out.printf("Час: %.3f с\n", time / 1e9);

        for (int threads : new int[]{2, 4, 8, 16}) {
            System.out.println("\n--- " + threads + " потоків ---");
            start = System.nanoTime();
            result = MatrixTranspose.transposeParallel(matrix, threads);
            time = System.nanoTime() - start;
            System.out.printf("Час: %.3f с\n", time / 1e9);
        }
    }

    private static void runFileTask() {
        System.out.println("Створення тестових файлів...");
        String dir = FileWordCounter.generateTestFiles(500);

        System.out.println("\n--- Послідовно ---");
        long start = System.nanoTime();
        long words = FileWordCounter.countSequential(dir);
        long time = System.nanoTime() - start;
        System.out.println("Слів: " + words);
        System.out.printf("Час: %.3f с\n", time / 1e9);

        for (int threads : new int[]{2, 4, 8, 16, 32}) {
            System.out.println("\n--- " + threads + " потоків ---");
            start = System.nanoTime();
            words = FileWordCounter.countParallel(dir, threads);
            time = System.nanoTime() - start;
            System.out.println("Слів: " + words);
            System.out.printf("Час: %.3f с\n", time / 1e9);
        }
    }
}
