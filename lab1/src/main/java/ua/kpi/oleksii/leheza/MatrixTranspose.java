package ua.kpi.oleksii.leheza;

import java.util.Random;
import java.util.concurrent.*;

public class MatrixTranspose {

    public static int[][] generate(int size) {
        Random random = new Random();
        int[][] matrix = new int[size][size];

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                matrix[i][j] = random.nextInt(100);
            }
        }

        return matrix;
    }

    public static int[][] transposeSequential(int[][] matrix) {
        int n = matrix.length;
        int[][] result = new int[n][n];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                result[j][i] = matrix[i][j];
            }
        }

        return result;
    }

    public static int[][] transposeParallel(int[][] matrix, int threads) {
        int n = matrix.length;
        int[][] result = new int[n][n];

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        int rowsPerThread = n / threads;

        Future<?>[] futures = new Future[threads];

        for (int t = 0; t < threads; t++) {
            int startRow = t * rowsPerThread;
            int endRow = (t == threads - 1) ? n : startRow + rowsPerThread;

            futures[t] = executor.submit(() -> {
                for (int i = startRow; i < endRow; i++) {
                    for (int j = 0; j < n; j++) {
                        result[j][i] = matrix[i][j];
                    }
                }
            });
        }

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        executor.shutdown();
        return result;
    }
}
