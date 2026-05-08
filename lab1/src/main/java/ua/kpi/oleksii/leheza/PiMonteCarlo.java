package ua.kpi.oleksii.leheza;

import java.util.concurrent.*;
import java.util.Random;

public class PiMonteCarlo {

    public static double calculateSequential(long iterations) {
        Random random = new Random();
        long insideCircle = 0;

        for (long i = 0; i < iterations; i++) {
            double x = random.nextDouble();
            double y = random.nextDouble();
            if (x * x + y * y <= 1.0) {
                insideCircle++;
            }
        }

        return 4.0 * insideCircle / iterations;
    }

    public static double calculateParallel(long iterations, int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        long iterationsPerThread = iterations / threads;

        Future<Long>[] futures = new Future[threads];

        for (int i = 0; i < threads; i++) {
            futures[i] = executor.submit(() -> {
                Random random = new Random();
                long inside = 0;
                for (long j = 0; j < iterationsPerThread; j++) {
                    double x = random.nextDouble();
                    double y = random.nextDouble();
                    if (x * x + y * y <= 1.0) {
                        inside++;
                    }
                }
                return inside;
            });
        }

        long totalInside = 0;
        try {
            for (Future<Long> future : futures) {
                totalInside += future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        executor.shutdown();
        return 4.0 * totalInside / iterations;
    }
}
