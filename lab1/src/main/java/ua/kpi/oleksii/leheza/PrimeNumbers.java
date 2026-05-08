package ua.kpi.oleksii.leheza;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PrimeNumbers {

    public static int countSequential(int limit) {
        int count = 0;
        for (int i = 2; i <= limit; i++) {
            if (isPrime(i)) {
                count++;
            }
        }
        return count;
    }

    public static int countParallel(int limit, int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger count = new AtomicInteger(0);

        int rangeSize = limit / threads;
        Future<?>[] futures = new Future[threads];

        for (int i = 0; i < threads; i++) {
            int start = 2 + i * rangeSize;
            int end = (i == threads - 1) ? limit : start + rangeSize - 1;

            futures[i] = executor.submit(() -> {
                int localCount = 0;
                for (int j = start; j <= end; j++) {
                    if (isPrime(j)) {
                        localCount++;
                    }
                }
                count.addAndGet(localCount);
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
        return count.get();
    }

    private static boolean isPrime(int n) {
        if (n < 2) return false;
        if (n == 2) return true;
        if (n % 2 == 0) return false;

        for (int i = 3; i * i <= n; i += 2) {
            if (n % i == 0) return false;
        }
        return true;
    }
}
