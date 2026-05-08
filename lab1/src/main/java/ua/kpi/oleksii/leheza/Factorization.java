package ua.kpi.oleksii.leheza;

import java.util.*;
import java.util.concurrent.*;

public class Factorization {

    public static List<Long> factorizeSequential(long number) {
        List<Long> factors = new ArrayList<>();

        for (long i = 2; i * i <= number; i++) {
            while (number % i == 0) {
                factors.add(i);
                number /= i;
            }
        }

        if (number > 1) {
            factors.add(number);
        }

        return factors;
    }

    public static List<Long> factorizeParallel(long number, int threads) {
        List<Long> factors = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        long limit = (long) Math.sqrt(number);
        long rangeSize = limit / threads;

        List<Future<?>> futures = new ArrayList<>();
        long[] currentNumber = {number};

        for (int i = 0; i < threads; i++) {
            long start = 2 + i * rangeSize;
            long end = (i == threads - 1) ? limit : start + rangeSize;

            futures.add(executor.submit(() -> {
                for (long j = start; j <= end; j++) {
                    synchronized (currentNumber) {
                        while (currentNumber[0] % j == 0) {
                            factors.add(j);
                            currentNumber[0] /= j;
                        }
                    }
                }
            }));
        }

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (currentNumber[0] > 1) {
            factors.add(currentNumber[0]);
        }

        executor.shutdown();
        Collections.sort(factors);
        return factors;
    }
}
