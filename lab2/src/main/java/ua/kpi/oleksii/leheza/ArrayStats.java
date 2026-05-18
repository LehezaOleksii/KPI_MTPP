package ua.kpi.oleksii.leheza;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class ArrayStats {
    
    static class Stats {
        double min, max, median, avg;
        
        Stats(double min, double max, double median, double avg) {
            this.min = min; this.max = max;
            this.median = median; this.avg = avg;
        }
    }
    
    static double[] generateArray(int size) {
        Random rand = new Random();
        double[] arr = new double[size];
        for (int i = 0; i < size; i++) {
            arr[i] = rand.nextDouble() * 1000;
        }
        return arr;
    }
    
    static Stats sequential(double[] arr) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        
        for (double v : arr) {
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }
        
        double[] sorted = arr.clone();
        Arrays.sort(sorted);
        double median = sorted[sorted.length / 2];
        double avg = sum / arr.length;
        
        return new Stats(min, max, median, avg);
    }
    
    static Stats mapReduce(double[] arr, int threads) {
        ForkJoinPool pool = new ForkJoinPool(threads);
        try {
            return pool.submit(() -> {
                double min = Arrays.stream(arr).parallel().min().orElse(0);
                double max = Arrays.stream(arr).parallel().max().orElse(0);
                double sum = Arrays.stream(arr).parallel().sum();
                double[] sorted = arr.clone();
                Arrays.parallelSort(sorted);
                return new Stats(min, max, sorted[sorted.length/2], sum/arr.length);
            }).get();
        } catch (Exception e) {
            return null;
        } finally {
            pool.shutdown();
        }
    }
    
    static Stats forkJoin(double[] arr, int threads) {
        ForkJoinPool pool = new ForkJoinPool(threads);
        try {
            MinTask minTask = new MinTask(arr, 0, arr.length);
            MaxTask maxTask = new MaxTask(arr, 0, arr.length);
            SumTask sumTask = new SumTask(arr, 0, arr.length);
            
            pool.execute(minTask);
            pool.execute(maxTask);
            pool.execute(sumTask);
            
            double min = minTask.join();
            double max = maxTask.join();
            double sum = sumTask.join();
            
            double[] sorted = arr.clone();
            Arrays.parallelSort(sorted);
            
            return new Stats(min, max, sorted[sorted.length/2], sum/arr.length);
        } finally {
            pool.shutdown();
        }
    }
    
    static Stats workerPool(double[] arr, int threads) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        int chunkSize = arr.length / threads;
        
        List<Future<Double>> minFutures = new ArrayList<>();
        List<Future<Double>> maxFutures = new ArrayList<>();
        List<Future<Double>> sumFutures = new ArrayList<>();
        
        for (int i = 0; i < threads; i++) {
            int start = i * chunkSize;
            int end = (i == threads - 1) ? arr.length : (i + 1) * chunkSize;
            
            minFutures.add(pool.submit(() -> {
                double m = Double.MAX_VALUE;
                for (int j = start; j < end; j++)
                    if (arr[j] < m) m = arr[j];
                return m;
            }));
            
            maxFutures.add(pool.submit(() -> {
                double m = Double.MIN_VALUE;
                for (int j = start; j < end; j++)
                    if (arr[j] > m) m = arr[j];
                return m;
            }));
            
            sumFutures.add(pool.submit(() -> {
                double s = 0;
                for (int j = start; j < end; j++) s += arr[j];
                return s;
            }));
        }
        
        double min = minFutures.stream().map(f -> {
            try { return f.get(); } catch (Exception e) { return Double.MAX_VALUE; }
        }).min(Double::compare).orElse(0.0);
        
        double max = maxFutures.stream().map(f -> {
            try { return f.get(); } catch (Exception e) { return Double.MIN_VALUE; }
        }).max(Double::compare).orElse(0.0);
        
        double sum = sumFutures.stream().mapToDouble(f -> {
            try { return f.get(); } catch (Exception e) { return 0; }
        }).sum();
        
        double[] sorted = arr.clone();
        Arrays.parallelSort(sorted);
        
        pool.shutdown();
        return new Stats(min, max, sorted[sorted.length/2], sum/arr.length);
    }
    
    static class MinTask extends RecursiveTask<Double> {
        double[] arr; int start, end;
        static final int THRESHOLD = 10000;
        
        MinTask(double[] arr, int start, int end) {
            this.arr = arr; this.start = start; this.end = end;
        }
        
        protected Double compute() {
            if (end - start <= THRESHOLD) {
                double min = Double.MAX_VALUE;
                for (int i = start; i < end; i++)
                    if (arr[i] < min) min = arr[i];
                return min;
            }
            int mid = (start + end) / 2;
            MinTask left = new MinTask(arr, start, mid);
            MinTask right = new MinTask(arr, mid, end);
            left.fork();
            return Math.min(right.compute(), left.join());
        }
    }
    
    static class MaxTask extends RecursiveTask<Double> {
        double[] arr; int start, end;
        static final int THRESHOLD = 10000;
        
        MaxTask(double[] arr, int start, int end) {
            this.arr = arr; this.start = start; this.end = end;
        }
        
        protected Double compute() {
            if (end - start <= THRESHOLD) {
                double max = Double.MIN_VALUE;
                for (int i = start; i < end; i++)
                    if (arr[i] > max) max = arr[i];
                return max;
            }
            int mid = (start + end) / 2;
            MaxTask left = new MaxTask(arr, start, mid);
            MaxTask right = new MaxTask(arr, mid, end);
            left.fork();
            return Math.max(right.compute(), left.join());
        }
    }
    
    static class SumTask extends RecursiveTask<Double> {
        double[] arr; int start, end;
        static final int THRESHOLD = 10000;
        
        SumTask(double[] arr, int start, int end) {
            this.arr = arr; this.start = start; this.end = end;
        }
        
        protected Double compute() {
            if (end - start <= THRESHOLD) {
                double sum = 0;
                for (int i = start; i < end; i++) sum += arr[i];
                return sum;
            }
            int mid = (start + end) / 2;
            SumTask left = new SumTask(arr, start, mid);
            SumTask right = new SumTask(arr, mid, end);
            left.fork();
            return right.compute() + left.join();
        }
    }
    
    public static void main(String[] args) {
        run();
    }
    
    public static void run() {
        System.out.println("\n=== Статистика масиву ===");
        int size = 5_000_000;
        double[] arr = generateArray(size);
        System.out.println("Розмір: " + size);
        
        long start = System.nanoTime();
        Stats seq = sequential(arr);
        long seqTime = System.nanoTime() - start;
        System.out.printf("Послідовно: %d мс%n", seqTime / 1_000_000);
        
        int[] threadCounts = {2, 4, 8};
        for (int t : threadCounts) {
            start = System.nanoTime();
            mapReduce(arr, t);
            long mrTime = System.nanoTime() - start;
            System.out.printf("Map-Reduce (%d потоків): %d мс, прискорення: %.2fx%n", 
                t, mrTime / 1_000_000, (double)seqTime / mrTime);
            
            start = System.nanoTime();
            forkJoin(arr, t);
            long fjTime = System.nanoTime() - start;
            System.out.printf("Fork-Join (%d потоків): %d мс, прискорення: %.2fx%n", 
                t, fjTime / 1_000_000, (double)seqTime / fjTime);
            
            try {
                start = System.nanoTime();
                workerPool(arr, t);
                long wpTime = System.nanoTime() - start;
                System.out.printf("Worker Pool (%d потоків): %d мс, прискорення: %.2fx%n", 
                    t, wpTime / 1_000_000, (double)seqTime / wpTime);
            } catch (Exception e) {}
        }
        
        System.out.printf("\nМін: %.2f, Макс: %.2f%n", seq.min, seq.max);
        System.out.printf("Медіана: %.2f, Середнє: %.2f%n", seq.median, seq.avg);
    }
}
