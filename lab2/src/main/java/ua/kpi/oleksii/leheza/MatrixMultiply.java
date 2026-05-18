package ua.kpi.oleksii.leheza;

import java.util.*;
import java.util.concurrent.*;

public class MatrixMultiply {
    
    static double[][] generateMatrix(int rows, int cols) {
        Random rand = new Random();
        double[][] matrix = new double[rows][cols];
        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                matrix[i][j] = rand.nextDouble() * 10;
        return matrix;
    }
    
    static double[][] sequential(double[][] a, double[][] b) {
        int n = a.length;
        int m = b[0].length;
        int p = b.length;
        double[][] c = new double[n][m];
        
        for (int i = 0; i < n; i++)
            for (int j = 0; j < m; j++)
                for (int k = 0; k < p; k++)
                    c[i][j] += a[i][k] * b[k][j];
        
        return c;
    }
    
    static double[][] mapReduce(double[][] a, double[][] b, int threads) {
        int n = a.length;
        int m = b[0].length;
        int p = b.length;
        double[][] c = new double[n][m];
        
        ForkJoinPool pool = new ForkJoinPool(threads);
        try {
            pool.submit(() -> {
                java.util.stream.IntStream.range(0, n).parallel().forEach(i -> {
                    for (int j = 0; j < m; j++)
                        for (int k = 0; k < p; k++)
                            c[i][j] += a[i][k] * b[k][j];
                });
            }).get();
        } catch (Exception e) {}
        finally {
            pool.shutdown();
        }
        
        return c;
    }
    
    static double[][] forkJoin(double[][] a, double[][] b, int threads) {
        ForkJoinPool pool = new ForkJoinPool(threads);
        try {
            MultiplyTask task = new MultiplyTask(a, b, 0, a.length);
            return pool.invoke(task);
        } finally {
            pool.shutdown();
        }
    }
    
    static class MultiplyTask extends RecursiveTask<double[][]> {
        double[][] a, b;
        int startRow, endRow;
        static final int THRESHOLD = 50;
        
        MultiplyTask(double[][] a, double[][] b, int startRow, int endRow) {
            this.a = a; this.b = b;
            this.startRow = startRow; this.endRow = endRow;
        }
        
        protected double[][] compute() {
            int rows = endRow - startRow;
            int m = b[0].length;
            int p = b.length;
            double[][] result = new double[a.length][m];
            
            if (rows <= THRESHOLD) {
                for (int i = startRow; i < endRow; i++)
                    for (int j = 0; j < m; j++)
                        for (int k = 0; k < p; k++)
                            result[i][j] += a[i][k] * b[k][j];
                return result;
            }
            
            int mid = (startRow + endRow) / 2;
            MultiplyTask left = new MultiplyTask(a, b, startRow, mid);
            MultiplyTask right = new MultiplyTask(a, b, mid, endRow);
            
            left.fork();
            double[][] rightResult = right.compute();
            double[][] leftResult = left.join();
            
            for (int i = 0; i < a.length; i++)
                for (int j = 0; j < m; j++)
                    result[i][j] = leftResult[i][j] + rightResult[i][j];
            
            return result;
        }
    }
    
    static double[][] workerPool(double[][] a, double[][] b, int threads) throws Exception {
        int n = a.length;
        int m = b[0].length;
        int p = b.length;
        double[][] c = new double[n][m];
        
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        int chunkSize = n / threads;
        List<Future<?>> futures = new ArrayList<>();
        
        for (int t = 0; t < threads; t++) {
            int startRow = t * chunkSize;
            int endRow = (t == threads - 1) ? n : (t + 1) * chunkSize;
            
            futures.add(pool.submit(() -> {
                for (int i = startRow; i < endRow; i++)
                    for (int j = 0; j < m; j++)
                        for (int k = 0; k < p; k++)
                            c[i][j] += a[i][k] * b[k][j];
            }));
        }
        
        for (Future<?> f : futures) f.get();
        pool.shutdown();
        
        return c;
    }
    
    public static void main(String[] args) {
        run();
    }
    
    public static void run() {
        System.out.println("\n=== Множення матриць ===");
        int size = 500;
        double[][] a = generateMatrix(size, size);
        double[][] b = generateMatrix(size, size);
        System.out.println("Розмір: " + size + "x" + size);
        
        long start = System.nanoTime();
        double[][] seq = sequential(a, b);
        long seqTime = System.nanoTime() - start;
        System.out.printf("Послідовно: %d мс%n", seqTime / 1_000_000);
        
        int[] threadCounts = {2, 4, 8};
        for (int t : threadCounts) {
            start = System.nanoTime();
            mapReduce(a, b, t);
            long mrTime = System.nanoTime() - start;
            System.out.printf("Map-Reduce (%d потоків): %d мс, прискорення: %.2fx%n", 
                t, mrTime / 1_000_000, (double)seqTime / mrTime);
            
            start = System.nanoTime();
            forkJoin(a, b, t);
            long fjTime = System.nanoTime() - start;
            System.out.printf("Fork-Join (%d потоків): %d мс, прискорення: %.2fx%n", 
                t, fjTime / 1_000_000, (double)seqTime / fjTime);
            
            try {
                start = System.nanoTime();
                workerPool(a, b, t);
                long wpTime = System.nanoTime() - start;
                System.out.printf("Worker Pool (%d потоків): %d мс, прискорення: %.2fx%n", 
                    t, wpTime / 1_000_000, (double)seqTime / wpTime);
            } catch (Exception e) {}
        }
        
        System.out.printf("\nПерший елемент: %.2f%n", seq[0][0]);
    }
}
