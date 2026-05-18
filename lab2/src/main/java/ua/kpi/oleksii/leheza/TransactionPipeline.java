package ua.kpi.oleksii.leheza;

import java.util.*;
import java.util.concurrent.*;

public class TransactionPipeline {
    
    static class Transaction {
        int userId;
        double amount;
        String currency;
        String type;
        
        Transaction(int userId, double amount, String currency, String type) {
            this.userId = userId;
            this.amount = amount;
            this.currency = currency;
            this.type = type;
        }
    }
    
    static List<Transaction> generateTransactions(int count) {
        Random rand = new Random();
        String[] currencies = {"USD", "EUR", "UAH"};
        String[] types = {"товар", "послуга", "підписка"};
        List<Transaction> transactions = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            transactions.add(new Transaction(
                rand.nextInt(1000),
                rand.nextDouble() * 1000,
                currencies[rand.nextInt(currencies.length)],
                types[rand.nextInt(types.length)]
            ));
        }
        return transactions;
    }
    
    static double convertToUAH(double amount, String currency) {
        return switch (currency) {
            case "USD" -> amount * 41.0;
            case "EUR" -> amount * 44.0;
            default -> amount;
        };
    }
    
    static double applyCashback(int userId, double amount) {
        return userId > 500 ? amount * 0.8 : amount;
    }
    
    static double sequential(List<Transaction> transactions) {
        double total = 0;
        for (Transaction t : transactions) {
            double converted = convertToUAH(t.amount, t.currency);
            double final_amount = applyCashback(t.userId, converted);
            total += final_amount;
        }
        return total;
    }
    
    static double pipeline(List<Transaction> transactions, int threads) throws Exception {
        BlockingQueue<Transaction> stage1Queue = new LinkedBlockingQueue<>();
        BlockingQueue<Double> stage2Queue = new LinkedBlockingQueue<>();
        BlockingQueue<Double> stage3Queue = new LinkedBlockingQueue<>();
        
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        
        // Етап 1: Конвертація валюти
        pool.submit(() -> {
            for (Transaction t : transactions) {
                double converted = convertToUAH(t.amount, t.currency);
                stage2Queue.offer(converted);
            }
            stage2Queue.offer(-1.0); // Маркер кінця
        });
        
        // Етап 2: Застосування кешбеку
        pool.submit(() -> {
            int idx = 0;
            while (true) {
                try {
                    Double amount = stage2Queue.poll(1, TimeUnit.SECONDS);
                    if (amount == null) continue;
                    if (amount == -1.0) {
                        stage3Queue.offer(-1.0);
                        break;
                    }
                    double final_amount = applyCashback(transactions.get(idx++).userId, amount);
                    stage3Queue.offer(final_amount);
                } catch (InterruptedException e) { break; }
            }
        });
        
        // Етап 3: Агрегація
        double[] total = {0};
        Future<?> aggregator = pool.submit(() -> {
            while (true) {
                try {
                    Double amount = stage3Queue.poll(1, TimeUnit.SECONDS);
                    if (amount == null) continue;
                    if (amount == -1.0) break;
                    total[0] += amount;
                } catch (InterruptedException e) { break; }
            }
        });
        
        aggregator.get();
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        
        return total[0];
    }
    
    public static void main(String[] args) {
        run();
    }
    
    public static void run() {
        System.out.println("\n=== Транзакції Pipeline ===");
        int count = 100_000;
        List<Transaction> transactions = generateTransactions(count);
        System.out.println("Транзакцій: " + count);
        
        long start = System.nanoTime();
        double seqResult = sequential(transactions);
        long seqTime = System.nanoTime() - start;
        System.out.printf("Послідовно: %d мс%n", seqTime / 1_000_000);
        System.out.printf("Сума: %.2f грн%n", seqResult);
        
        int[] threadCounts = {2, 3, 4, 8};
        for (int t : threadCounts) {
            try {
                start = System.nanoTime();
                double result = pipeline(transactions, t);
                long time = System.nanoTime() - start;
                System.out.printf("Pipeline (%d потоків): %d мс, прискорення: %.2fx%n", 
                    t, time / 1_000_000, (double)seqTime / time);
            } catch (Exception e) {
                System.out.println("Помилка: " + e.getMessage());
            }
        }
    }
}
