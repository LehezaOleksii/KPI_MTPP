package ua.kpi.oleksii.leheza;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionProducerConsumer {
    
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
    
    static double processTransaction(Transaction t) {
        double converted = convertToUAH(t.amount, t.currency);
        return applyCashback(t.userId, converted);
    }
    
    static double sequential(List<Transaction> transactions) {
        double total = 0;
        for (Transaction t : transactions) {
            total += processTransaction(t);
        }
        return total;
    }
    
    static double producerConsumer(List<Transaction> transactions, int producers, int consumers) 
            throws Exception {
        BlockingQueue<Transaction> queue = new LinkedBlockingQueue<>(1000);
        AtomicInteger producedCount = new AtomicInteger(0);
        double[] results = new double[consumers];
        CountDownLatch latch = new CountDownLatch(consumers);
        
        ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);
        
        // Producers - додають транзакції в чергу
        int chunkSize = transactions.size() / producers;
        for (int p = 0; p < producers; p++) {
            int start = p * chunkSize;
            int end = (p == producers - 1) ? transactions.size() : (p + 1) * chunkSize;
            
            pool.submit(() -> {
                for (int i = start; i < end; i++) {
                    try {
                        queue.put(transactions.get(i));
                        producedCount.incrementAndGet();
                    } catch (InterruptedException e) { break; }
                }
            });
        }
        
        // Consumers - обробляють транзакції
        for (int c = 0; c < consumers; c++) {
            int consumerId = c;
            pool.submit(() -> {
                double localSum = 0;
                while (producedCount.get() < transactions.size() || !queue.isEmpty()) {
                    try {
                        Transaction t = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (t != null) {
                            localSum += processTransaction(t);
                        }
                    } catch (InterruptedException e) { break; }
                }
                results[consumerId] = localSum;
                latch.countDown();
            });
        }
        
        latch.await(30, TimeUnit.SECONDS);
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        
        double total = 0;
        for (double r : results) total += r;
        return total;
    }
    
    public static void main(String[] args) {
        run();
    }
    
    public static void run() {
        System.out.println("\n=== Транзакції Producer-Consumer ===");
        int count = 100_000;
        List<Transaction> transactions = generateTransactions(count);
        System.out.println("Транзакцій: " + count);
        
        long start = System.nanoTime();
        double seqResult = sequential(transactions);
        long seqTime = System.nanoTime() - start;
        System.out.printf("Послідовно: %d мс%n", seqTime / 1_000_000);
        System.out.printf("Сума: %.2f грн%n", seqResult);
        
        int[][] configs = {{1, 2}, {2, 2}, {2, 4}, {4, 4}, {2, 8}};
        for (int[] config : configs) {
            int producers = config[0];
            int consumers = config[1];
            try {
                start = System.nanoTime();
                double result = producerConsumer(transactions, producers, consumers);
                long time = System.nanoTime() - start;
                System.out.printf("P-C (%d prod, %d cons): %d мс, прискорення: %.2fx%n", 
                    producers, consumers, time / 1_000_000, (double)seqTime / time);
            } catch (Exception e) {
                System.out.println("Помилка: " + e.getMessage());
            }
        }
    }
}
