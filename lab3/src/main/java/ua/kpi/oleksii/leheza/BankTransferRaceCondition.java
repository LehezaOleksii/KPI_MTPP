package ua.kpi.oleksii.leheza;

import java.util.*;
import java.util.concurrent.*;

public class BankTransferRaceCondition {
    static class BankAccount {
        private double balance;
        private final int id;

        public BankAccount(int id, double initialBalance) {
            this.id = id;
            this.balance = initialBalance;
        }

        public double getBalance() {
            return balance;
        }

        public void deposit(double amount) {
            balance += amount;
        }

        public boolean withdraw(double amount) {
            if (balance >= amount) {
                balance -= amount;
                return true;
            }
            return false;
        }

        public int getId() {
            return id;
        }
    }

    private static final int ACCOUNT_COUNT = 150;
    private static final int THREAD_COUNT = 1500;
    private static final double INITIAL_BALANCE = 1000.0;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Bank Transfer Race Condition Demo ===\n");

        System.out.println("1. WITHOUT SYNCHRONIZATION (Race Condition):");
        runTransfers(false);

        System.out.println("\n" + "=".repeat(50) + "\n");

        System.out.println("2. WITH SYNCHRONIZATION (Fixed):");
        runTransfers(true);
    }

    private static void runTransfers(boolean useSynchronization) throws Exception {
        List<BankAccount> accounts = new ArrayList<>();
        Random random = new Random(42);

        for (int i = 0; i < ACCOUNT_COUNT; i++) {
            accounts.add(new BankAccount(i, INITIAL_BALANCE));
        }

        double totalBefore = accounts.stream().mapToDouble(BankAccount::getBalance).sum();
        System.out.println("Total balance BEFORE: " + totalBefore);

        ExecutorService executor = Executors.newFixedThreadPool(100);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    Random r = new Random();
                    int from = r.nextInt(ACCOUNT_COUNT);
                    int to = r.nextInt(ACCOUNT_COUNT);
                    if (from == to) return;

                    double amount = 10.0 + r.nextDouble() * 90.0;

                    if (useSynchronization) {
                        transferWithSync(accounts.get(from), accounts.get(to), amount);
                    } else {
                        transferWithoutSync(accounts.get(from), accounts.get(to), amount);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();

        double totalAfter = accounts.stream().mapToDouble(BankAccount::getBalance).sum();
        System.out.println("Total balance AFTER:  " + totalAfter);
        System.out.println("Difference: " + Math.abs(totalBefore - totalAfter));
        System.out.println("Time: " + (endTime - startTime) + " ms");

        if (Math.abs(totalBefore - totalAfter) > 0.01) {
            System.out.println("RACE CONDITION DETECTED!");
        } else {
            System.out.println("No race condition");
        }
    }

    private static void transferWithoutSync(BankAccount from, BankAccount to, double amount) {
        if (from.getBalance() >= amount) {
            from.withdraw(amount);
            to.deposit(amount);
        }
    }

    private static void transferWithSync(BankAccount from, BankAccount to, double amount) {
        BankAccount first = from.getId() < to.getId() ? from : to;
        BankAccount second = from.getId() < to.getId() ? to : from;

        synchronized (first) {
            synchronized (second) {
                if (from.getBalance() >= amount) {
                    from.withdraw(amount);
                    to.deposit(amount);
                }
            }
        }
    }
}
