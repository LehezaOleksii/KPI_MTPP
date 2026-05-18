package ua.kpi.oleksii.leheza;

import java.io.*;
import java.util.Random;

public class PipeIPC {
    private static final int NUM_OPERATIONS = 1000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Pipe-based IPC Demo ===\n");

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos);

        PipedOutputStream posBack = new PipedOutputStream();
        PipedInputStream pisBack = new PipedInputStream(posBack);

        Thread consumer = new Thread(() -> {
            try {
                DataInputStream in = new DataInputStream(pis);
                DataOutputStream out = new DataOutputStream(posBack);

                for (int i = 0; i < NUM_OPERATIONS; i++) {
                    int number = in.readInt();

                    if (i % 100 == 0) {
                        System.out.println("Consumer received: " + number);
                    }

                    // Send back
                    out.writeInt(number * 2);
                    out.flush();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        consumer.start();

        DataOutputStream out = new DataOutputStream(pos);
        DataInputStream in = new DataInputStream(pisBack);
        Random random = new Random();
        long totalTime = 0;

        System.out.println("Starting operations...\n");

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            int number = random.nextInt(10000);

            long start = System.nanoTime();

            out.writeInt(number);
            out.flush();

            int result = in.readInt();

            long end = System.nanoTime();
            totalTime += (end - start);

            if (i % 100 == 0) {
                System.out.println("Producer sent: " + number + ", received: " + result);
            }
        }

        System.out.println("\nAverage time per operation: " + (totalTime / NUM_OPERATIONS / 1000) + " μs");

        consumer.join();
    }
}
