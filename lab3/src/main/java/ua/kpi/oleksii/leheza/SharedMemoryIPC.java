package ua.kpi.oleksii.leheza;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class SharedMemoryIPC {
    private static final String SHARED_FILE = "/tmp/shared_memory.dat";
    private static final int BUFFER_SIZE = 1024;
    private static final int NUM_OPERATIONS = 1000;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java SharedMemoryIPC [producer|consumer]");
            return;
        }

        if (args[0].equals("producer")) {
            runProducer();
        } else {
            runConsumer();
        }
    }

    private static void runProducer() throws Exception {
        System.out.println("=== PRODUCER (Shared Memory) ===");

        RandomAccessFile file = new RandomAccessFile(SHARED_FILE, "rw");
        MappedByteBuffer buffer = file.getChannel().map(
                FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);

        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            int number = random.nextInt(10000);

            long start = System.nanoTime();

            buffer.position(0);
            buffer.putInt(number);
            buffer.putInt(1);
            buffer.force();

            while (buffer.getInt(4) == 1) {
                Thread.sleep(1);
            }

            buffer.position(8);
            int result = buffer.getInt();

            long end = System.nanoTime();
            totalTime += (end - start);

            if (i % 100 == 0) {
                System.out.println("Sent: " + number + ", Received: " + result);
            }
        }

        System.out.println("\nAverage time per operation: " + (totalTime / NUM_OPERATIONS / 1000) + " μs");

        file.close();
    }

    private static void runConsumer() throws Exception {
        System.out.println("=== CONSUMER (Shared Memory) ===");

        RandomAccessFile file = new RandomAccessFile(SHARED_FILE, "rw");
        MappedByteBuffer buffer = file.getChannel().map(
                FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            while (buffer.getInt(4) != 1) {
                Thread.sleep(1);
            }

            buffer.position(0);
            int number = buffer.getInt();

            if (i % 100 == 0) {
                System.out.println("Received: " + number);
            }

            buffer.position(8);
            buffer.putInt(number * 2);

            buffer.position(4);
            buffer.putInt(0);
            buffer.force();
        }

        file.close();
    }
}
