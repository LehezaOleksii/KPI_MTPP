package ua.kpi.oleksii.leheza;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class FileWordCounter {

    public static String generateTestFiles(int fileCount) {
        try {
            Path testDir = Files.createTempDirectory("test_files");
            Random random = new Random();

            for (int i = 0; i < fileCount; i++) {
                Path file = testDir.resolve("file_" + i + ".txt");
                int wordCount = 100 + random.nextInt(200);

                StringBuilder content = new StringBuilder();
                for (int j = 0; j < wordCount; j++) {
                    content.append("word").append(j).append(" ");
                }

                Files.writeString(file, content.toString());
            }

            return testDir.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static long countSequential(String directory) {
        long totalWords = 0;

        try {
            List<Path> files = Files.walk(Paths.get(directory))
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".txt"))
                    .toList();

            for (Path file : files) {
                totalWords += countWordsInFile(file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return totalWords;
    }

    public static long countParallel(String directory, int threads) {
        AtomicLong totalWords = new AtomicLong(0);

        try {
            List<Path> files = Files.walk(Paths.get(directory))
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".txt"))
                    .toList();

            ExecutorService executor = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();

            for (Path file : files) {
                futures.add(executor.submit(() -> {
                    long count = countWordsInFile(file);
                    totalWords.addAndGet(count);
                }));
            }

            for (Future<?> future : futures) {
                future.get();
            }

            executor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return totalWords.get();
    }

    private static long countWordsInFile(Path file) {
        try {
            String content = Files.readString(file);
            return content.split("\\s+").length;
        } catch (IOException e) {
            return 0;
        }
    }
}
