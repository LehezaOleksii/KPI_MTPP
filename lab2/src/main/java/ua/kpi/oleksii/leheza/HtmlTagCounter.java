package ua.kpi.oleksii.leheza;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import java.util.regex.*;

public class HtmlTagCounter {
    
    static List<String> generateHtmlDocs(int count) {
        String[] tags = {"div", "p", "span", "a", "img", "h1", "h2", "ul", "li", "table"};
        Random rand = new Random();
        List<String> docs = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            StringBuilder html = new StringBuilder("<html><body>");
            int tagCount = 50 + rand.nextInt(200);
            for (int j = 0; j < tagCount; j++) {
                String tag = tags[rand.nextInt(tags.length)];
                html.append("<").append(tag).append(">вміст</").append(tag).append(">");
            }
            html.append("</body></html>");
            docs.add(html.toString());
        }
        return docs;
    }
    
    static Map<String, Integer> countTags(String html) {
        Map<String, Integer> counts = new HashMap<>();
        Pattern pattern = Pattern.compile("<(\\w+)[^>]*>");
        Matcher matcher = pattern.matcher(html);
        
        while (matcher.find()) {
            String tag = matcher.group(1).toLowerCase();
            counts.merge(tag, 1, Integer::sum);
        }
        return counts;
    }
    
    static Map<String, Integer> sequential(List<String> docs) {
        Map<String, Integer> result = new HashMap<>();
        for (String doc : docs) {
            Map<String, Integer> counts = countTags(doc);
            counts.forEach((tag, count) -> result.merge(tag, count, Integer::sum));
        }
        return result;
    }
    
    static Map<String, Integer> mapReduce(List<String> docs, int threads) {
        return docs.parallelStream()
            .map(HtmlTagCounter::countTags)
            .reduce(new HashMap<>(), (map1, map2) -> {
                map2.forEach((k, v) -> map1.merge(k, v, Integer::sum));
                return map1;
            });
    }
    
    static Map<String, Integer> forkJoin(List<String> docs, int threads) {
        ForkJoinPool pool = new ForkJoinPool(threads);
        try {
            return pool.submit(() -> 
                docs.parallelStream()
                    .map(HtmlTagCounter::countTags)
                    .reduce(new HashMap<>(), (m1, m2) -> {
                        m2.forEach((k, v) -> m1.merge(k, v, Integer::sum));
                        return m1;
                    })
            ).get();
        } catch (Exception e) {
            return new HashMap<>();
        } finally {
            pool.shutdown();
        }
    }
    
    static Map<String, Integer> workerPool(List<String> docs, int threads) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Map<String, Integer> result = new ConcurrentHashMap<>();
        
        List<Future<?>> futures = new ArrayList<>();
        for (String doc : docs) {
            futures.add(pool.submit(() -> {
                Map<String, Integer> counts = countTags(doc);
                counts.forEach((tag, count) -> 
                    result.merge(tag, count, Integer::sum));
            }));
        }
        
        for (Future<?> f : futures) f.get();
        pool.shutdown();
        return result;
    }
    
    public static void main(String[] args) {
        run();
    }
    
    public static void run() {
        System.out.println("\n=== Підрахунок HTML тегів ===");
        List<String> docs = generateHtmlDocs(2000);
        System.out.println("Документів: " + docs.size());
        
        long start = System.nanoTime();
        Map<String, Integer> seq = sequential(docs);
        long seqTime = System.nanoTime() - start;
        System.out.printf("Послідовно: %d мс%n", seqTime / 1_000_000);
        
        int[] threadCounts = {2, 4, 8};
        for (int t : threadCounts) {
            start = System.nanoTime();
            mapReduce(docs, t);
            long mrTime = System.nanoTime() - start;
            System.out.printf("Map-Reduce (%d потоків): %d мс, прискорення: %.2fx%n", 
                t, mrTime / 1_000_000, (double)seqTime / mrTime);
            
            start = System.nanoTime();
            forkJoin(docs, t);
            long fjTime = System.nanoTime() - start;
            System.out.printf("Fork-Join (%d потоків): %d мс, прискорення: %.2fx%n", 
                t, fjTime / 1_000_000, (double)seqTime / fjTime);
            
            try {
                start = System.nanoTime();
                workerPool(docs, t);
                long wpTime = System.nanoTime() - start;
                System.out.printf("Worker Pool (%d потоків): %d мс, прискорення: %.2fx%n", 
                    t, wpTime / 1_000_000, (double)seqTime / wpTime);
            } catch (Exception e) {}
        }
        
        System.out.println("\nТегів знайдено: " + seq.size());
    }
}
