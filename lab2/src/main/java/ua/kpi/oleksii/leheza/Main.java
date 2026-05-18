package ua.kpi.oleksii.leheza;

import java.util.*;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        while (true) {
            System.out.println("\n=== Лабораторна робота 2 МТПП ===");
            System.out.println("1. Підрахунок тегів HTML");
            System.out.println("2. Статистика масиву");
            System.out.println("3. Множення матриць");
            System.out.println("4. Транзакції Pipeline");
            System.out.println("5. Транзакції Producer-Consumer");
            System.out.println("0. Вихід");
            System.out.print("Оберіть: ");
            
            int choice = scanner.nextInt();
            
            if (choice == 0) break;
            
            switch (choice) {
                case 1:
                    HtmlTagCounter.run();
                    break;
                case 2:
                    ArrayStats.run();
                    break;
                case 3:
                    MatrixMultiply.run();
                    break;
                case 4:
                    TransactionPipeline.run();
                    break;
                case 5:
                    TransactionProducerConsumer.run();
                    break;
                default:
                    System.out.println("Невірний вибір");
            }
        }
        
        scanner.close();
    }
}
