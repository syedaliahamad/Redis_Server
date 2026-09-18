import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisServer {

    // ConcurrentHashMap is safe to use from multiple threads at once
    private static final Map<String, String> storage = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        int port = 6379;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Server listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected!");

            Thread clientThread = new Thread(() -> handleClient(clientSocket));
            clientThread.start();
        }
    }

    private static void handleClient(Socket clientSocket) {
        try {
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream())
            );
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Received: " + line);
                String response = handleCommand(line);
                writer.println(response);
            }

            System.out.println("Client disconnected.");
            clientSocket.close();
        } catch (Exception e) {
            System.out.println("Error handling client: " + e.getMessage());
        }
    }

    private static String handleCommand(String line) {
        String[] parts = line.trim().split("\\s+");

        if (parts.length == 0 || parts[0].isEmpty()) {
            return "ERROR: empty command";
        }

        String command = parts[0].toUpperCase();

        switch (command) {
            case "SET": {
                if (parts.length < 3) return "ERROR: usage SET key value";
                storage.put(parts[1], parts[2]);
                return "OK";
            }

            case "GET": {
                if (parts.length < 2) return "ERROR: usage GET key";
                String result = storage.get(parts[1]);
                return (result == null) ? "(nil)" : result;
            }

            case "DEL": {
                if (parts.length < 2) return "ERROR: usage DEL key";
                String removed = storage.remove(parts[1]);
                return (removed == null) ? "0" : "1";
            }

            case "EXISTS": {
                if (parts.length < 2) return "ERROR: usage EXISTS key";
                return storage.containsKey(parts[1]) ? "1" : "0";
            }

            default:
                return "ERROR: unknown command '" + command + "'";
        }
    }
}