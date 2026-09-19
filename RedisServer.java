import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisServer {

    private static final Map<String, String> storage = new ConcurrentHashMap<>();
    // Stores the exact millisecond timestamp when a key should expire
    private static final Map<String, Long> expirations = new ConcurrentHashMap<>();

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

    // Checks if a key has expired. If so, deletes it and returns true.
    private static boolean isExpired(String key) {
        Long expiryTime = expirations.get(key);
        if (expiryTime == null) {
            return false; // no expiration set for this key
        }
        if (System.currentTimeMillis() >= expiryTime) {
            // Time's up — remove the key entirely
            storage.remove(key);
            expirations.remove(key);
            return true;
        }
        return false;
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
                expirations.remove(parts[1]); // new SET clears any old expiration
                return "OK";
            }

            case "GET": {
                if (parts.length < 2) return "ERROR: usage GET key";
                if (isExpired(parts[1])) return "(nil)";
                String result = storage.get(parts[1]);
                return (result == null) ? "(nil)" : result;
            }

            case "DEL": {
                if (parts.length < 2) return "ERROR: usage DEL key";
                expirations.remove(parts[1]);
                String removed = storage.remove(parts[1]);
                return (removed == null) ? "0" : "1";
            }

            case "EXISTS": {
                if (parts.length < 2) return "ERROR: usage EXISTS key";
                if (isExpired(parts[1])) return "0";
                return storage.containsKey(parts[1]) ? "1" : "0";
            }

            case "EXPIRE": {
                if (parts.length < 3) return "ERROR: usage EXPIRE key seconds";
                String key = parts[1];
                if (!storage.containsKey(key)) return "0"; // key doesn't exist

                try {
                    int seconds = Integer.parseInt(parts[2]);
                    long expiryTime = System.currentTimeMillis() + (seconds * 1000L);
                    expirations.put(key, expiryTime);
                    return "1";
                } catch (NumberFormatException e) {
                    return "ERROR: seconds must be a number";
                }
            }
            default:
                return "ERROR: unknown command '" + command + "'";
        }
    }
}