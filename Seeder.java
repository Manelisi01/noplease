import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.Arrays;
import java.util.concurrent.*;

public class Seeder {

    // Constants
    private static final int CHUNK_SIZE = 512 * 1024; // 512 KB chunk size
    private static final int TRACKER_PORT = 5000; // Port for the tracker

    public static void main(String[] args) throws Exception {

        // Validate command-line arguments
        if (args.length < 4) {
            System.err.println("Usage: java Seeder <filePath> <TRACKER_IP> <SEEDER_IP> <SEEDER_PORT>");
            return;
        }

        // Parse command-line arguments
        String filePath = args[0]; // Path to the file to be shared
        String TRACKER_IP = args[1]; // IP address of the tracker
        String SEEDER_IP = args[2]; // IP address of this seeder
        String SEEDER_PORT = args[3]; // Port on which this seeder will listen

        int seederPort = Integer.parseInt(SEEDER_PORT); // Convert port to integer

        // Split the file into chunks
        byte[][] chunks = splitFile(filePath);
        System.out.println("File split into " + chunks.length + " chunks.");

        // Start TCP server to handle client requests
        ExecutorService executor = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(seederPort)) {
            System.out.println("Seeder started on port " + seederPort);

            // Register with the tracker
            register(filePath, TRACKER_IP, seederPort, SEEDER_IP);
            System.out.println("Registered with tracker for file: " + filePath);

            // Schedule periodic heartbeats to the tracker
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                update(filePath, TRACKER_IP, seederPort, SEEDER_IP);
                System.out.println("Sent heartbeat to tracker.");
            }, 0, 30, TimeUnit.SECONDS);

            // Handle incoming TCP connections
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());

                // Submit a new task to handle the client
                executor.submit(() -> handleClient(clientSocket, chunks));
            }
        }
    }

    /**
     * Registers the seeder with the tracker.
     *
     * @param fileHash   The hash of the file being shared.
     * @param TRACKER_IP The IP address of the tracker.
     * @param port       The port on which the seeder is listening.
     * @param SEEDER_IP  The IP address of the seeder.
     */
    private static void register(String fileHash, String TRACKER_IP, int port, String SEEDER_IP) {
        try (DatagramSocket socket = new DatagramSocket()) {
            String message = "REGISTER|" + fileHash + "|" + SEEDER_IP + "|" + port;
            byte[] data = message.getBytes();
            InetAddress address = InetAddress.getByName(TRACKER_IP);
            socket.send(new DatagramPacket(data, data.length, address, TRACKER_PORT));
        } catch (Exception e) {
            System.err.println("Failed to register with tracker: " + e.getMessage());
        }
    }

    /**
     * Sends a periodic update (heartbeat) to the tracker.
     *
     * @param fileHash   The hash of the file being shared.
     * @param TRACKER_IP The IP address of the tracker.
     * @param port       The port on which the seeder is listening.
     * @param SEEDER_IP  The IP address of the seeder.
     */
    private static void update(String fileHash, String TRACKER_IP, int port, String SEEDER_IP) {
        try (DatagramSocket socket = new DatagramSocket()) {
            String message = "UPDATE|" + fileHash + "|" + SEEDER_IP + "|" + port;
            byte[] data = message.getBytes();
            InetAddress address = InetAddress.getByName(TRACKER_IP);
            socket.send(new DatagramPacket(data, data.length, address, TRACKER_PORT));
        } catch (Exception e) {
            System.err.println("Failed to send heartbeat to tracker: " + e.getMessage());
        }
    }

    /**
     * Splits the file into chunks of size CHUNK_SIZE.
     *
     * @param filePath The path to the file.
     * @return A 2D byte array containing the file chunks.
     * @throws IOException If an I/O error occurs.
     */
    private static byte[][] splitFile(String filePath) throws IOException {
        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        int numChunks = (int) Math.ceil(fileBytes.length / (double) CHUNK_SIZE);
        byte[][] chunks = new byte[numChunks][];

        for (int i = 0; i < numChunks; i++) {
            int start = i * CHUNK_SIZE;
            int end = Math.min(start + CHUNK_SIZE, fileBytes.length);
            chunks[i] = Arrays.copyOfRange(fileBytes, start, end);
        }
        return chunks;
    }

    /**
     * Handles a client connection by serving the requested chunk.
     *
     * @param socket The client socket.
     * @param chunks The array of file chunks.
     */
    private static void handleClient(Socket socket, byte[][] chunks) {
        try (DataInputStream in = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            int chunkNumber = in.readInt(); // Read the requested chunk number

            if (chunkNumber == -1) {
                // Respond with the total number of chunks
                out.writeInt(chunks.length);
                System.out.println("Sent total chunks to client: " + chunks.length);
            } else {
                // Send the requested chunk
                out.write(chunks[chunkNumber]);
                System.out.println("Sent chunk " + chunkNumber + " to client.");
            }
        } catch (IOException e) {
            System.err.println("Error handling client request: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}