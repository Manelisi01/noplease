import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Leecher {

    private static final int TRACKER_PORT = 5000; // Port for communicating with the tracker

    public static void main(String[] args) throws Exception {

        // Validate command-line arguments
        if (args.length < 4) {
            System.err.println("Usage: java Leecher <TARGET_FILE> <TRACKER_IP> <LEECHER_IP> <LEECHER_IP_PORT>");
            return;
        }

        // Parse command-line arguments
        String TARGET_FILE = args[0]; // File to download
        String TRACKER_IP = args[1]; // IP address of the tracker
        String LEECHER_IP = args[2]; // IP address of this leecher
        String LEECHER_IP_PORT = args[3]; // Port on which this leecher will act as a seeder

        System.out.println("Starting download for file: " + TARGET_FILE);

        // Query the tracker for seeders hosting the file
        List<String> seeders = queryTracker(TARGET_FILE, TRACKER_IP);

        if (seeders.isEmpty()) {
            System.err.println("No seeders available for this file!");
            System.exit(0); // Terminate the program
        }

        System.out.println("Seeders found: " + seeders);

        // Get the total number of chunks from the first seeder
        String firstSeeder = seeders.get(0);
        String[] parts = firstSeeder.split(":");
        String seederIP = parts[0];
        int seederPort = Integer.parseInt(parts[1]);

        System.out.println("Fetching total chunks from seeder: " + firstSeeder);
        int totalChunks = getTotalChunks(seederIP, seederPort);

        if (totalChunks <= 0) {
            System.err.println("Failed to retrieve total chunks from seeder.");
            return;
        }

        System.out.println("Total chunks to download: " + totalChunks);

        // Download chunks in parallel using a thread pool
        ExecutorService executor = Executors.newFixedThreadPool(seeders.size());
        byte[][] chunks = new byte[totalChunks][];

        System.out.println("Starting parallel download of chunks...");

        for (int i = 0; i < totalChunks; i++) {
            int chunkNumber = i;

            executor.submit(() -> {
                String seeder = seeders.get(chunkNumber % seeders.size());
                String[] seederParts = seeder.split(":");
                String ip = seederParts[0];
                int port = Integer.parseInt(seederParts[1].trim());

                System.out.println("Downloading chunk " + chunkNumber + " from seeder: " + seeder);
                chunks[chunkNumber] = downloadChunk(ip, port, chunkNumber);

                if (chunks[chunkNumber] == null || chunks[chunkNumber].length == 0) {
                    System.err.println("Failed to download chunk " + chunkNumber + " from seeder: " + seeder);
                } else {
                    System.out.println("Successfully downloaded chunk " + chunkNumber);
                }
            });
        }

        // Shutdown the executor and wait for all threads to finish
        executor.shutdown();
        System.out.println("Waiting for all chunks to be downloaded...");
        executor.awaitTermination(1, TimeUnit.HOURS);

        // Validate all chunks before assembling the file
        for (int i = 0; i < chunks.length; i++) {
            if (chunks[i] == null || chunks[i].length == 0) {
                System.err.println("Chunk " + i + " is null or empty. File may be incomplete or corrupted.");
                return;
            }
        }

        System.out.println("All chunks downloaded successfully. Assembling file...");

        // Assemble the file from the downloaded chunks
        try (FileOutputStream fos = new FileOutputStream(TARGET_FILE)) {
            for (byte[] chunk : chunks) {
                fos.write(chunk);
            }
        }

        System.out.println("File downloaded successfully: " + TARGET_FILE);

        // Become a seeder for the downloaded file
        System.out.println("Starting seeder to share the downloaded file...");
        Seeder.main(new String[] { TARGET_FILE, TRACKER_IP, LEECHER_IP, LEECHER_IP_PORT });
    }

    /**
     * Queries the tracker for seeders hosting the specified file.
     *
     * @param fileHash   The hash of the file to query.
     * @param TRACKER_IP The IP address of the tracker.
     * @return A list of seeders in the format "IP:Port".
     */
    private static List<String> queryTracker(String fileHash, String TRACKER_IP) throws Exception {
        try (DatagramSocket socket = new DatagramSocket()) {
            String message = "QUERY|" + fileHash;
            byte[] data = message.getBytes();
            InetAddress address = InetAddress.getByName(TRACKER_IP);
            socket.send(new DatagramPacket(data, data.length, address, TRACKER_PORT));

            byte[] buffer = new byte[102400000];
            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            socket.receive(response);

            // Extract the response data and trim any extra whitespace or null bytes
            String responseData = new String(response.getData(), 0, response.getLength()).trim();

            // Check if the response is empty or invalid
            if (responseData.isEmpty() || responseData.equals("")) {
                return Collections.emptyList(); // Return an empty list if no seeders are found
            }

            // Split the response into a list of seeders
            return Arrays.asList(responseData.split(","));
        }
    }

    /**
     * Downloads a specific chunk from a seeder.
     *
     * @param ip          The IP address of the seeder.
     * @param port        The port of the seeder.
     * @param chunkNumber The chunk number to download.
     * @return The downloaded chunk as a byte array.
     */
    private static byte[] downloadChunk(String ip, int port, int chunkNumber) {
        try (Socket socket = new Socket(ip, port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream())) {
            out.writeInt(chunkNumber); // Request the specific chunk
            return in.readAllBytes(); // Read the chunk data
        } catch (IOException e) {
            System.err.println("Error downloading chunk " + chunkNumber + " from " + ip + ":" + port);
            e.printStackTrace();
            return new byte[0];
        }
    }

    /**
     * Retrieves the total number of chunks from a seeder.
     *
     * @param seederIP   The IP address of the seeder.
     * @param seederPort The port of the seeder.
     * @return The total number of chunks.
     */
    private static int getTotalChunks(String seederIP, int seederPort) {
        try (Socket socket = new Socket(seederIP, seederPort);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream())) {
            out.writeInt(-1); // Special request for total chunks
            return in.readInt(); // Read the total number of chunks
        } catch (IOException e) {
            System.err.println("Failed to get total chunks from seeder: " + seederIP + ":" + seederPort);
            e.printStackTrace();
            return 0;
        }
    }
}