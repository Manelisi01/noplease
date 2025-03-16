import java.net.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Tracker {
    private static final int PORT = 5000;
    private static final long PEER_TIMEOUT = 10000; // 10 seconds
    private static ConcurrentHashMap<String, PeerInfo> peers = new ConcurrentHashMap<>();
    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws Exception {
        DatagramSocket socket = new DatagramSocket(PORT);
        
        scheduler.scheduleAtFixedRate(
             new Runnable() {
                public void run() {
                    Tracker.cleanupPeers();
                }
            },0, 1,TimeUnit.SECONDS);


        byte[] buffer = new byte[1024];
        
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String message = new String(packet.getData(),0,packet.getLength());
            String[] parts = message.split("\\|");
            String command = parts[0];

            switch (command) {
                case "REGISTER":
                    String fileHash = parts[1];
                    String ip = parts[2];
                    int port = Integer.parseInt(parts[3]);
                    peers.put(fileHash, new PeerInfo(ip, port));
                    break;
                case "QUERY":
                    String hash = parts[1];

                    List<String> matchingPeers = new ArrayList<>();
                    
                    for (Map.Entry<String, PeerInfo> entry : peers.entrySet()) {
                        if (entry.getKey().equals(hash)) {
                            matchingPeers.add(entry.getValue().toString());
                        }
                    }

                    String response = String.join(",", matchingPeers);
                    byte[] respData = response.getBytes();
                    socket.send(new DatagramPacket(respData, respData.length, packet.getAddress(), packet.getPort()));
                    break;
            }
        }
    }

    private static void cleanupPeers() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, PeerInfo>> iterator = peers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PeerInfo> entry = iterator.next();
            if (now - entry.getValue().lastSeen > PEER_TIMEOUT) {
                iterator.remove();
            }
        }
    }

    static class PeerInfo {
        String ip;
        int port;
        long lastSeen;

        PeerInfo(String ip, int port) {
            this.ip = ip;
            this.port = port;
            this.lastSeen = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }
    }
}