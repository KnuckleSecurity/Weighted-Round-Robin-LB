package taskerPackage;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class tasker {

    void listener(Integer jobCount, String port){
        byte[] buffer = new byte[8192];
        String prt = port;
        Integer jc = jobCount;

        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        try {
            DatagramSocket socketListen = new DatagramSocket(Integer.parseInt(prt));
            while (true){
                socketListen.receive(packet);
                String message = new String(buffer).trim();
                System.out.println(message);
                jc--;
                if (jc.equals(0)){
                    System.out.println("All the jobs sent, succesfully received from the load-balancer, exiting...");
                    break;
                }
            }
        } catch (SocketException e) {
        } catch (IOException e) {
        }

    }
    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("");
        System.out.println("-------------------------------------------------------------");
        System.out.println(" /$$$$$$$$ /$$$$$$   /$$$$$$  /$$   /$$ /$$$$$$$$ /$$$$$$$ ");
        System.out.println("|__  $$__//$$__  $$ /$$__  $$| $$  /$$/| $$_____/| $$__  $$");
        System.out.println("   | $$  | $$  \\ $$| $$  \\__/| $$ /$$/ | $$      | $$  \\ $$");
        System.out.println("   | $$  | $$$$$$$$|  $$$$$$ | $$$$$/  | $$$$$   | $$$$$$$/");
        System.out.println("   | $$  | $$__  $$ \\____  $$| $$  $$  | $$__/   | $$__  $$");
        System.out.println("   | $$  | $$  | $$ /$$  \\ $$| $$\\  $$ | $$      | $$  \\ $$");
        System.out.println("   | $$  | $$  | $$|  $$$$$$/| $$ \\  $$| $$$$$$$$| $$  | $$");
        System.out.println("   |__/  |__/  |__/ \\______/ |__/  \\__/|________/|__/  |__/");
        System.out.println("Burak Baris-T0161214------------------------------------------");
        System.out.println("");
        System.out.println("Welcome to the Tasker.");
        String jobCount;
        String jobTime;
        String ipAddress;
        String port;
        String lbAddress;
        Integer lbPort;
        Scanner myScanner = new Scanner(System.in);
        System.out.println("How many jobs do you want to sent?: ");
        jobCount = myScanner.nextLine();
        System.out.println("How many seconds will those jobs take (for each)?: ");
        jobTime = myScanner.nextLine();
        System.out.println("Interface IP of this machine?: ");
        ipAddress = myScanner.nextLine();
        System.out.println("Communication Port?: ");
        port = myScanner.nextLine();
        System.out.println("IP address of the load balancer?: ");
        lbAddress = myScanner.nextLine();
        System.out.println("Port number of the load balancer?: ");
        lbPort = Integer.parseInt(myScanner.nextLine());

        try (DatagramSocket socket = new DatagramSocket()) {
            InetAddress addr = InetAddress.getByName(lbAddress);
            String message2 = "JOB,"+Integer.parseInt(jobTime)+","+ipAddress+","+port;
            DatagramPacket packet2 = new DatagramPacket(message2.getBytes(StandardCharsets.UTF_8),
                    message2.length(),addr,lbPort);
            for (int i=0;i<Integer.parseInt(jobCount);i++){
                socket.send(packet2);
            }
        }

        tasker tasker = new tasker();
        System.out.println("Jobs have been sent to the load balancer for their execution. Waiting for results...");
        Runnable runnable = () -> {tasker.listener(Integer.valueOf(jobCount),port);};
        Thread listenerThread = new Thread(runnable);
        listenerThread.start();
    }
}