package nodePackage;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class nodeManager {

    HashMap<Integer, node> nodes = new HashMap<>();
    String loadBalancerAddress;
    Integer loadBalancerPort;

    public void userPrompt(){
        System.out.println("Type 'help' to see available commands.");
        Runnable runnable = () -> {
            String input="";
            while (!input.equals("exit")){
                Scanner scanner = new Scanner(System.in);
                System.out.println("NM > ");
                input = scanner.nextLine();

                switch (input){
                    case "help":
                        System.out.println("crtnd: Create a new node.\nsnd: Show nodes' details. "
                                + "\ndreg: De-Register a node.\nexit: Exit the manager.");
                        break;
                    case "crtnd":
                        try{
                            Integer nodePort;
                            Integer maxJobs;
                            String nodeName;
                            String addr;
                            System.out.println("Node Port (E.g -> 3434): ");
                            nodePort = Integer.parseInt(scanner.nextLine());
                            System.out.println("Max Jobs (E.g -> 25): ");
                            maxJobs = Integer.parseInt(scanner.nextLine());
                            System.out.println("Load Balancer Address (E.g -> 127.0.0.1): ");
                            loadBalancerAddress = scanner.nextLine();
                            System.out.println("Load Balancer Port (E.g -> 5000): ");
                            loadBalancerPort = Integer.parseInt(scanner.nextLine());
                            System.out.println("Node name (E.g -> Node1): ");
                            nodeName = scanner.nextLine();
                            System.out.println("Interface IP of this machine: ");
                            addr = scanner.nextLine();

                            Object[] nodeDetails = new Object[6];
                            nodeDetails[0]=nodePort;
                            nodeDetails[1]=maxJobs;
                            nodeDetails[2]=loadBalancerAddress;
                            nodeDetails[3]=loadBalancerPort;
                            nodeDetails[4]=nodeName;
                            nodeDetails[5]=addr;
                            try {
                                createNode(nodeDetails);
                            } catch (InterruptedException e) {
                            }
                            break;
                        }catch(Exception e){
                            System.out.println("Try again.");
                            continue;
                        }
                    case "snd":
                        nodeInfo();
                        break;
                    case "dreg":
                        try {
                            deReg();
                        } catch (IOException e) {
                        }
                        break;
                }
            }
            for (node node : nodes.values()){
                node.killNode();
            }
        };
        Thread prompt = new Thread(runnable);
        prompt.start();
    }

    public void nodeInfo(){

        if (nodes.size() < 1){
            System.out.println("There is no registered node.");
            return;
        }
        for (Map.Entry<Integer, node> worker : nodes.entrySet()){
            boolean alive = false;
            try {
                alive = nodes.get(worker.getKey()).getReg();
            } finally {
                if (!alive){
                    continue;
                }
                System.out.print("["+(worker.getKey()+1)+"] -> ");
                System.out.print("Name: "+nodes.get(worker.getKey()).getNodeName());
                System.out.print(", IP: "+nodes.get(worker.getKey()).getNodeIPAddress());
                System.out.print(", Port: "+nodes.get(worker.getKey()).getNodePort());
                System.out.print(", Max Jobs: "+nodes.get(worker.getKey()).getMaxJobs());
                System.out.print(", Runnig Jobs: "+nodes.get(worker.getKey()).getCurrentJobs());
                Double loadRate = 100*(new Double(nodes.get(worker.getKey()).getCurrentJobs())/
                        new Double(nodes.get(worker.getKey()).getMaxJobs()));
                Integer loadRateInt = Math.toIntExact(Math.round(loadRate));
                System.out.print(", Load Rate: %"+ loadRateInt);
                System.out.println(", Load Balancer Address: "+loadBalancerAddress+":"+loadBalancerPort);
            }
        }
    }
    public void createNode(Object[] nodeDetails) throws InterruptedException, IOException {
        for (Map.Entry<Integer, node> worker : nodes.entrySet()) {
            if (nodes.get(worker.getKey()).getNodePort().equals(nodeDetails[0])) {
                System.out.println("This port can not be used, try another.");
                return;
            }
        }
        nodes.put(Integer.parseInt(String.valueOf(nodes.size())), new node());
        node newNode = nodes.get(Integer.parseInt(String.valueOf(nodes.size()))-1);
        nodes.get(Integer.parseInt(String.valueOf(nodes.size()))-1).setNode((String) nodeDetails[5],
                (Integer) nodeDetails[0], (Integer) nodeDetails[1],(String) nodeDetails[2],(Integer) nodeDetails[3]);
        nodes.get(Integer.parseInt(String.valueOf(nodes.size()))-1).setNodeName((String) nodeDetails[4]);
        nodes.get(Integer.parseInt(String.valueOf(nodes.size()))-1).start();
        String nodeName = nodes.get(Integer.parseInt(String.valueOf(nodes.size()))-1).getNodeName();
        System.out.println(nodeName+": Waiting for 2 seconds for the reg-success message.");
        TimeUnit.SECONDS.sleep(2);
        if (!newNode.getReg()){
            newNode.killNode();
            nodes.remove(nodes.size()-1);
            System.out.println(nodeName+": Could not receive any reg-success message");
        }
    }

    public void deReg() throws IOException {
        if (nodes.size() < 1){
            System.out.println("There is no registered node already.");
            return;
        }
        System.out.println("Select the worker whose registration is desired to be deleted (E.g [3]).");
        nodeInfo();
        Scanner myScanner = new Scanner(System.in);
        System.out.print("> ");
        Integer input = Integer.parseInt(myScanner.nextLine());
        input--;
        try{
            nodes.get(input).sendMessage(nodes.get(input).getNodeIPAddress(),nodes.get(input).getNodePort(),"abort");
            nodes.remove(input);
        }catch(IOException e){
            System.out.println("Try again.");
        }

    }

    public static void main(String[] args) throws UnknownHostException {
        System.out.println("");
        System.out.println("------------------------------------------");
        System.out.println(" /$$   /$$  /$$$$$$  /$$$$$$$  /$$$$$$$$");
        System.out.println("| $$$ | $$ /$$__  $$| $$__  $$| $$_____/");
        System.out.println("| $$$$| $$| $$  \\ $$| $$  \\ $$| $$      ");
        System.out.println("| $$ $$ $$| $$  | $$| $$  | $$| $$$$$   ");
        System.out.println("| $$  $$$$| $$  | $$| $$  | $$| $$__/   ");
        System.out.println("| $$\\  $$$| $$  | $$| $$  | $$| $$      ");
        System.out.println("| $$ \\  $$|  $$$$$$/| $$$$$$$/| $$$$$$$$");
        System.out.println("|__/  \\__/ \\______/ |_______/ |________/");
        System.out.println("Burak Baris-T0161214----------------------");
        System.out.println("");
        System.out.println("Welcome to the Node Manager.");

        nodeManager nm = new nodeManager();
        nm.userPrompt();
    }
}