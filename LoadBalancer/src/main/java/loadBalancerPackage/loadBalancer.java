package loadBalancerPackage;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class loadBalancer extends Thread {

    private int listeningPort = 0;
    byte[] buffer;
    private DatagramPacket packet = null;
    private DatagramSocket socket = null;
    private HashMap<String, HashMap<String,String>> onlineWorkers = new HashMap<>();
    private HashMap<String, HashMap<String,String>> jobs = new HashMap<>();
    private HashSet<String> pulsingNodes = new HashSet<>();
    private ArrayList<String> allMessages = new ArrayList<>();
    private HashSet<String> removableWorkers = new HashSet<>();
    public boolean running = true;

    public HashMap<String, HashMap<String,String>> getWorkers(){
        return onlineWorkers;
    }
    public HashMap<String, HashMap<String,String>> getJobs(){
        return jobs;
    }

    public void seeMessages(){
        for(String message : allMessages){
            System.out.println(message);
        }
    }

    private void registerNode(Command cmd) throws IOException, InterruptedException {

        System.out.println("Notification: A new worker is registered!");
        String desiredPulseTime = "3";
        String messageConf = "Registration successfull!,"+desiredPulseTime;
        boolean registered = false;
        String testNode = searchNode(cmd.tokenizer()[1], cmd.tokenizer()[2]);
        if (testNode != ""){
            messageConf = "You have already registered !";
            registered = true;
        }
        TimeUnit.SECONDS.sleep(1);
        if ( registered == false){
            HashMap<String,String> workerDetails = new HashMap<String,String>();
            workerDetails.put("IP Address",cmd.tokenizer()[1]);
            workerDetails.put("Port",cmd.tokenizer()[2]);
            workerDetails.put("Max Job Capacity",cmd.tokenizer()[3]);
            workerDetails.put("Currently Running Job Count", "0");
            workerDetails.put("Load Rate %","0");
            Integer workerCount = onlineWorkers.size()+1;
            String workerName = "Worker" + workerCount.toString();
            onlineWorkers.put(workerName, workerDetails);
        }
        sendMessage(cmd.tokenizer()[1],Integer.parseInt(cmd.tokenizer()[2]),messageConf);
        registered = false;
    }

    private void imAlive(){
        Runnable runnable = () -> {
            while (this.running){
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (onlineWorkers.size() == 0){
                    continue;
                }
                for (Map.Entry<String, HashMap<String, String>> workers : onlineWorkers.entrySet()){
                    String workerIP = onlineWorkers.get(workers.getKey()).get("IP Address");
                    Integer workerPort = Integer.parseInt(onlineWorkers.get(workers.getKey()).get("Port"));
                    try {
                        sendMessage(workerIP,workerPort,"alive");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        Thread aliveThread = new Thread(runnable);
        aliveThread.start();
    }

    private void kickNode(String remove, String status){
        String displayMessage = "";
        switch (status){
            case "heartbeat":
                displayMessage="Notification: "+remove+" kicked due to bad connectivity. Unsufficent hearbeats.";
                break;
            case "loadbalancer":
                displayMessage="Notification: "+remove+" kicked successfully.";
                break;
            case "node":
                displayMessage="Notification: "+remove+" has left.";
                break;
        }
        onlineWorkers.remove(remove);
        System.out.println(displayMessage);
        for (Map.Entry<String, HashMap<String, String>> undoneJob : jobs.entrySet()){
            if(undoneJob.getValue().get("Worker ID").equals(remove) &&
                    !undoneJob.getValue().get("Job State").equals("Done")){
                System.out.println("Notification: Job "+undoneJob.getKey()+" is pending, needs to be transferred !");
                undoneJob.getValue().remove("Job State");
                undoneJob.getValue().remove("Worker ID");
                undoneJob.getValue().put("Worker ID", "UNSELECTED");
                undoneJob.getValue().put("Job State", "Pending");
            }
        }
    }

    private void removeNode(Command cmd){
        String removedNodeName = searchNode(cmd.tokenizer()[1], cmd.tokenizer()[2]);
        if (!removedNodeName.startsWith("Worker")){
            return;
        }
        removableWorkers.remove(removedNodeName);
        kickNode(removedNodeName, "node");
    }

    public void deRegNode(String worker) throws IOException {
        sendMessage(onlineWorkers.get(worker).get("IP Address"),
                Integer.parseInt(onlineWorkers.get(worker).get("Port")),"abort");
        removableWorkers.remove(worker);
        kickNode(worker, "loadbalancer");
    }
    private String searchNode(String ip, String port){
        for (HashMap.Entry<String, HashMap<String, String>> workers : onlineWorkers.entrySet()){
            if (onlineWorkers.get(String.valueOf(workers.getKey())).get("IP Address").equals(ip) &&
                    onlineWorkers.get(String.valueOf(workers.getKey())).get("Port").equals(port)){
                return workers.getKey();
            }
        }
        return "";
    }
    private void heartBeatListener(){
        Runnable runnable = () -> {
            Integer timeSeconds = 0;
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (this.running){
                for (Map.Entry<String, HashMap<String, String>> worker : onlineWorkers.entrySet()){
                    removableWorkers.add(worker.getKey());
                }
                for (int i=0;i<5;i++){
                    timeSeconds+=1;
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (timeSeconds.equals(5)){
                        timeSeconds=0;
                        for (String remove : removableWorkers){
                            if (!pulsingNodes.contains(remove)){
                                kickNode(remove, "heartbeat");
                            }
                        }
                        removableWorkers = new HashSet<>();
                        pulsingNodes = new HashSet<>();
                    }
                }
            }
        };
        Thread listener = new Thread(runnable);
        listener.start();
    }

    private String selectNode(){
        Integer minLoad = 100;
        ArrayList<String> eligibleWorkers = new ArrayList<>();

        for (Map.Entry<String, HashMap<String, String>> worker : onlineWorkers.entrySet()){
            String candidate = worker.getKey();
            Integer metric_one = Integer.parseInt(onlineWorkers.get(candidate).get("Load Rate %"));
            if (metric_one < minLoad) minLoad=metric_one;
        }
        if (minLoad > 89){
            return "null";
        }
        for (HashMap.Entry<String, HashMap<String, String>> workers : onlineWorkers.entrySet()){
            if (onlineWorkers.get(String.valueOf(workers.getKey())).get("Load Rate %").equals(minLoad.toString())){
                eligibleWorkers.add(workers.getKey());
            }
        }
        if (eligibleWorkers.size() == 1) {
            return eligibleWorkers.get(0);
        }else if (eligibleWorkers.size() > 1){
            Integer maxCap = 0;
            String selectedNode = null;
            for (String worker : eligibleWorkers){
                Integer metric_two = Integer.parseInt(onlineWorkers.get(worker).get("Max Job Capacity"));
                if (metric_two > maxCap){
                    maxCap=metric_two;
                    selectedNode=worker;
                }
            }
            return selectedNode;
        }
        return null;
    }

    private boolean doNewJob(Command cmd) throws IOException {
        String selectedNode = selectNode();
        String uniqueID = UUID.randomUUID().toString();
        HashMap<String, String> jobDetails = new HashMap<String, String>();
        jobDetails.put("Worker ID",selectedNode);
        jobDetails.put("Client IP",cmd.tokenizer()[2]);
        jobDetails.put("Client Port",cmd.tokenizer()[3]);
        jobDetails.put("Job State","Running");
        jobDetails.put("Job Time",cmd.tokenizer()[1]);
        jobs.put(uniqueID,jobDetails);

        if (selectedNode.equals("null")){
            System.out.println("Notification: A new job has received but no available workers. Job "+uniqueID+" will "
                    + "be added to the pending jobs.");
            jobDetails.remove("Job State");
            jobDetails.put("Job State","Pending");
            return false;
        }

        String messageJob = "JOB"+","+uniqueID+","+cmd.tokenizer()[1];
        sendMessage(onlineWorkers.get(selectedNode).get("IP Address"),
                Integer.parseInt(onlineWorkers.get(selectedNode).get("Port")),messageJob);
        workerUpdate(selectedNode,"Increment");
        String updatedLoad = onlineWorkers.get(selectedNode).get("Load Rate %");
        System.out.println("Notification: "+selectedNode+" (Load Rate %="+updatedLoad+") has tasked with job "+uniqueID+".");
        return true;
    }

    private boolean doPendingJob(String jobID) throws IOException {
        System.out.println("Notification: Trying to transfer a pending job to an available worker...");
        String selectedNode = selectNode();
        if (selectedNode.equals("null")){
            System.out.println("No available workers.");
            return false;
        }
        System.out.println("Notification: The job with the ID of "+jobID+" is now transferring to the "+selectedNode);
        jobs.get(jobID).remove("Worker ID");
        jobs.get(jobID).put("Worker ID", selectedNode);
        String messageJob = "JOB,"+jobID+","+jobs.get(jobID).get("Job Time");
        sendMessage(onlineWorkers.get(selectedNode).get("IP Address"),
                Integer.parseInt(onlineWorkers.get(selectedNode).get("Port")),messageJob);
        workerUpdate(selectedNode, "Increment");
        jobs.get(jobID).remove("Job State");
        jobs.get(jobID).put("Job State", "Running");
        return true;
    }

    private boolean jobDone(Command cmd) throws IOException {
        String doneJobId = cmd.tokenizer()[1];
        System.out.println("Notification: Job with the ID of "+doneJobId+" is done!");
        String doneWorker = jobs.get(doneJobId).get("Worker ID");
        jobs.get(doneJobId).remove("Job State");
        jobs.get(doneJobId).put("Job State", "Done");
        workerUpdate(doneWorker, "Decrement");
        String jobDoneMsg = "Job is completed successfully !";
        sendMessage(jobs.get(doneJobId).get("Client IP"),Integer.parseInt(jobs.get(doneJobId).get("Client Port")),jobDoneMsg);
        return true;
    }

    private void workerUpdate(String worker,String mode){
        Integer CRJB = Integer.parseInt(onlineWorkers.get(worker).get("Currently Running Job Count"));
        Integer MJC = Integer.parseInt(onlineWorkers.get(worker).get("Max Job Capacity"));
        if (mode.equals("Increment")){
            CRJB++;

        }else if (mode.equals("Decrement")){

            CRJB--;
        }
        Double newLoadRate = 100*(new Double(CRJB)/new Double(MJC));
        Integer newLoadRateInt = Math.toIntExact(Math.round(newLoadRate));
        onlineWorkers.get(worker).remove("Load Rate %");
        onlineWorkers.get(worker).put("Load Rate %",newLoadRateInt.toString());
        onlineWorkers.get(worker).remove("Currently Running Job Count");
        onlineWorkers.get(worker).put("Currently Running Job Count",CRJB.toString());
    }

    public void sendMessage(String host, Integer port, String message) throws IOException {
        DatagramSocket socketSend = new DatagramSocket();
        InetAddress addr = InetAddress.getByName(host);
        String messageSend = message;
        DatagramPacket packetSend = new DatagramPacket(messageSend.getBytes(StandardCharsets.UTF_8),
                messageSend.length(),addr, port);
        socketSend.send(packetSend);
        socketSend.close();
    }

    private void pendingJobs(){
        Runnable runnable;
        runnable = () -> {
            while (loadBalancer.this.running) {
                try {
                    TimeUnit.SECONDS.sleep(20);
                } catch (InterruptedException e) {
                }
                for (Map.Entry<String, HashMap<String, String>> pendings : jobs.entrySet()){
                    if (pendings.getValue().get("Job State").equals("Pending")){
                        try {
                            doPendingJob(pendings.getKey());
                        } catch (IOException e) {
                        }
                    }
                }	}
        };
        Thread pendingTasker = new Thread(runnable);
        pendingTasker.start();
    }

    private void receiveData() throws IOException, InterruptedException {
        buffer = new byte[1024];
        packet = new DatagramPacket(buffer, buffer.length);

        socket.receive(packet);
        String message = new String(buffer).trim();

        Command cmd = new Command(message);
        allMessages.add(message);

        switch (cmd.getType()) {
            case "REG" -> registerNode(cmd);
            case "JOB" -> doNewJob(cmd);
            case "DONE" -> jobDone(cmd);
            case "DE-REG" -> removeNode(cmd);
            case "PULSE" -> pulsingNodes.add(searchNode(cmd.tokenizer()[1], cmd.tokenizer()[2]));
            case "Exit" -> this.running = false;
            default -> System.out.println("Unknown command.");
        }
    }
    public loadBalancer(int port){
        super();
        this.buffer = null;
        this.listeningPort=port;
    }

    @Override
    public void run(){
        System.out.println("Load balancer started to operate on port :"+listeningPort);
        System.out.println("See 'help' for auxiliary commands.");
        try{
            socket = new DatagramSocket(listeningPort);
            socket.setSoTimeout(0);
            heartBeatListener();
            pendingJobs();
            imAlive();
            while (this.running){
                receiveData();
            }


        } catch (SocketException socketError){
        }   catch (IOException | InterruptedException ex) {
            Logger.getLogger(loadBalancer.class.getName()).log(Level.SEVERE, null, ex);
        }finally {
            socket.close();
        }
    }
}