package nodePackage;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class node extends Thread{

    private String lbAddress=null;
    private Integer lbPort=null;
    private String nodeName=null;

    private String nodeIPAddress=null;
    private Integer nodePort=0;
    private Integer maxJobs=0;
    private volatile Integer currentJobs=0;

    private Integer pulseTime = 0;
    private boolean registered = false;
    private boolean running = true;
    private boolean lbHealth = false;

    byte[] buffer = null;
    DatagramPacket packet = null;
    DatagramSocket socket = null;

    private synchronized boolean running(){
	    return running;
    }
    
    private synchronized void currentlyRunningJobs(String mode){
	    if (mode == "Increment") this.currentJobs++;
	    if (mode == "Decrement") this.currentJobs--;
    }
    public void setNode(String nodeIPAddress,Integer nodePort,Integer maxJobs, String lbAddress, Integer lbPort){
        this.nodeIPAddress=nodeIPAddress;
        this.nodePort=nodePort;
        this.maxJobs=maxJobs;
        this.lbAddress=lbAddress;
        this.lbPort=lbPort;
    }
    public void setNodeName(String name){
        this.nodeName=name;
    }
    public String getNodeName(){
        return this.nodeName;
    }
    public String getNodeIPAddress(){
        return this.nodeIPAddress;
    }
    public Integer getNodePort(){
        return this.nodePort;
    }
    public Integer getMaxJobs(){
        return this.maxJobs;
    }
    public Integer getCurrentJobs(){
        return this.currentJobs;
    }

    private void isLbAlive(){
        Runnable runnable = () -> {
            while (running()){
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (lbHealth){
                    lbHealth=false;
                    continue;
                }else{
                    if (!running()) break;
                    System.out.println(this.nodeName+": can not communicate with the load balancer at "
                            +this.lbAddress+":"+this.lbPort+" if there is any. Killing "+ nodeName);
                    killNode();
                    return;
                }
            }
        };
        Thread aliveThread = new Thread(runnable);
        aliveThread.start();
    }
    public void deRegister() throws IOException {
        String message = "DE-REG,"+nodeIPAddress+","+nodePort;
        sendMessage(lbAddress,lbPort,message);
    }
    private void sendRegistration() throws IOException, InterruptedException {
        System.out.println(this.nodeName+": Sending a registration message for the load balancer at "+lbAddress+":"+lbPort.toString());
        String message = "REG,"+nodeIPAddress+","+nodePort+","+maxJobs;
        sendMessage(lbAddress,lbPort,message);
    }
    private void sendPulse() throws IOException {
        Runnable runnable = () -> {
            Integer time = 0;
            while (running()){
                time++;
                if (time == pulseTime){
                    time=0;
                    try {
                        String messagePulse = "PULSE,"+nodeIPAddress+","+nodePort;
                        sendMessage(lbAddress,lbPort,messagePulse);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                }
            }
        };
        Thread pulseSender = new Thread(runnable);
        pulseSender.start();
    }

    public void killNode(){
        try {
            sendMessage(this.nodeIPAddress, this.nodePort, "abort");
        } catch (IOException e) {
        }
        registered=false;
        running=false;
    }

    public void sendMessage(String host, Integer port, String message) throws IOException {
        try (DatagramSocket socketSend = new DatagramSocket()) {
            InetAddress addr = InetAddress.getByName(host);
            String messageSend = message;
            DatagramPacket packetSend = new DatagramPacket(messageSend.getBytes(StandardCharsets.UTF_8),
                    messageSend.length(),addr, port);
            socketSend.send(packetSend);
            socketSend.close();
        }
    }
    private void regSuccess(Command cmd) throws IOException{
        System.out.println(this.nodeName+": Registration successful!");
        pulseTime = Integer.parseInt(cmd.tokenizer()[1]);
        registered = true;
        sendPulse();
        isLbAlive();
    }

    public boolean getReg(){
        return this.registered;
    }

    private void doJob(Command cmd){
        System.out.println("A new job is received !");
        Runnable runnable = () -> {
            Integer jobTime = Integer.parseInt(cmd.tokenizer()[2]);
            String jobID = cmd.tokenizer()[1];
            System.out.println(nodeName+": Job with ID "+ jobID+" will be delivered in "+jobTime+" seconds.");
        	//this.currentJobs++;
		currentlyRunningJobs("Increment");
            try {
                TimeUnit.SECONDS.sleep(jobTime);
            }catch (InterruptedException e) {
            }		if(registered){
                System.out.println(nodeName+": Job with ID of "+ jobID+" is done ! ");
                String jobDoneMsg = "DONE,"+jobID;
                try {
                    sendMessage(lbAddress,lbPort,jobDoneMsg);
                } catch (IOException e) {
                }
            }else{
                System.out.println(nodeName+": Failed to deliver the job with the ID of "+ jobID);
            }
	    currentlyRunningJobs("Decrement");
            //this.currentJobs--;
        };
        Thread jobThread = new Thread(runnable);
        jobThread.start();
    }

    private boolean receiveData() throws IOException {
        buffer = new byte[1024];
        packet = new DatagramPacket(buffer, buffer.length);

        socket.receive(packet);
        String message = new String(buffer).trim();
        String[] messageSplitted = message.split(",");

        Command cmd = new Command(message);

        switch (cmd.tokenizer()[0]) {
            case "Registration successfull!":
                regSuccess(cmd);
                break;
            case "JOB":
                doJob(cmd);
                break;
            case "alive":
                lbHealth = true;
                break;
        }
        return cmd.isOption("abort");
    }

    @Override
    public void run(){
        try {
            sendRegistration();
        } catch (IOException ioException) {
            return;

        } catch (InterruptedException e) {
        }
        try{
            socket = new DatagramSocket(this.nodePort);
            socket.setSoTimeout(0);
            while (running){
                try{
                    if (receiveData()){
                        break;
                    }
                }catch (IOException ioError){
                }
            }
        }catch (SocketException socketError){
        }finally {
            try {
                deRegister();
            } catch (IOException e) {
            }
        }
        registered = false;
        if (socket != null){
            socket.close();
        }
        System.out.println(this.nodeName+": Aborting...");
    }
}