package loadBalancerPackage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class loadBalancerManager {

    Integer port;
    public loadBalancerManager(Integer port){
        this.port=port;
    }

    public void lbPrompt(loadBalancer loadBalancer){
        Runnable runnable = () -> {
            Scanner myScanner = new Scanner(System.in);
            String input="";
            try {
                TimeUnit.MILLISECONDS.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!input.equals("exit")){
                System.out.println("LBM > ");
                input = myScanner.nextLine();
                try {
                    switch (input.split(" ")[0]){
                        case "help":
                            System.out.println("jobs: See all the jobs.\nworkers: See all the workers\nde-reg WorkerX (E.g Worker2): "
                                    + "Send de-reg message to the workerX\nexit: Exit the manager.");
                            break;
                        case "jobs":
                            HashMap<String, HashMap<String,String>> jobs = loadBalancer.getJobs();
                            if (jobs.size()<1){
                                System.out.println("There is not any job.");
                            }else{
                                for (Map.Entry<String, HashMap<String, String>> job : jobs.entrySet()){
                                    System.out.println(job);
                                }
                            }
                            break;
                        case "workers":
                            HashMap<String, HashMap<String,String>> workers = loadBalancer.getWorkers();
                            if (workers.size()<1){
                                System.out.println("There is not any registered worker.");
                            }else{
                                for (Map.Entry<String, HashMap<String, String>> worker : workers.entrySet()){
                                    System.out.println(worker);
                                }
                            }
                            break;
                        case "messages":
                            loadBalancer.seeMessages();
                            break;
                        case "de-reg":
                            if (loadBalancer.getWorkers().containsKey(input.split(" ")[1])){
                                try {
                                    loadBalancer.deRegNode(input.split(" ")[1]);
                                } catch (IOException e) {
                                }
                            }else{
                                System.out.println(input.split(" ")[1]+" does not exists.");
                            }
                            break;
                    }

                }catch (Exception e){
                    System.out.println("Try again. E.g 'de-reg Worker2'");
                }

            }
            try {
                loadBalancer.sendMessage("localhost",port,"Exit");
                System.out.println("Shutting the load balancer down.");
                loadBalancer.running=false;
            } catch (IOException ex) {
                Logger.getLogger(loadBalancerManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        };
        Thread lbPrompt = new Thread(runnable);
        lbPrompt.start();
    }
    public static void main(String[] args) throws IOException {
        System.out.println("");
        System.out.println("----------------------------------------------------------------------------------------------------------------------------");
        System.out.println(" /$$       /$$$$$$  /$$$$$$ /$$$$$$$        /$$$$$$$  /$$$$$$ /$$       /$$$$$$ /$$   /$$ /$$$$$$ /$$$$$$$$/$$$$$$$ ") ;
        System.out.println("| $$      /$$__  $$/$$__  $| $$__  $$      | $$__  $$/$$__  $| $$      /$$__  $| $$$ | $$/$$__  $| $$_____| $$__  $$");
        System.out.println("| $$     | $$  \\ $| $$  \\ $| $$  \\ $$      | $$  \\ $| $$  \\ $| $$     | $$  \\ $| $$$$| $| $$  \\__| $$     | $$  \\ $$");
        System.out.println("| $$     | $$  | $| $$$$$$$| $$  | $$      | $$$$$$$| $$$$$$$| $$     | $$$$$$$| $$ $$ $| $$     | $$$$$  | $$$$$$$/");
        System.out.println("| $$     | $$  | $| $$__  $| $$  | $$      | $$__  $| $$__  $| $$     | $$__  $| $$  $$$| $$     | $$__\\/  | $$__  $$");
        System.out.println("| $$     | $$  | $| $$  | $| $$  | $$      | $$  \\ $| $$  | $| $$     | $$  | $| $$\\  $$| $$    $| $$     | $$  \\ $$");
        System.out.println("| $$$$$$$|  $$$$$$| $$  | $| $$$$$$$/      | $$$$$$$| $$  | $| $$$$$$$| $$  | $| $$ \\  $|  $$$$$$| $$$$$$$| $$  | $$");
        System.out.println("|________/\\______/|__/  |__|_______/       |_______/|__/  |__|________|__/  |__|__/  \\__/\\______/|________|__/  |__/");
        System.out.println("Burak Baris-T0161214--------------------------------------------------------------------------------------------------------");
        System.out.println("");
        System.out.println("Welcome to the Load Balancer Manager.");
        System.out.println("Declare a port for the load balancer to run between 4000-65535: ");
        Scanner myScanner = new Scanner(System.in);
        Integer port;

        while (true){
            port = myScanner.nextInt();
            if (!(port>4000) || (!(port<65535))){
                System.out.println("Declare a port for the load balancer to run between 4000-65535: ");
                continue;
            }
            break;
        }

        loadBalancer loadBalancer = new loadBalancer(port);
        loadBalancerManager lbm = new loadBalancerManager(port);
        lbm.lbPrompt(loadBalancer);
        loadBalancer.start();
    }
}