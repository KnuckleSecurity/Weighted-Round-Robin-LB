package loadBalancerPackage;
public class Command {
    private String type = null;
    private String[] tokenized;
    public Command(String message) {
        tokenized = message.split(",");
        this.type=tokenized[0];

    }
    public boolean isOption(String option){
        return type.equalsIgnoreCase(option);
    }
    public String getType(){
        return type;
    }
    public String[] tokenizer(){
        return tokenized;
    }
}