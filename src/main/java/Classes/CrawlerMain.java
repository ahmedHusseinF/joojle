package Classes;

public class CrawlerMain {
    public static void main(String args[]){

        int threadsLimit;

        if(args.length == 0) {

            threadsLimit = 5;
            System.out.println("Default Threads limit (5) is being used");

        }else {
            threadsLimit = Integer.parseInt(args[0]);
        }

        for(int i=0;i<threadsLimit;i++){
            (new Crawler()).start();
        }

        //new Crawler().run();
    }
}

