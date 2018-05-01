package Classes;

import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;

public class Ranker {

    private DBConnection db;
    private double DampingFactor;
    public static void main(String[] args) {

        new Ranker();

    }

    Ranker(){
        this.db = DBConnection.getInstance();
        for(int j=0;j<100;j++)
        {
            this.pageRanker();
        }
    }
    
    private void pageRanker() {

        HashMap<String, Document> arr = db.getDocumentsByFilter(new Document(), DBConnection.FETCHED_URLs);

        for(HashMap.Entry<String, Document> el : arr.entrySet()){
            String URL=el.getKey();

            @SuppressWarnings("unchecked")
            ArrayList<String> inLinks=  el.getValue().get("inLinks", ArrayList.class);

            double PageRank=(1-this.DampingFactor);
            for(int i=0;i<inLinks.size();i++)
            {
                PageRank+=this.DampingFactor*(arr.get(inLinks.get(i)).getDouble("rank")/arr.get(inLinks.get(i)).getDouble("outLinks"));
            }

            el.getValue().put("rank",PageRank);

        }

    }

    private void tfidf()
    {
        
    }
}
