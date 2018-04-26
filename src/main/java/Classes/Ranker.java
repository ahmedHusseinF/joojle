package Classes;

import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;

public class Ranker {

    private DBConnection db;

    public static void main(String[] args) {


    }

    Ranker(){
        this.db = DBConnection.getInstance();
    }
    
    private void pageRanker() {

        HashMap<String, Document> arr = db.getDocumentsByFilter(new Document(), DBConnection.FETCHED_URLs);

        for(HashMap.Entry<String, Document> el : arr.entrySet()){
            el.getKey();

            @SuppressWarnings("unchecked")
            ArrayList<String> aaa=  el.getValue().get("inLinks", ArrayList.class);
        }

    }
}
