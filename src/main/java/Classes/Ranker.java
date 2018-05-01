package Classes;

import org.bson.Document;


import java.lang.reflect.Array;
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
        TFIDF();
    }
    
    private void pageRanker() {

        HashMap<String, Document> arr = db.getDocumentsByFilter(new Document(), DBConnection.FETCHED_URLs);

        for(HashMap.Entry<String, Document> el : arr.entrySet()){
            el.getKey();

            @SuppressWarnings("unchecked")
            ArrayList<String> aaa=  el.getValue().get("inLinks", ArrayList.class);
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


    private void TFIDF()
    {
        HashMap<String, Document> arr = db.getDocumentsByFilter(new Document(), DBConnection.INDEXED_WORDs);

        long NumberOfPages= db.getCollectionSize(DBConnection.FETCHED_URLs);
        for(HashMap.Entry<String, Document> el : arr.entrySet()){
            String Word=el.getKey();
            int TotalWordCount=0;


            ArrayList<Document> Urls= el.getValue().get("urls", ArrayList.class);

            for(int i=0;i<Urls.size();i++)
            TotalWordCount+=(int)(Urls.get(i).getInteger("count"));
            double idf=Math.log(TotalWordCount/NumberOfPages);
            double  tf=0;

            for(int i=0;i<Urls.size();i++) {

                //tb mana bardo hena 3ayzhom
                //el word count w total number of word fil page

                @SuppressWarnings("unchecked")
                int WordOccurence = (int)(Urls.get(i).getInteger("count"));
                @SuppressWarnings("unchecked")
                int WordsInDocument = (int)(Urls.get(i).getInteger("allWordsCount"));

                tf=WordOccurence/WordsInDocument;

                double WordRank=tf+idf;

              Urls.get(i).put("Rank",WordRank);

            }

        }
    }
}
