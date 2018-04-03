package Classes;

import java.io.BufferedReader;
import java.io.IOException;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jsoup.Jsoup;

import java.util.*;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Indexer extends Thread {


    private String stopWords[];
    private DBConnection DBconn;

    public static void main(String arg[]) {
        Indexer indexer = new Indexer();

        indexer.start();
    }

    private Indexer() {
        stopWords = getStopWords();

        DBconn = DBConnection.getInstance();
    }

    private String[] getStopWords() {
        FileReader fileReader;
        List<String> lines = new ArrayList<>();
        try {
            fileReader = new FileReader("StopWords.txt");


            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
            bufferedReader.close();


        } catch (FileNotFoundException exception) {
            System.out.println("The file was not found.");
        } catch (IOException exception) {

            System.out.println(exception.getMessage());
        }

        return lines.toArray(new String[lines.size()]);
    }


    private void parseDocument(String html, String url) {

        org.jsoup.nodes.Document doc = Jsoup.parse(html);

        //Parse document into string of words and removing all short words
        String text = doc.text(); // get all text in this document


        //Remove all stop words from the parsed document
        for (String stopWord : stopWords) {
            Matcher matchStopWords = Pattern.compile(" " + stopWord + " ").matcher(text);

            text = matchStopWords.replaceAll(" ");
        }


        //Convert the parsed text document into array of strings
        String[] words = text.split("\\s+");

        //remove special charcters from words
        for (int i = 0; i < words.length; i++) {
            words[i] = words[i].replaceAll("[^\\w]", "");
        }

        int wordCountInText;

        HashMap<String, Document> wordsDocuments = new HashMap<>();

        // insert all words in database while having
        for (String word : words) {

            if (wordsDocuments.containsKey(word)) {
                continue; // skip this word we already processed it in this document
            }

            wordCountInText = 0;

            Matcher wordPattern = Pattern
                    .compile(word)
                    .matcher(text);

            while (wordPattern.find())
                wordCountInText++;


            ArrayList<Integer> occurrences = new ArrayList<>();

            // find all occurrences of string
            //int index = text.indexOf(word);
            //while (index >= 0) {
            //    occurrences.add(index);
            //    index = text.indexOf(word, index + 1);
            //}


            wordsDocuments.put(
                    word,
                    new Document()
                            .append("url", url)
                            //.append("occurrences", (occurrences.size() == 0) ? "" : occurrences.subList(0, occurrences.size() - 1))
                            .append("count", wordCountInText)
            );

        }

        saveWordsInDB(wordsDocuments);
    }

    private HashMap<String, Object> getLatestUrlData() {
        org.bson.Document d = DBconn.getLatestEntry(DBConnection.FETCHED_URLs, true);

        if (d == null)
            return null;

        HashMap<String, Object> h = new HashMap<>();
        h.put("url", d.get("url", String.class));
        h.put("body", d.get("body", String.class));

        return h;
    }

    public void run() {
        while (true) {
            try {

                HashMap data = this.getLatestUrlData();

                if (data == null)
                    throw new Exception("Nulled Data");

                parseDocument(data.get("body").toString(), data.get("url").toString());

                // update the fetched urls to be indexed and reduce its HUGE size by deleting body's html
                Bson updateFetcehedDocument = Updates.combine(Updates.set("indexed", true), Updates.unset("body"));

                DBconn.updateDocumentInCollection(updateFetcehedDocument, Filters.eq("url", data.get("url").toString()), DBConnection.FETCHED_URLs);

                System.out.println("Processed this link: " + data.get("url").toString());


            } catch (Exception e) {
                System.out.println("generic exception: " + e.toString());
                System.out.println("exception cause: " + e.getCause());
                System.out.print("exception trace: ");
                e.printStackTrace(System.out);
            }
        }
    }

    private void saveWordsInDB(HashMap<String, Document> wordsDocuments) {
        //  an insertion query to insert the word int the invertedIndex if it doesn't exisit, if exists just add url into list of urls,
        // if url is already existing just add the index position to array of occurunces.i.e the word existed more than one time in the document

        List<Document> toBeInserted = new ArrayList<>(); // for new words only


        for (HashMap.Entry<String, Document> wordDocument : wordsDocuments.entrySet()) {

            Document ourWordDocument = new Document().append("word", wordDocument.getKey());

            if (DBconn.isThisObjectExist(Filters.eq("word", wordDocument.getKey()), DBConnection.INDEXED_WORDs)) {

                // update the index
                ArrayList<Document> foundWords =
                        DBconn.getDocumentsByFilter(Filters.eq("word", wordDocument.getKey()), DBConnection.INDEXED_WORDs);

                if (foundWords.size() > 1)
                    foundWords.size(); // getting away with it

                Document foundWord = foundWords.get(0);

                ArrayList<Document> urls = foundWord.get("urls", ArrayList.class);

                urls.add(
                        wordDocument.getValue()
                );

                Bson updatedParts = Updates.combine(Updates.set("urls", urls.subList(0, urls.size() - 1)));


                // update words as they are seen in the loop
                DBconn.updateDocumentInCollection(updatedParts, Filters.eq("word", wordDocument.getKey()), DBConnection.INDEXED_WORDs);


            } else {
                // first time to insert the word in the index
                ourWordDocument.append(
                        "urls", Collections.singletonList(
                                wordDocument.getValue()
                        )
                );

                toBeInserted.add(ourWordDocument);
            }

        }


        if (toBeInserted.size() != 0)
            DBconn.insertManyIntoCollection(toBeInserted, DBConnection.INDEXED_WORDs);

    }


}
