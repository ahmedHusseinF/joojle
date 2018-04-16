package Classes;

import java.io.BufferedReader;
import java.io.IOException;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jsoup.Jsoup;

import javax.swing.text.html.HTMLDocument;
import java.util.*;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Indexer {


    private String stopWords[];
    private DBConnection DBconn;

    public static void main(String arg[]) {
        Indexer indexer = new Indexer();

        indexer.run();
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

        return lines.toArray(new String[0]);
    }


    private HashMap<String, Document> parseDocument(String body, String url) throws Exception {

        org.jsoup.nodes.Document doc = Jsoup.parse(body);

        //Parse document into string of words and removing all short words
        String text = doc.text(); // get all text in this document

        // get each word in the doc as seperate string
        List<String> wordsList = new LinkedList<>(Arrays.asList(text.split("\\s+")));

        //Remove all stop words from the parsed document
        for (String stopWord : stopWords) {
            wordsList.removeIf(str -> str.equals(stopWord));
        }

        // remove strings of only numbers and empty strings
        wordsList.removeIf(word -> (word.matches("\\d+") || word.isEmpty()));

        //remove special charcters from words
        UnaryOperator<String> uoRef = (word) -> word.replaceAll("[^\\w]", "");
        wordsList.replaceAll(uoRef);


        //Convert the parsed text document into array of strings
        //String[] words = Arrays.copyOf(wordsList, wordsList.size(), String[].class);//(String[]) wordsList.toArray();
        String[] words = new String[wordsList.size()];
        for (int i = 0; i < wordsList.size(); i++) {
            words[i] = wordsList.get(i);
        }

        // calculate occurences for all words in a map
        HashMap<String, List<Integer>> occurences = new HashMap<>();
        int index = 1;
        for (String wordsIterator : words) {

            if (!occurences.containsKey(wordsIterator)) {
                List<Integer> wordOcc = new ArrayList<>();
                wordOcc.add(index);
                occurences.put(wordsIterator, wordOcc);
            } else {
                List<Integer> wordOcc = occurences.get(wordsIterator);
                wordOcc.add(index);
                occurences.put(wordsIterator, wordOcc);
            }

            index++;
        }


        HashMap<String, Document> wordsDocuments = new HashMap<>();

        for (String word : words) {

            if (wordsDocuments.containsKey(word)) {
                continue; // skip this word we already processed it in this document
            }

            if(word.isEmpty()) {
                continue; // i hate empty words
            }


            wordsDocuments.put(
                    word,
                    new Document()
                            .append("url", url)
                            .append("occurrences", occurences.get(word))
                            .append("count", occurences.get(word).size())
            );

        }


        return wordsDocuments;
    }

    private HashMap<String, String> getLatestUrlData() {
        Document d = DBconn.getLatestEntry(DBConnection.FETCHED_URLs, true);

        if (d == null)
            return null;

        HashMap<String, String> h = new HashMap<>();
        h.put("url", d.get("url", String.class));
        h.put("body", d.get("body", String.class));

        return h;
    }

    private void run() {
        while (true) {
            try {

                HashMap<String, String> data = this.getLatestUrlData();

                if (data == null) {
                    System.out.println("Fetched Urls Collection is Empty");
                    break;
                }

                // update the fetched urls to be indexed and reduce its HUGE size by deleting body's html
                Bson updateFetcehedDocument = Updates.combine(Updates.set("indexed", true), Updates.unset("body"));
                DBconn.updateDocumentInCollection(updateFetcehedDocument, Filters.eq("url", data.get("url")), DBConnection.FETCHED_URLs);

                HashMap<String, Document> wordsDocuments = parseDocument(data.get("body"), data.get("url"));

                saveWordsInDB(wordsDocuments);


                System.out.println("Processed this link: " + data.get("url"));

            } catch (Exception e) {
                System.out.println("generic exception: " + e.toString());
                System.out.println("exception cause: " + e.getCause());
                System.out.print("exception trace: ");
                e.printStackTrace(System.out);
            }
        }
    }

    private void saveWordsInDB(HashMap<String, Document> wordsDocuments) throws Exception {
        //  an insertion query to insert the word int the invertedIndex if it doesn't exisit, if exists just add url into list of urls,
        // if url is already existing just add the index position to array of occurunces.i.e the word existed more than one time in the document

        List<Document> toBeInserted = new ArrayList<>(); // for new words only

        for (HashMap.Entry<String, Document> wordDocument : wordsDocuments.entrySet()) {

            Document ourWordDocument = new Document().append("word", wordDocument.getKey());

            boolean wordExistsInDB = DBconn.isThisObjectExist(Filters.eq("word", wordDocument.getKey()), DBConnection.INDEXED_WORDs);

            if (wordExistsInDB) {

                // update the index
                ArrayList<Document> foundWords =
                        DBconn.getDocumentsByFilter(Filters.eq("word", wordDocument.getKey()), DBConnection.INDEXED_WORDs);

                if (foundWords.size() > 1)
                    throw new Exception("Word: " + wordDocument.getKey() + " have more than 1 entry in DB"); // how did that happen ?

                Document foundWord = foundWords.get(0);

                ArrayList<Document> urls = foundWord.get("urls", ArrayList.class);

                urls.add(
                         wordDocument.getValue()
                );

                Bson updatedParts = Updates.combine(Updates.set("urls", urls));

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
