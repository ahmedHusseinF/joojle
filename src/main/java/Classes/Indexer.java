package Classes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;
import org.bson.Document;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.Scanner;
import java.util.List;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List
import java.io.FileNotFoundException;

public class Indexer extends Thread {



    String stopWords[];
    Indexer()
    {
        stopWords=getStopWords();

    }

    public String[] getStopWords() {
        FileReader fileReader;
        List<String> lines = new ArrayList<String>();
        try {
            fileReader = new FileReader("StopWords.txt");


            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
            bufferedReader.close();

        }
        catch (FileNotFoundException exception) {
            System.out.println("The file was not found.");
        } catch (IOException exception) {
            System.out.println(exception);
        }
        return lines.toArray(new String[lines.size()]);

    }


    private void parseDocument(String html, String url) throws IOException {

        //TODO: function to check if the document was indexed before in the search index,if so it should return


        org.jsoup.nodes.Document doc = Jsoup.parse(html);

        Element title = doc.head().selectFirst("title");
        if(title == null)
            title = doc.head().selectFirst("meta[name='og:title']");


        Element description = doc.head().selectFirst("meta[name='description']");
        if(description == null)
            description = doc.head().selectFirst("meta[name='og:description']");
        if(description == null)
            description = doc.head().selectFirst("meta[name='twitter:description']");

        //Parse document into string of words and removing all short words
        String text = doc.body().text(); // get all text in this document


        //Remove all stop words from the parsed document
        for(int i=0;i<stopWords.length;i++)
        {
            text.replaceAll(stopWords[i],"");
        }



        //TODO: law 3rfna inshallah ne3ml stemming


        //Convert the parsed text document into array of strings
        String[] words=text.split("\\s+");
        //remove special charcters from words
        for(int i=0;i<words.length;i++)
        {
            words[i]=words[i].replaceAll("[^\\w]","");
        }

        for(int i=0;i<words.length;i++)
        {
            this.Insert(words[i],url,i);
        }


    }
    public void run()
    {
    //TODO: for each url in the seed set call the parse document function
    }

    void Insert(String word,String url,int position)
    {
       //TODO: an insertion query to insert the word int the invertedIndex if it doesn't exisit, if exists just add url into list of urls,
       // TODO:if url is already existing just add the index position to array of occurunces.i.e the word existed more than one time in the document
    }




}
