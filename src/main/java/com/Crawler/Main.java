package com.Crawler;

import com.mongodb.client.FindIterable;
import org.bson.Document;

import javax.print.Doc;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static final int Thread_count = 10;

    public static void main(String[] args) throws IOException {
        dbConnector db = new dbConnector();
        Web_Crawling.setTagPriorities();
        Web_Crawling.setDBObject(db);
        int kam_crawl=0;
        while (true) {
            System.out.println("ana fel crawlaayaa: "+(++kam_crawl));
            System.out.println("to_crawl "+Web_Crawling.getTo_crawl().get());
            long startTime = System.currentTimeMillis();
            ArrayList<String> urls = new ArrayList<>();

            //db.clean();
            // db.insertDocument("https://www.google.com.eg");
            // break;

            FindIterable<Document> iterDocs = db.getAllDocuments();
            // Getting the iterator
            //Iterator it = iterDocs.iterator();
            //split the documents
            ArrayList<String> to_crawl_urls = new ArrayList<>();
            Map<String, Url_Data> visited = Collections.synchronizedMap(new HashMap<>());
            for (Document iterDoc : iterDocs) {
                String url = iterDoc.getString("url");
                if (iterDoc.getInteger("crawled") == 1) {
                    Url_Data url_data = new Url_Data();
                    url_data.addText((ArrayList<String>) iterDoc.get("url_data"));
                    url_data.addTag((ArrayList<Integer>) iterDoc.get("tags"));
                    visited.put(url, url_data);
                } else {
                    to_crawl_urls.add(url);
                }

            }
            if (!to_crawl_urls.isEmpty()) {
                //set the current queue and visited map
                try {
                    Web_Crawling.setCurrent_urls(to_crawl_urls);
                    Web_Crawling.setVisited(visited);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ArrayList<Thread> threads = new ArrayList<>();

                for (int i = 0; i < Thread_count; ++i) {
                    threads.add(new Thread(new Web_Crawling()));
                }
                for (int i = 0; i < Thread_count; ++i) threads.get(i).start();
                for (int i = 0; i < Thread_count; ++i) {
                    try {
                        threads.get(i).join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
          BlockingQueue<String> arr=Web_Crawling.getFinal_urls();
            System.out.println("final size: "+arr.size());
            Set<String>seet=new HashSet<>();
            for(String arrr: arr){
                seet.add(arrr);
            }
            System.out.println("unique: "+seet.size());
            db.printDocs();
            db.removeOverhead();


            //reset the ones with high priority
            db.sort_and_update();
            System.out.println("able el set bta3 to_crawl: "+Web_Crawling.getTo_crawl().get());
            db.setRecrawl();
            System.out.println("b3d el set bta3 to_crawl: "+Web_Crawling.getTo_crawl().get());

            System.out.println(db.printDocs());
            System.out.println(db.getCrawlReminder());


        }



    }
}