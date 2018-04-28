package com.Crawler;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import javax.print.Doc;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Sorts.descending;

public class dbConnector {
    private MongoClient mongoClient;
    private MongoDatabase database;
    private static final int PRIORITY = 100;
    private static final int MAX_CRAWL = 50;
    private static final int ID = 1911;
    private static final int MAX_RECRAWL =50;

    MongoCollection<Document> documents, to_crawl,users;

    private boolean collectionExists(final String collectionName) {
        MongoIterable<String> collectionNames = database.listCollectionNames();
        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    dbConnector() {
        mongoClient = new MongoClient("localhost", 27017);
        database = mongoClient.getDatabase("APT");
        if (!collectionExists("documents")) {
            database.createCollection("documents");
        }
        if (!collectionExists("to_crawl_coll")) {
            database.createCollection("to_crawl_coll");
        }
        if (!collectionExists("users")) {
            database.createCollection("users");
        }
        documents = database.getCollection("documents");
        users = database.getCollection("users");
        to_crawl = database.getCollection("to_crawl_coll");

        clean();
        insertDocument("https://www.google.com.eg","");
        //insertDocument("https://www.wikipedia.org/");

        Document cur = to_crawl.find(new Document("id", ID)).first();
        if (cur == null) {
            Document page = new Document();
            page.append("to_crawl", MAX_CRAWL)
                    .append("id", ID);
            to_crawl.insertOne(page);
            Web_Crawling.setTo_crawl(MAX_CRAWL);
        }
        else {
            Web_Crawling.setTo_crawl(cur.getInteger("to_crawl"));
        }
    }

    public void clean() {
        documents.deleteMany(new Document());
        to_crawl.deleteMany(new Document());
    }

    public void insertDocument(String url,String from) {
        Document page = new Document();

        Document cur = documents.find(new Document("url", url)).first();
        ArrayList<String> arr = new ArrayList<>();
        if (cur == null) {
            page = new Document("url", url);
            page.append("crawled", 0)
                    .append("to_index", 0)
                    .append("priority", PRIORITY)
                    .append("from",from)
                    .append("in",arr)
                    .append("out",arr)
                    .append("numOfWords",0)
                    .append("page_rank" , 0.0)
                    .append("title" , "")
                    .append("interested" , arr);
            documents.insertOne(page);
        }


    }

    public void updateDocument(String url, Url_Data url_data,String from,int numOfWords , String title) {
        BasicDBObject updateQuery;

        Document cur = documents.find(new Document("url", url)).first();
        if(cur==null){
            insertDocument(url,from);
            cur=documents.find(new Document("url", url)).first();
        }

        ArrayList<String> texts_db = (ArrayList<String>) cur.get("url_data");
        ArrayList<String> interested = (ArrayList<String>) cur.get("interested");
        ArrayList<Integer> tags_db = (ArrayList<Integer>) cur.get("tags");
        ArrayList<String> texts = url_data.getText();
        ArrayList<Integer> tags = url_data.getTags();
        if (texts_db != null) {
            if (texts.size() == texts_db.size()) {
                boolean same = true;
                for (int i = 0; i < texts.size(); ++i) {
                    if (!(texts.get(i).equals(texts_db.get(i))) || tags.get(i)!=tags_db.get(i)) {
                        same = false;
                        break;
                    }
                }
                if (same) {
                    updateQuery = new BasicDBObject();
                    updateQuery.put("$set", new BasicDBObject().append("crawled", 1)
                            .append("priority", cur.getInteger("priority") - 1));
                    BasicDBObject updateObject = new BasicDBObject("url", url);
                    documents.updateOne(updateObject, updateQuery);
                    return;
                }
            }
        }
        updateQuery = new BasicDBObject();
        updateQuery.put("$set", new BasicDBObject().append("url_data", texts)
                .append("tags", tags)
                .append("crawled", 1)
                .append("to_index", 1)
                .append("numOfWords",numOfWords)
                .append("priority", cur.getInteger("priority") + 1)
                .append("title" , title));
        for(int i = 0 ; i<interested.size() ; i++)
        {
            String user = interested.get(i);
            DBObject listItem = new BasicDBObject("notifications_urls", url);
            BasicDBObject updateQueryy = new BasicDBObject("$addToSet", listItem);
            BasicDBObject updateObject = new BasicDBObject("username", user);
            users.updateOne(updateObject, updateQueryy);
            listItem = new BasicDBObject("notifications_titles", title);
            updateQueryy = new BasicDBObject("$addToSet", listItem);
            users.updateOne(updateObject, updateQueryy);
        }
        // apply the update to the database
        BasicDBObject updateObject = new BasicDBObject("url", url);
        documents.updateOne(updateObject, updateQuery);

    }

    public int getCrawlReminder() {
        Document cur = to_crawl.find(new Document("id", ID)).first();
        return cur.getInteger("to_crawl");
    }

    public void updateCrawlReminder(AtomicInteger to_crawll) {
        BasicDBObject updateQuery;
        //Document cur = documents.find(new Document("url", url)).first();
        updateQuery = new BasicDBObject();
        updateQuery.put("$set", new BasicDBObject().append("to_crawl", to_crawll));
        BasicDBObject updateObject = new BasicDBObject("id", ID);
        to_crawl.updateOne(updateObject, updateQuery);
        System.out.println("to_crawl: "+to_crawll);

    }

    public void setRecrawl() {
        BasicDBObject updateQuery;

        //Document cur = documents.find(new Document("url", url)).first();
        updateQuery = new BasicDBObject();
        updateQuery.put("$set", new BasicDBObject().append("to_crawl", MAX_CRAWL));
        BasicDBObject updateObject = new BasicDBObject("id", ID);
        to_crawl.updateOne(updateObject, updateQuery);
        //edafa
        Web_Crawling.setTo_crawl(MAX_CRAWL);
    }

    public FindIterable<Document> getAllDocuments() {
        return documents.find();
    }

    public void sort_and_update() {
        FindIterable<Document> docs = documents.find().sort(descending("priority")).limit(MAX_RECRAWL);
        for (Document doc : docs) {
            BasicDBObject updateQuery;
            String url = doc.getString("url");
            //Document cur = documents.find(new Document("url", url)).first();
            updateQuery = new BasicDBObject();
            updateQuery.put("$set", new BasicDBObject().append("crawled", 0));
            BasicDBObject updateObject = new BasicDBObject("url", url);
            documents.updateOne(updateObject, updateQuery);
        }
    }

    public void setAllOne() {
        FindIterable<Document> docs = documents.find();
        for (Document doc : docs) {
            BasicDBObject updateQuery;
            String url = doc.getString("url");
            //Document cur = documents.find(new Document("url", url)).first();
            updateQuery = new BasicDBObject();
            updateQuery.put("$set", new BasicDBObject().append("crawled", 1));
            BasicDBObject updateObject = new BasicDBObject("url", url);
            documents.updateOne(updateObject, updateQuery);
        }
    }

    public int printDocs() throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter("out.txt");
        // Getting the iterable object
        FindIterable<Document> iterDoc = documents.find();
        // Getting the iterator
        Iterator it = iterDoc.iterator();
        int cnt=0;
        while (it.hasNext()) {
            writer.println(it.next());
            ++cnt;
        }
        writer.close();
        return cnt;
    }

    public void removeOverhead() {
        BasicDBObject query = new BasicDBObject();
        query.append("crawled" , 0);
        documents.deleteMany(query);
    }

    public void tzbet_in_out(String url,String from){
        BasicDBObject updateQuery;
        Document cur = documents.find(new Document("url", url)).first();
        String from_db = (String) cur.get("from");
        if(from_db.isEmpty())return;



        DBObject listItem = new BasicDBObject("out", url);
        updateQuery = new BasicDBObject("$addToSet", listItem);
        BasicDBObject updateObject = new BasicDBObject("url", from);
        documents.updateOne(updateObject, updateQuery);


        listItem = new BasicDBObject("in", from);
        updateQuery = new BasicDBObject("$addToSet", listItem);
        updateObject = new BasicDBObject("url", url);
        documents.updateOne(updateObject, updateQuery);
    }
    public void updatePageRank(double [] PR , ArrayList<String> keys)
    {
        List<WriteModel<Document>> updates = new ArrayList<>();
        for(int i = 0 ; i<PR.length ; i++)
        {
            BasicDBObject updateObject = new BasicDBObject("url", keys.get(i));
            BasicDBObject updateQuery = new BasicDBObject("$set", new Document("page_rank" , PR[i]));
            updates.add(
                    new UpdateOneModel<>(
                            updateObject,
                            updateQuery
                    )
            );
        }
        if(!updates.isEmpty()) documents.bulkWrite(updates);
    }


    public void close() {
        mongoClient.close();
    }

}