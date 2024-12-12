package com.benchmark.database;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.openjdk.jmh.annotations.*;

import com.mongodb.client.model.IndexOptions;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3) // Warmup iterations
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class MongoDBTest implements DBOperations<MongoDBTest.Connection> {


    @State(Scope.Benchmark)
    public static class Connection {

        MongoClient mongoClient;
        MongoDatabase database;
        MongoCollection<Document> collection;
        JSONArray objects;

        public Connection() {
            mongoClient = MongoClients.create("mongodb://localhost:27017/threat");
            database = mongoClient.getDatabase("threat");
            collection = database.getCollection("stix");
            collection.createIndex(new Document("description", "text"), new IndexOptions());
            String content = null;
            try {
                content = new String(Files.readAllBytes(Paths.get("D:\\threat\\stix2.json")));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            JSONObject jsonObject = new JSONObject(content);
            objects = jsonObject.getJSONArray("objects");
        }

        @TearDown(Level.Trial)
        public void teardown() {
            mongoClient.close();
        }
    }


   @Override @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void write(Connection conn, Blackhole blackhole) {
        List<Document> documents = new ArrayList<>();
        for(int i=0;i<conn.objects.length();i++) {
            documents.add(Document.parse(conn.objects.getJSONObject(i).toString()));
        }
        conn.collection.insertMany(documents);
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void read(Connection conn, Blackhole blackhole) {
        Document doc = conn.collection.find(Filters.eq("sectors", "technology")).first();
        if (doc != null) {
            blackhole.consume(doc.toJson());
        }
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void readAll(Connection conn, Blackhole blackhole) {
        FindIterable<Document> documents = conn.collection.find();
        for (Document doc : documents) {
            blackhole.consume(doc.toJson());
        }
    }

   @Override
    public void search(Connection conn, Blackhole blackhole) {

    }

   @Override
    public void delete(Connection conn, Blackhole blackhole) {

    }

   @Override
    public void update(Connection conn, Blackhole blackhole) {

    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void rangeQuery(Connection conn, Blackhole blackhole) {
        FindIterable<Document> results = conn.collection.find(Filters.and(
                Filters.gte("reputation", 60),  // age >= 60
                Filters.lte("reputation", 100) // age <= 100
        ));
        for (Document doc : results) {
            blackhole.consume(doc.toJson());
        }
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void fullTextQuery(Connection conn, Blackhole blackhole) {
        FindIterable<Document> results = conn.collection.find(Filters.text("Pulsedive"));

        for (Document doc : results) {
            blackhole.consume(doc.toJson());
        }
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void filterQuery(Connection conn, Blackhole blackhole) {
        FindIterable<Document> results = conn.collection.find(Filters.and(
                Filters.eq("type", "identity"),
                Filters.gt("reputation", 50)
        ));

        for (Document doc : results) {
            blackhole.consume(doc.toJson());
        }
    }


}
