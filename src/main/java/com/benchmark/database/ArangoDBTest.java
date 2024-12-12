package com.benchmark.database;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import org.json.JSONArray;
import org.json.JSONObject;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3) // Warmup iterations
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ArangoDBTest implements DBOperations<ArangoDBTest.Connection> {

    @State(Scope.Benchmark)
    public static class Connection {
        public ArangoDB db;
        public ArangoDatabase database;
        public ArangoCollection collection;
        JSONArray objects;
        public Connection() {
            db = new ArangoDB.Builder()
                    .host("localhost", 8529)
                    .user("root")
                    .password("BG4mlNZbJhHZjgdi")
                    .build();
            database = db.db("threat");
            collection = database.collection("stix");
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
            db.shutdown();
        }

    }

   @Override  @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void write(Connection conn,Blackhole blackhole) {
        for(int i=0;i<conn.objects.length();i++) {
            BaseDocument document = new BaseDocument();
            document.setProperties(conn.objects.getJSONObject(i).toMap());
            conn.collection.insertDocument(document);
        }
    }

   @Override  @Benchmark  @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void read(Connection conn,Blackhole blackhole) {
        String query = "FOR doc IN stix FILTER 'technology' IN doc.sectors RETURN doc";
        ArangoCursor<BaseDocument> cursor = conn.database.query(query, BaseDocument.class);
        if (cursor.hasNext()) {
            BaseDocument doc = cursor.next();
            JSONObject json = new JSONObject(doc.getProperties());
            blackhole.consume(json);
        }
    }

   @Override  @Benchmark  @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void readAll(Connection conn,Blackhole blackhole) {
        String query = "FOR doc IN stix RETURN doc";
        ArangoCursor<BaseDocument> cursor= conn.database.query(query , BaseDocument.class);
        cursor.forEachRemaining(document -> {
            JSONObject json = new JSONObject(document.getProperties());
            blackhole.consume(json);
        });
    }

   @Override
    public void search(Connection conn,Blackhole blackhole) {

    }

   @Override
    public void delete(Connection conn,Blackhole blackhole) {

    }

   @Override
    public void update(Connection conn,Blackhole blackhole) {

    }

   @Override  @Benchmark  @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void rangeQuery(Connection conn,Blackhole blackhole) {
        String query = "FOR doc IN stix FILTER doc.reputation >= 70 && doc.reputation <= 100 RETURN doc";
        ArangoCursor<BaseDocument> cursor = conn.database.query(query, BaseDocument.class, null,null);
        cursor.forEachRemaining(document -> {
            JSONObject json = new JSONObject(document.getProperties());
            blackhole.consume(json);
        });
    }

   @Override  @Benchmark  @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void fullTextQuery(Connection conn,Blackhole blackhole) {
        String query = "FOR doc IN Tstix  SEARCH ANALYZER(doc.description IN TOKENS(\"Pulsedive\", \"text_en\"), \"text_en\") RETURN doc";
        ArangoCursor<BaseDocument> cursor = conn.database.query(query, BaseDocument.class);
        cursor.forEachRemaining(document -> {
            JSONObject json = new JSONObject(document.getProperties());
            blackhole.consume(json);
        });
    }

   @Override  @Benchmark  @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void filterQuery(Connection conn,Blackhole blackhole) {
        String query = "FOR doc IN stix FILTER doc.type == 'identity' && doc.reputation > 50 RETURN doc";
        ArangoCursor<BaseDocument> cursor = conn.database.query(query, BaseDocument.class);
        cursor.forEachRemaining(document -> {
            JSONObject json = new JSONObject(document.getProperties());
            blackhole.consume(json);
        });
    }
}
