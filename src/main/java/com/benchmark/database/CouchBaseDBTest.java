package com.benchmark.database;

import com.couchbase.client.java.*;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import org.json.JSONArray;
import org.json.JSONObject;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3) // Warmup iterations
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class CouchBaseDBTest implements DBOperations<CouchBaseDBTest.Connection> {

    @State(org.openjdk.jmh.annotations.Scope.Benchmark)
    public static class Connection {
        Cluster cluster;
        Bucket bucket;
        Scope scope;
        Collection collection;
        JSONArray objects;
        public Connection() {
            cluster = Cluster.connect(
                    "couchbase://127.0.0.1",
                    ClusterOptions.clusterOptions("Administrator", "couchbase").environment(env -> {
                        // Customize client settings by calling methods on the "env" variable.
                    })
            );
            bucket = cluster.bucket("stix");
            bucket.waitUntilReady(Duration.ofSeconds(10));
            scope = bucket.scope("_default");
            collection = scope.collection("_default");
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
            cluster.close();
        }
    }

   @Override  @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void write(Connection conn, Blackhole blackhole) {
        String unique = ("_" + System.currentTimeMillis());
        for(int i=0;i<conn.objects.length();i++) {
            JsonObject o = JsonObject.fromJson(String.valueOf(conn.objects.getJSONObject(i)));
            conn.collection.upsert("indicator"+i+ unique,o);
        }
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void read(Connection conn, Blackhole blackhole) {
        String query = "SELECT * FROM _default WHERE ARRAY_CONTAINS(sectors,'technology')";
        try {
            QueryResult result = conn.scope.query(query);
            result.rowsAsObject().forEach(blackhole::consume);
        } catch (Exception e) {
            System.err.println("Error fetching documents: " + e.getMessage());
        }
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void readAll(Connection conn, Blackhole blackhole) {
        String query = "SELECT * FROM _default";
        try {
            QueryResult result = conn.scope.query(query);
            result.rowsAsObject().forEach(blackhole::consume);
        } catch (Exception e) {
            System.err.println("Error fetching documents: " + e.getMessage());
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
        String query = "SELECT * FROM _default WHERE reputation BETWEEN 70 AND 100";
        try {
            QueryResult result = conn.scope.query(query);
            result.rowsAsObject().forEach(blackhole::consume);
        } catch (Exception e) {
            System.err.println("Error executing range query: " + e.getMessage());
        }
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void fullTextQuery(Connection conn, Blackhole blackhole) {
        String query = "SELECT * FROM _default WHERE SEARCH(_default.description, 'analyst-centric')";
        try {
            QueryResult result = conn.scope.query(query);
            result.rowsAsObject().forEach(blackhole::consume);
        } catch (Exception e) {
            System.err.println("Error executing full text query: " + e.getMessage());
        }
    }

   @Override @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void filterQuery(Connection conn, Blackhole blackhole) {
        String query = "SELECT * FROM _default WHERE type = 'identity' AND reputation > 50";
        try {
            QueryResult result = conn.scope.query(query);
            result.rowsAsObject().forEach(blackhole::consume);
        } catch (Exception e) {
            System.err.println("Error executing filter query: " + e.getMessage());
        }
    }


}
