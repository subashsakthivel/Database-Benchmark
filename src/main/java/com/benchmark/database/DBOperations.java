package com.benchmark.database;

import org.json.JSONArray;
import org.openjdk.jmh.infra.Blackhole;

public interface DBOperations<T> {
    void write(T conn, Blackhole blackhole);
    void read(T conn,Blackhole blackhole);
    void readAll(T conn,Blackhole blackhole);
    void search(T conn,Blackhole blackhole);
    void delete(T conn,Blackhole blackhole);
    void update(T conn,Blackhole blackhole);
    void rangeQuery(T conn,Blackhole blackhole);
    void fullTextQuery(T conn,Blackhole blackhole);
    void filterQuery(T conn,Blackhole blackhole);
}

