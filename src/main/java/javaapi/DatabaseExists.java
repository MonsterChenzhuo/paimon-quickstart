package javaapi;

import org.apache.paimon.catalog.Catalog;

public class DatabaseExists {

    public static void main(String[] args) {
        Catalog catalog = CreateCatalog.createFilesystemCatalog();
        boolean exists = catalog.databaseExists("my_db");
        System.out.printf("db exists: " + exists +"");
    }
}