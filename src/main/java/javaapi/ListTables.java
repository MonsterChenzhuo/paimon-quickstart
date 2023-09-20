package javaapi;

import org.apache.paimon.catalog.Catalog;

import java.util.List;

public class ListTables {

    public static void main(String[] args) {
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            List<String> tables = catalog.listTables("my_db");
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }
    }
}