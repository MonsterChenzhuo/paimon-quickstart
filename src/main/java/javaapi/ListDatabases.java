package javaapi;

import org.apache.paimon.catalog.Catalog;

import java.util.List;

import static org.apache.hadoop.util.functional.RemoteIterators.foreach;

public class ListDatabases {

    public static void main(String[] args) {
        Catalog catalog = CreateCatalog.createFilesystemCatalog();
        List<String> databases = catalog.listDatabases();
        for( String it : databases)
        {
            System.out.println(it);
        }

    }
}