package javaapi;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

public class TableExists {

    public static void main(String[] args) {
        Identifier identifier = Identifier.create("my_db", "my_table");
        Catalog catalog = CreateCatalog.createFilesystemCatalog();
        boolean exists = catalog.tableExists(identifier);
        System.out.printf("table exists: " + exists);
    }
}