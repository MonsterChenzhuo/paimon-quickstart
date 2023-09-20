package javaapi;

import org.apache.paimon.catalog.Catalog;

public class DropDatabase {

    public static void main(String[] args) {
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.dropDatabase("my_db", false, true);
        } catch (Catalog.DatabaseNotEmptyException e) {
            // do something
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }
    }
}