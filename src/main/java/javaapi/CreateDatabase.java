package javaapi;

import org.apache.paimon.catalog.Catalog;

public class CreateDatabase {

    public static void main(String[] args) {
        // 在创建 的catalog目录下 改为带.db
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.createDatabase("my_db", false);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // do something
        }
    }
}