package javaapi;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

public class RenameTable {

    public static void main(String[] args) {
        Identifier fromTableIdentifier = Identifier.create("my_db", "my_table");
        Identifier toTableIdentifier = Identifier.create("my_db", "test_table");
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.renameTable(fromTableIdentifier, toTableIdentifier, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
        } catch (Catalog.TableNotExistException e) {
            // do something
        }
    }
}