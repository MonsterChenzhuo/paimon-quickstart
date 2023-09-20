package javaapi;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

public class CreateCatalog {

    public static void main(String[] args) {
        createFilesystemCatalog();
    }

    public static Catalog createFilesystemCatalog() {
        //在文件系统指定的路径下生成目录
        CatalogContext context = CatalogContext.create(new Path("file:///Users/leicq/share_dir/my_test"));
        return CatalogFactory.createCatalog(context);
    }

    public static Catalog createHiveCatalog() {
        // Paimon Hive catalog relies on Hive jars
        // You should add hive classpath or hive bundled jar.
        Options options = new Options();
        options.set("warehouse", "...");
        options.set("metastore", "hive");
        options.set("uri", "...");
        options.set("hive-conf-dir", "...");
        options.set("hadoop-conf-dir", "...");
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
}
