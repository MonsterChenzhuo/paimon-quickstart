package flinkapi.cdc;

import javaapi.CreateCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcSinkBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.paimon.types.RowKind.INSERT;

public class WriteCdcToTable {

    public static void main(String[] args) throws Exception {
        writeTo();

    }

    public static void writeTo() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RichCdcRecord> dataStream =
                env.fromElements(
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "123")
                                .field("price", DataTypes.DOUBLE(), "62.2")
                                .build(),
                        // dt field will be added with schema evolution
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "245")
                                .field("price", DataTypes.DOUBLE(), "82.1")
                                .field("dt", DataTypes.TIMESTAMP(), "2023-06-12 20:21:12")
                                .build());

        Identifier identifier = Identifier.create("my_db", "T");

//        Catalog catalog = CreateCatalog.createFilesystemCatalog();
//        catalog.createDatabase("my_db", false);

        Options catalogOptions = new Options();
//        catalogOptions.set("warehouse", "/path/to/warehouse");
        catalogOptions.set("warehouse", "file:///Users/leicq/share_dir/my_test/");
        Catalog.Loader catalogLoader =  () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);

        new RichCdcSinkBuilder()
                .withInput(dataStream)
                .withTable(createTableIfNotExists(identifier))
                .withIdentifier(identifier)
                .withCatalogLoader(catalogLoader)
                .build();

        env.execute();
    }

    private static Table createTableIfNotExists(Identifier identifier) throws Exception {
        CatalogContext context = CatalogContext.create(new Path("file:///Users/leicq/share_dir/my_test"));
        Catalog catalog = CatalogFactory.createCatalog(context);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("order_id");
        schemaBuilder.column("order_id", DataTypes.BIGINT());
        schemaBuilder.column("price", DataTypes.DOUBLE());
        Schema schema = schemaBuilder.build();
        try {
            catalog.createTable(identifier, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
            System.out.printf(" create catalog " + e.toString());
        }
        return catalog.getTable(identifier);
    }
}