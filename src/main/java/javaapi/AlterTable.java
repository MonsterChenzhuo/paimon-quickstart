package javaapi;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AlterTable {

    public static void main(String[] args) throws Catalog.DatabaseAlreadyExistException {
        Identifier identifier = Identifier.create("my_db", "my_table");

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("compaction.max.file-num", "40");

        Catalog catalog = CreateCatalog.createFilesystemCatalog();
//        // 建库
        catalog.createDatabase("my_db", false);

        try {
            catalog.createTable(
                    identifier,
                    new Schema(
                            Lists.newArrayList(
                                    new DataField(0, "col1", DataTypes.STRING(), "field1"),
                                    new DataField(1, "col2", DataTypes.STRING(), "field2"),
                                    new DataField(2, "col3", DataTypes.STRING(), "field3"),
                                    new DataField(3, "col4", DataTypes.BIGINT(), "field4"),
                                    new DataField(
                                            4,
                                            "col5",
                                            DataTypes.ROW(
                                                    new DataField(
                                                            5, "f1", DataTypes.STRING(), "f1"),
                                                    new DataField(
                                                            6, "f2", DataTypes.STRING(), "f2"),
                                                    new DataField(
                                                            7, "f3", DataTypes.STRING(), "f3")),
                                            "field5"),
                                    new DataField(8, "col6", DataTypes.STRING(), "field6")),
                            Lists.newArrayList("col1"), // partition keys
                            Lists.newArrayList("col1", "col2"), // primary key
                            options,
                            "table comment"),
                    false);
        } catch (Catalog.TableAlreadyExistException e) {
            System.out.printf("TableAlreadyExistException: " + e.getMessage() + "\n");
        } catch (Catalog.DatabaseNotExistException e) {
            System.out.printf("DatabaseNotExistException: " + e.getMessage()+ "\n");
        }

        System.out.printf("create tables successfully\n");

        //显示表
//        System.out.printf("table name: " + catalog.getTable(identifier).name() + "is already exists\n");

        // add option
        SchemaChange addOption = SchemaChange.setOption("snapshot.time-retained", "2h");
        // remove option
        SchemaChange removeOption = SchemaChange.removeOption("compaction.max.file-num");
        // add column
        SchemaChange addColumn = SchemaChange.addColumn("col1_after", DataTypes.STRING());
        // add a column after col1
        SchemaChange.Move after = SchemaChange.Move.after("col1_after", "col1");
        SchemaChange addColumnAfterField =
                SchemaChange.addColumn("col7", DataTypes.STRING(), "", after);
        // rename column
        SchemaChange renameColumn = SchemaChange.renameColumn("col3", "col3_new_name");
        // drop column
        SchemaChange dropColumn = SchemaChange.dropColumn("col6");
        // update column comment
        SchemaChange updateColumnComment =
                SchemaChange.updateColumnComment(new String[] {"col4"}, "col4 field");
        // update nested column comment
        SchemaChange updateNestedColumnComment =
                SchemaChange.updateColumnComment(new String[] {"col5", "f1"}, "col5 f1 field");
        // update column type
        SchemaChange updateColumnType = SchemaChange.updateColumnType("col4", DataTypes.DOUBLE());
        // update column position, you need to pass in a parameter of type Move
        SchemaChange updateColumnPosition =
                SchemaChange.updateColumnPosition(SchemaChange.Move.first("col4"));
        // update column nullability
        SchemaChange updateColumnNullability =
                SchemaChange.updateColumnNullability(new String[] {"col4"}, false);
        // update nested column nullability
        SchemaChange updateNestedColumnNullability =
                SchemaChange.updateColumnNullability(new String[] {"col5", "f2"}, false);

        SchemaChange[] schemaChanges =
                new SchemaChange[] {
                        addOption,
                        removeOption,
                        addColumn,
                        addColumnAfterField,
                        renameColumn,
                        dropColumn,
                        updateColumnComment,
                        updateNestedColumnComment,
                        updateColumnType,
                        updateColumnPosition,
                        updateColumnNullability,
                        updateNestedColumnNullability
                };
        try {
            catalog.alterTable(identifier, Arrays.asList(schemaChanges), false);
        } catch (Catalog.TableNotExistException e) {
            System.out.printf("TableNotExistException: " + e.getMessage());
        } catch (Catalog.ColumnAlreadyExistException e) {
            System.out.printf("ColumnAlreadyExistException: " + e.getMessage());
        } catch (Catalog.ColumnNotExistException e) {
            System.out.printf("ColumnNotExistException: " + e.getMessage());
        }
    }
}