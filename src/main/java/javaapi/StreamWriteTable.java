package javaapi;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import java.util.List;

public class StreamWriteTable {

    public static void main(String[] args) throws Exception {
        // 1. Create a WriteBuilder (Serializable)
        Table table = GetTable.getTable();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();

        // 2. Write records in distributed tasks
        StreamTableWrite write = writeBuilder.newWrite();
        // commitIdentifier like Flink checkpointId
        long commitIdentifier = 0;

        while (true) {
            GenericRow record1 = GenericRow.of(BinaryString.fromString("Alice"), 12);
            GenericRow record2 = GenericRow.of(BinaryString.fromString("Bob"), 5);
            GenericRow record3 = GenericRow.of(BinaryString.fromString("Emily"), 18);
            write.write(record1);
            write.write(record2);
            write.write(record3);
            List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
            commitIdentifier++;

            // 3. Collect all CommitMessages to a global node and commit
            StreamTableCommit commit = writeBuilder.newCommit();
            commit.commit(commitIdentifier, messages);

            // 4. When failure occurs and you're not sure if the commit process is successful,
            //    you can use `filterAndCommit` to retry the commit process.
            //    Succeeded commits will be automatically skipped.
            /*
            Map<Long, List<CommitMessage>> commitIdentifiersAndMessages = new HashMap<>();
            commitIdentifiersAndMessages.put(commitIdentifier, messages);
            commit.filterAndCommit(commitIdentifiersAndMessages);
            */

            Thread.sleep(1000);
        }
    }
}