package io.github.olavloite.spanner.emulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import java.util.Arrays;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

public class PartitionSpannerTest extends AbstractSpannerTest {
  private static final long NUMBER_OF_ROWS = 200;

  @BeforeClass
  public static void createTestData() {
    createNumberTable();
    insertTestNumbers(NUMBER_OF_ROWS);
  }

  @Test
  public void testPartitionQuery() {
    BatchClient client = getBatchClient();
    try (BatchReadOnlyTransaction tx = client.batchReadOnlyTransaction(TimestampBound.strong())) {
      List<Partition> partitions =
          tx.partitionQuery(PartitionOptions.newBuilder().setMaxPartitions(5L).build(),
              Statement.of("select * from number"));
      assertFalse(partitions.isEmpty());
      long count = 0;
      for (Partition partition : partitions) {
        try (ResultSet rs = tx.execute(partition)) {
          while (rs.next()) {
            assertEquals(rs.getString("name"), EnglishNumberToWords.convert(rs.getLong("number")));
            count++;
          }
        }
      }
      assertEquals(NUMBER_OF_ROWS, count);
    }
  }

  @Test
  public void testPartitionRead() {
    BatchClient client = getBatchClient();
    try (BatchReadOnlyTransaction tx = client.batchReadOnlyTransaction(TimestampBound.strong())) {
      List<Partition> partitions =
          tx.partitionRead(PartitionOptions.newBuilder().setMaxPartitions(5L).build(), "number",
              KeySet.all(), Arrays.asList("number", "name"));
      assertFalse(partitions.isEmpty());
      long count = 0;
      for (Partition partition : partitions) {
        try (ResultSet rs = tx.execute(partition)) {
          while (rs.next()) {
            assertEquals(rs.getString("name"), EnglishNumberToWords.convert(rs.getLong("number")));
            count++;
          }
        }
      }
      assertEquals(NUMBER_OF_ROWS, count);
    }
  }

}
