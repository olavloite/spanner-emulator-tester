package io.github.olavloite.spanner.emulator.interleaved;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ComplexInterleavedTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(ComplexInterleavedTest.class);

  @BeforeClass
  public static void createTables() {
    log.info("Creating tables");
    executeDdl(
        "create table parent1 (parent1_id int64 not null, col1 bool, col2 bytes(16), col3 float64, col4 int64, col5 string(100)) primary key (parent1_id)");
    executeDdl(
        "create table parent2 (parent1_id int64 not null, parent2_id string(10) not null, array1 array<bool>, array2 array<bytes(16)>, array3 array<float64>, array4 array<int64>, array5 array<string(100)>) primary key (parent1_id, parent2_id), interleave in parent parent1");
    executeDdl(
        "create table parent3 (parent1_id int64 not null, parent2_id string(10) not null, parent3_id bool, description string(100)) primary key (parent1_id, parent2_id, parent3_id), interleave in parent parent2");
    executeDdl(
        "create table child (parent1_id int64 not null, parent2_id string(10) not null, parent3_id bool, child_id int64, child_name string(100)) primary key (parent1_id, parent2_id, parent3_id, child_id), interleave in parent parent3");
    log.info("Finished creating tables");
  }

  @AfterClass
  public static void dropTables() {
    log.info("Dropping tables");
    executeDdl("drop table child");
    executeDdl("drop table parent3");
    executeDdl("drop table parent2");
    executeDdl("drop table parent1");
    log.info("Finished dropping tables");
  }

  @Test
  public void test1_InsertData() {
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.newInsertBuilder("parent1").set("parent1_id").to(1L).set("col1")
            .to(Boolean.TRUE).set("col2").to(ByteArray.copyFrom("TEST")).set("col3").to(49.90D)
            .set("col4").to(100L).set("col5").to("TEST").build());
        transaction.buffer(Mutation.newInsertBuilder("parent1").set("parent1_id").to(2L).set("col1")
            .to(Boolean.FALSE).set("col2").to(ByteArray.copyFrom("TEST2")).set("col3").to(49.90D)
            .set("col4").to(100L).set("col5").to("TEST2").build());
        transaction.buffer(Mutation.newInsertBuilder("parent1").set("parent1_id").to(3L).set("col1")
            .to((Boolean) null).set("col2").to((ByteArray) null).set("col3").to((Double) null)
            .set("col4").to((Long) null).set("col5").to((String) null).build());

        transaction.buffer(Mutation.newInsertBuilder("parent2").set("parent1_id").to(1L)
            .set("parent2_id").to("1").set("array1").toBoolArray(new boolean[] {true, true, false})
            .set("array2")
            .toBytesArray(Arrays.asList(ByteArray.copyFrom("TEST1"), ByteArray.copyFrom("TEST2")))
            .set("array3").toFloat64Array(new double[] {1.1D, 1.2D, 1.3D}).set("array4")
            .toInt64Array(new long[] {1L, 2L, 3L}).set("array5")
            .toStringArray(Arrays.asList("TEST1", "TEST2")).build());
        transaction.buffer(Mutation.newInsertBuilder("parent2").set("parent1_id").to(1L)
            .set("parent2_id").to("2").set("array1").toBoolArray(new boolean[] {true, true, false})
            .set("array2")
            .toBytesArray(Arrays.asList(ByteArray.copyFrom("TEST1"), ByteArray.copyFrom("TEST2")))
            .set("array3").toFloat64Array(new double[] {1.1D, 1.2D, 1.3D}).set("array4")
            .toInt64Array(new long[] {1L, 2L, 3L}).set("array5")
            .toStringArray(Arrays.asList("TEST1", "TEST2")).build());
        transaction.buffer(Mutation.newInsertBuilder("parent2").set("parent1_id").to(2L)
            .set("parent2_id").to("1").set("array1").toBoolArray((boolean[]) null).set("array2")
            .toBytesArray(null).set("array3").toFloat64Array((double[]) null).set("array4")
            .toInt64Array((long[]) null).set("array5").toStringArray(null).build());

        transaction
            .buffer(Mutation.newInsertBuilder("parent3").set("parent1_id").to(1L).set("parent2_id")
                .to("2").set("parent3_id").to(Boolean.TRUE).set("description").to("true").build());
        transaction.buffer(
            Mutation.newInsertBuilder("parent3").set("parent1_id").to(1L).set("parent2_id").to("2")
                .set("parent3_id").to(Boolean.FALSE).set("description").to("false").build());
        transaction.buffer(
            Mutation.newInsertBuilder("parent3").set("parent1_id").to(1L).set("parent2_id").to("2")
                .set("parent3_id").to((Boolean) null).set("description").to("null").build());

        transaction.buffer(Mutation.newInsertBuilder("child").set("parent1_id").to(1L)
            .set("parent2_id").to("2").set("parent3_id").to(Boolean.TRUE).set("child_id").to(1L)
            .set("child_name").to("1").build());
        transaction.buffer(Mutation.newInsertBuilder("child").set("parent1_id").to(1L)
            .set("parent2_id").to("2").set("parent3_id").to(Boolean.FALSE).set("child_id").to(1L)
            .set("child_name").to("1").build());
        transaction.buffer(Mutation.newInsertBuilder("child").set("parent1_id").to(1L)
            .set("parent2_id").to("2").set("parent3_id").to((Boolean) null).set("child_id").to(1L)
            .set("child_name").to("1").build());
        return null;
      }
    });
  }

  @Test
  public void test2_ReadData() {
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select * from parent1"))) {
      long counter = 1;
      int index = 0;
      boolean[] nullValues = new boolean[] {false, false, true};
      while (rs.next()) {
        assertEquals(counter, rs.getLong("parent1_id"));
        assertEquals(nullValues[index], rs.isNull("col1"));
        assertEquals(nullValues[index], rs.isNull("col2"));
        assertEquals(nullValues[index], rs.isNull("col3"));
        assertEquals(nullValues[index], rs.isNull("col4"));
        assertEquals(nullValues[index], rs.isNull("col5"));
        if (!nullValues[index]) {
          rs.getBoolean("col1");
          rs.getBytes("col2");
          rs.getDouble("col3");
          rs.getLong("col4");
          rs.getString("col5");
        }
        counter++;
        index++;
      }
    }
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select * from parent2"))) {
      long[] parent1_ids = new long[] {1L, 1L, 2L};
      String[] parent2_ids = new String[] {"1", "2", "1"};
      boolean[] nullValues = new boolean[] {false, false, true};
      int index = 0;
      while (rs.next()) {
        assertEquals(parent1_ids[index], rs.getLong("parent1_id"));
        assertEquals(parent2_ids[index], rs.getString("parent2_id"));
        assertEquals(nullValues[index], rs.isNull("array1"));
        assertEquals(nullValues[index], rs.isNull("array2"));
        assertEquals(nullValues[index], rs.isNull("array3"));
        assertEquals(nullValues[index], rs.isNull("array4"));
        assertEquals(nullValues[index], rs.isNull("array5"));
        if (!nullValues[index]) {
          rs.getBooleanArray("array1");
          rs.getBytesList("array2");
          rs.getDoubleArray("array3");
          rs.getLongArray("array4");
          rs.getStringList("array5");
        }
        index++;
      }
    }
  }

  @Test
  public void test3_CreateInterleavedIndex() {
    String indexName = "test_create_interleaved_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format(
        "create index %s on parent2 (parent1_id, array1), interleave in parent1", indexName));
    assertTrue(indexExists(indexName));
    assertEquals("parent1", getIndexParentTable(indexName));
    assertFalse(isIndexUnique(indexName));
    assertFalse(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "parent1_id"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "array1"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void test4_CreateInterleavedUniqueIndex() {
    String indexName = "test_create_interleaved_unique_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format(
        "create unique index %s on parent2 (parent1_id, parent2_id, array1), interleave in parent1",
        indexName));
    assertTrue(indexExists(indexName));
    assertEquals("parent1", getIndexParentTable(indexName));
    assertTrue(isIndexUnique(indexName));
    assertFalse(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "parent1_id"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "array1"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void test5_CreateInterleavedNullFilteredIndex() {
    String indexName = "test_create_interleaved_null_filtered_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format(
        "create null_filtered index %s on parent2 (parent1_id, array1), interleave in parent1",
        indexName));
    assertTrue(indexExists(indexName));
    assertEquals("parent1", getIndexParentTable(indexName));
    assertFalse(isIndexUnique(indexName));
    assertTrue(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "parent1_id"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "array1"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void test6_CreateInterleavedUniqueNullFilteredIndex() {
    String indexName = "test_create_interleaved_unique_null_filtered_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format(
        "create unique null_filtered index %s on parent2 (parent1_id, parent2_id, array1), interleave in parent1",
        indexName));
    assertTrue(indexExists(indexName));
    assertEquals("parent1", getIndexParentTable(indexName));
    assertTrue(isIndexUnique(indexName));
    assertTrue(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "parent1_id"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "array1"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

}
