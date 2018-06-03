package io.github.olavloite.spanner.emulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.junit.Test;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

public class SimpleSpannerTest extends AbstractSpannerTest {
  private static final long NUMBER_OF_INSERT_ROWS = 20l;

  @Test
  public void testSpannerImpl() {
    testCreateTable();
    testInsert();
    testSelect();
    testExecuteSqlWithParams();
    testRead();
    testDeleteSingle();
    testDeleteKeyRanges();
    testUpdate();
    testUpdateMultiple();
  }

  private void testCreateTable() {
    Operation<Void, UpdateDatabaseDdlMetadata> operation =
        getDatabaseAdminClient().updateDatabaseDdl("test-instance", "test-database", Arrays.asList(
            "create table number (number int64 not null, name string(100) not null) primary key (number)"),
            null).waitFor();
    assertTrue(operation.isDone());
    assertTrue(operation.isSuccessful());
  }

  private void testInsert() {
    TransactionRunner runner = getDatabaseClient().readWriteTransaction();
    runner.run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        for (long counter = 1l; counter <= NUMBER_OF_INSERT_ROWS; counter++) {
          Mutation mutation = Mutation.newInsertBuilder("number").set("number").to(counter)
              .set("name").to("one").build();
          transaction.buffer(mutation);
        }
        return null;
      }
    });
  }

  private void testSelect() {
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from number order by number"))) {
      long count = 0;
      while (rs.next()) {
        count++;
        assertEquals(count, rs.getLong("number"));
        assertEquals("one", rs.getString("name"));
        assertEquals(count, rs.getLong(0));
        assertEquals("one", rs.getString(1));
      }
      assertEquals(NUMBER_OF_INSERT_ROWS, count);
    }
  }

  private void testDeleteSingle() {
    TransactionRunner runner = getDatabaseClient().readWriteTransaction();
    runner.run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.delete("number", Key.of(2L)));
        return null;
      }
    });
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select count(*) from number"))) {
      while (rs.next()) {
        assertEquals(NUMBER_OF_INSERT_ROWS - 1, rs.getLong(0));
      }
    }
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from number where number=2"))) {
      assertFalse(rs.next());
    }
  }

  private void testDeleteKeyRanges() {
    TransactionRunner runner = getDatabaseClient().readWriteTransaction();
    runner.run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.delete("number",
            KeySet.range(KeyRange.closedClosed(Key.of(10L), Key.of(14L)))));
        return null;
      }
    });
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select count(*) from number"))) {
      while (rs.next()) {
        assertEquals(NUMBER_OF_INSERT_ROWS - 6, rs.getLong(0));
      }
    }
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from number where number in (10,11,12,13,14)"))) {
      assertFalse(rs.next());
    }
  }

  private void testUpdate() {
    TransactionRunner runner = getDatabaseClient().readWriteTransaction();
    runner.run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        Mutation mutation =
            Mutation.newUpdateBuilder("number").set("number").to(3).set("name").to("three").build();
        transaction.buffer(mutation);
        return null;
      }
    });
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from number where number=3"))) {
      assertTrue(rs.next());
      assertEquals("three", rs.getString("name"));
    }
  }

  private void testUpdateMultiple() {
    TransactionRunner runner = getDatabaseClient().readWriteTransaction();
    runner.run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        for (long l = 4l; l <= 6l; l++) {
          transaction.buffer(Mutation.newUpdateBuilder("number").set("number").to(l).set("name")
              .to(EnglishNumberToWords.convert(l)).build());
        }
        return null;
      }
    });
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from number where number in (4,5,6)"))) {
      long current = 4l;
      while (rs.next()) {
        assertEquals(EnglishNumberToWords.convert(current), rs.getString("name"));
        current++;
      }
    }
  }

  private void testExecuteSqlWithParams() {
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement
            .newBuilder("select * from number where number>=@begin and number<@end order by number")
            .bind("begin").to(10).bind("end").to(15).build())) {
      long count = 0;
      while (rs.next()) {
        assertEquals(count + 10, rs.getLong("number"));
        assertEquals("one", rs.getString("name"));
        assertEquals(count + 10, rs.getLong(0));
        assertEquals("one", rs.getString(1));
        count++;
      }
      assertEquals(5, count);
    }
  }

  private void testRead() {
    Struct row = getDatabaseClient().singleUse().readRow("number", Key.of(1L),
        Arrays.asList("number", "name"));
    assertNotNull(row);
    assertEquals(1L, row.getLong("number"));
    assertEquals("one", row.getString("name"));
  }

}
