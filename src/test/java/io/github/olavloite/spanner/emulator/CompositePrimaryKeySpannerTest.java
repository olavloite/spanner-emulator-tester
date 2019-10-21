package io.github.olavloite.spanner.emulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

public class CompositePrimaryKeySpannerTest extends AbstractSpannerTest {
  private static final int NUMBER_OF_TEST_ROWS = 100;

  @Test
  public void testCompositePrimaryKey() {
    createTestTable();
    insertTestData();
    testDeleteOneRecord();
    testDeleteMultipleRecords();
  }

  private void createTestTable() {
    String sql =
        "CREATE TABLE number (number int64 not null, name string(100) not null, description string(max)) primary key (number, name)";
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op = getDatabaseAdminClient()
        .updateDatabaseDdl(INSTANCE_ID, DATABASE_ID, Arrays.asList(sql), null);
    try {
      op.get();
    } catch (InterruptedException | ExecutionException e) {
      throw SpannerExceptionFactory.newSpannerException(e);
    }
    assertTrue(op.isDone());
  }

  private void insertTestData() {
    DatabaseClient client = getDatabaseClient();
    client.readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        for (int number = 1; number <= NUMBER_OF_TEST_ROWS; number++) {
          transaction.buffer(Mutation.newInsertBuilder("number").set("number").to(number)
              .set("name").to(EnglishNumberToWords.convert(number)).set("description")
              .to(number % 2 == 0 ? EnglishNumberToWords.convert(number) : null).build());
        }
        return null;
      }
    });
    try (ResultSet rs =
        client.singleUse().executeQuery(Statement.of("select count(*) from number"))) {
      assertTrue(rs.next());
      assertEquals(NUMBER_OF_TEST_ROWS, rs.getLong(0));
    }
  }

  private void testDeleteOneRecord() {
    DatabaseClient client = getDatabaseClient();
    client.readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.delete("number", Key.of(10L, "ten")));
        return null;
      }
    });
    try (ResultSet rs =
        client.singleUse().executeQuery(Statement.of("select count(*) from number"))) {
      assertTrue(rs.next());
      assertEquals(NUMBER_OF_TEST_ROWS - 1, rs.getLong(0));
    }
    try (ResultSet rs = client.singleUse()
        .executeQuery(Statement.of("select * from number where number.number=10"))) {
      assertFalse(rs.next());
    }
  }

  private void testDeleteMultipleRecords() {
    DatabaseClient client = getDatabaseClient();
    boolean expectedException = false;
    try {
      client.readWriteTransaction().run(new TransactionCallable<Void>() {
        @Override
        public Void run(TransactionContext transaction) throws Exception {
          transaction.buffer(Mutation.delete("number",
              KeySet.range(KeyRange.closedOpen(Key.of(50L, "fifty"), Key.of(60L)))));
          return null;
        }
      });
    } catch (SpannerException e) {
      if (e.getErrorCode() == ErrorCode.UNIMPLEMENTED) {
        // This is expected, Cloud Spanner actually does not implement the possibility to delete a
        // range where the start and end part of the range differ in anything but the last part
        expectedException = true;
      }
    }
    assertTrue(expectedException);

    // Now do a similar delete, but only specify the first part of the key
    client.readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(
            Mutation.delete("number", KeySet.range(KeyRange.closedOpen(Key.of(50L), Key.of(60L)))));
        return null;
      }
    });
    try (ResultSet rs =
        client.singleUse().executeQuery(Statement.of("select count(*) from number"))) {
      assertTrue(rs.next());
      assertEquals(NUMBER_OF_TEST_ROWS - 11, rs.getLong(0));
    }
    try (ResultSet rs = client.singleUse().executeQuery(
        Statement.of("select * from number where number.number>=50 and number.number<60"))) {
      assertFalse(rs.next());
    }
    try (ResultSet rs = client.singleUse()
        .executeQuery(Statement.of("select * from number where number.number=60"))) {
      assertTrue(rs.next());
      assertFalse(rs.next());
    }
  }

}
