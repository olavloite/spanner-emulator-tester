package io.github.olavloite.spanner.emulator.errorhandling;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class InvalidSQLTest extends AbstractSpannerTest {
  private static final long NUMBER_OF_ROWS = 10L;

  @BeforeClass
  public static void before() {
    createNumberTable();
    insertTestNumbers(NUMBER_OF_ROWS);
  }

  @Test
  public void testSingleUseExecuteWithInvalidSQL() {
    // First do an invalid query
    long count = 0;
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select * from num"))) {
      while (rs.next()) {
        count++;
      }
    } catch (SpannerException e) {
      // ignore
    }
    assertEquals(0L, count);
    // now do a valid query
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select * from number"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(NUMBER_OF_ROWS, count);
  }

  @Test
  public void testReadOnlyExecuteWithInvalidSQL() {
    ReadOnlyTransaction tx = getDatabaseClient().readOnlyTransaction();
    // First do an invalid query
    long count = 0;
    try (ResultSet rs = tx.executeQuery(Statement.of("select * from num"))) {
      while (rs.next()) {
        count++;
      }
    } catch (SpannerException e) {
      // ignore expected exception
    }
    assertEquals(0L, count);
    // now do a valid query using the same transaction
    try (ResultSet rs = tx.executeQuery(Statement.of("select * from number"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(NUMBER_OF_ROWS, count);
  }

  @Test
  public void testReadWriteExecuteWithInvalidSQL() {
    TransactionRunner tx = getDatabaseClient().readWriteTransaction();
    tx.run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        // First do an invalid query
        long count = 0;
        try (ResultSet rs = transaction.executeQuery(Statement.of("select * from num"))) {
          while (rs.next()) {
            count++;
          }
        } catch (SpannerException e) {
          // ignore expected exception
        }
        assertEquals(0L, count);
        // now do a valid query using the same transaction
        try (ResultSet rs = transaction.executeQuery(Statement.of("select * from number"))) {
          while (rs.next()) {
            count++;
          }
        }
        assertEquals(NUMBER_OF_ROWS, count);
        return null;
      }
    });
  }
}
