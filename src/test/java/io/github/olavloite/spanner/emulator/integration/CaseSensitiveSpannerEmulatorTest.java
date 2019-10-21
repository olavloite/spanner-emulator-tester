package io.github.olavloite.spanner.emulator.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CaseSensitiveSpannerEmulatorTest extends AbstractSpannerEmulatorTest {

  @Test
  public void test1_CreateTable() {
    OperationFuture<Void, UpdateDatabaseDdlMetadata> operation =
        getDatabaseAdminClient().updateDatabaseDdl(getDatabaseId().getInstanceId().getInstance(),
            getDatabaseId().getDatabase(),
            Arrays.asList(
                "CREATE TABLE Number (Number INT64 NOT NULL, Name STRING(100) NOT NULL) PRIMARY KEY (Number)"),
            null);
    try {
      operation.get();
    } catch (InterruptedException | ExecutionException e) {
      throw SpannerExceptionFactory.newSpannerException(e);
    }
    assertTrue(operation.isDone());
  }

  @Test
  public void test2_InsertData() {
    DatabaseClient client = getDatabaseClient();
    TransactionRunner runner = client.readWriteTransaction();
    runner.run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(
            Mutation.newInsertBuilder("Number").set("NuMbEr").to(1L).set("NaMe").to("One").build());
        return null;
      }
    });
    assertNotNull(runner.getCommitTimestamp());
  }

  @Test
  public void test3_SelectData() {
    int count = 0;
    DatabaseClient client = getDatabaseClient();
    try (ResultSet rs = client.singleUse().executeQuery(Statement.of("select * from Number"))) {
      while (rs.next()) {
        count++;
        assertEquals(1L, rs.getLong(0));
        assertEquals(1L, rs.getLong("Number"));
        assertEquals("One", rs.getString(1));
        assertEquals("One", rs.getString("Name"));
      }
    }
    assertEquals(1, count);
  }

}
