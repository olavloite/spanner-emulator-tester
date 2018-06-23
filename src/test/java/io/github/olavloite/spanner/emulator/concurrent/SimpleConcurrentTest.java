package io.github.olavloite.spanner.emulator.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleConcurrentTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(SimpleConcurrentTest.class);
  private static final long INITIAL_NUMBER_OF_ROWS = 100L;

  @BeforeClass
  public static void before() {
    createNumberTable();
    insertTestNumbers(INITIAL_NUMBER_OF_ROWS);
  }

  private static abstract class ReadCallable implements Callable<Void> {
    private static final int NUMBER_OF_READS = 200;
    protected final DatabaseClient client;
    private final Random rnd = new Random();

    private ReadCallable(DatabaseClient client) {
      this.client = client;
    }

    protected abstract ReadContext getReadContext();

    protected void closeReadContext() {}

    protected int getMax() {
      try (ResultSet rs =
          getReadContext().executeQuery(Statement.of("select max(number.number) from number"))) {
        if (rs.next()) {
          return (int) rs.getLong(0);
        }
      }
      throw new AssertionError("Getting max number failed");
    }

    @Override
    public Void call() throws Exception {
      log.debug("Starting");
      int max = getMax();
      log.debug("Fetched max");
      assertEquals(100, max);
      String sql = "select * from number where number.number>=@p1 and number.number<=@p2";
      for (int i = 0; i < NUMBER_OF_READS; i++) {
        int p1 = rnd.nextInt(max) + 1;
        int p2 = rnd.nextInt(max) + 1;
        Statement statement = Statement.newBuilder(sql).bind("p1").to(Math.min(p1, p2)).bind("p2")
            .to(Math.max(p1, p2)).build();
        int current = Math.min(p1, p2);
        int count = Math.max(p1, p2) - current + 1;
        int actualCount = 0;
        try (ResultSet rs = getReadContext().executeQuery(statement)) {
          while (rs.next()) {
            assertEquals(current, rs.getLong("number"));
            current++;
            actualCount++;
          }
        }
        assertEquals(count, actualCount);
      }
      closeReadContext();
      log.debug("Finished");
      return null;
    }
  }

  private static class SingleUseReadCallable extends ReadCallable {
    private SingleUseReadCallable(DatabaseClient client) {
      super(client);
    }

    @Override
    protected ReadContext getReadContext() {
      return client.singleUse();
    }
  }

  private static class TransactionReadCallable extends ReadCallable {
    private ReadOnlyTransaction transaction;

    private TransactionReadCallable(DatabaseClient client) {
      super(client);
    }

    @Override
    protected ReadContext getReadContext() {
      if (transaction == null) {
        transaction = client.readOnlyTransaction();
      }
      return transaction;
    }

    @Override
    protected void closeReadContext() {
      if (transaction != null) {
        transaction.close();
      }
    }
  }

  @Test
  public void test1_ConcurrentReadSingleUse() throws InterruptedException, ExecutionException {
    log.info("Starting concurrent-read-single-use test");
    testConcurrentRead(SingleUseReadCallable::new);
    log.info("Finished concurrent-read-single-use test");
  }

  @Test
  public void test2_ConcurrentReadTransaction() throws InterruptedException, ExecutionException {
    log.info("Starting concurrent-read-transaction test");
    testConcurrentRead(TransactionReadCallable::new);
    log.info("Finished concurrent-read-transaction test");
  }

  private void testConcurrentRead(Function<DatabaseClient, ReadCallable> callableConstructor)
      throws InterruptedException, ExecutionException {
    final DatabaseClient client = getDatabaseClient();
    int numberOfConcurrentThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentThreads);
    List<Future<Void>> res = new ArrayList<>(numberOfConcurrentThreads);
    for (int i = 0; i < numberOfConcurrentThreads; i++) {
      res.add(executor.submit(callableConstructor.apply(client)));
    }
    executor.shutdown();
    executor.awaitTermination(1L, TimeUnit.MINUTES);
    for (Future<Void> future : res) {
      assertNull(future.get());
    }
  }

  private final class WriteCallable implements Callable<Void> {
    private final long from;
    private final long noOfRows;

    private WriteCallable(long from, long noOfRows) {
      this.from = from;
      this.noOfRows = noOfRows;
    }

    @Override
    public Void call() throws Exception {
      SimpleConcurrentTest.insertTestNumbers(from, noOfRows);
      return null;
    }
  }

  @Test
  public void test3_ConcurrentWrite() throws InterruptedException, ExecutionException {
    log.info("Starting concurrent-write test");
    int numberOfConcurrentThreads = 10;
    long noOfRows = 100L;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentThreads);
    List<Future<Void>> res = new ArrayList<>(numberOfConcurrentThreads);
    for (int i = 0; i < numberOfConcurrentThreads; i++) {
      long from = INITIAL_NUMBER_OF_ROWS + (i * noOfRows) + 1;
      res.add(executor.submit(new WriteCallable(from, noOfRows)));
    }
    executor.shutdown();
    executor.awaitTermination(1L, TimeUnit.MINUTES);
    for (Future<Void> future : res) {
      assertNull(future.get());
    }
    // Check the total number of rows in the table
    long count = 0L;
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select count(*) from number"))) {
      if (rs.next()) {
        count = rs.getLong(0);
      }
    }
    assertEquals(INITIAL_NUMBER_OF_ROWS + noOfRows * numberOfConcurrentThreads, count);
    log.info("Finished concurrent-write test");
  }

}
