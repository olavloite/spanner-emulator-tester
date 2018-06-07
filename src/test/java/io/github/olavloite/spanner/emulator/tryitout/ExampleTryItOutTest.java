package io.github.olavloite.spanner.emulator.tryitout;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

/**
 * Example test class that can be used as a template if you want to submit a pull request with a new
 * test case. Pull requests are automatically built on Travis and the test cases are ran against an
 * emulator instance running on a small cloud machine. This way, you are able to see whether your
 * test case will run on the emulator.
 * 
 * Note the @FixMethodOrder(MethodSorters.NAME_ASCENDING). This fixates the order in which the test
 * cases are run, which means that you can create a test class that follows this pattern:
 * <p>
 * <ul>
 * <li>1. Create test tables</li>
 * <li>2. Insert test data</li>
 * <li>3. Do test selects and other tests on the test data</li>
 * <li>4. Drop test tables</li>
 * </ul>
 * 
 * @author loite
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExampleTryItOutTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(ExampleTryItOutTest.class);
  private static final String MY_TEST_TABLE = "my_test_table";

  /**
   * Create a test table that will be used for this entire class
   */
  @BeforeClass
  public static void createTables() {
    log.info("Creating test tables");
    executeDdl(String.format(
        "create table %s (id int64 not null, name string(100) not null) primary key (id)",
        MY_TEST_TABLE));
    log.info("Finished creating test tables");
  }


  @AfterClass
  public static void dropTables() {
    log.info("Cleaning up test tables");
    executeDdl(String.format("drop table %s", MY_TEST_TABLE));
    log.info("Finished cleaning up test tables");
  }

  @Test
  public void test1_InsertData() {
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.newInsertBuilder(MY_TEST_TABLE).set("id").to(1L).set("name")
            .to("one").build());
        transaction.buffer(Mutation.newInsertBuilder(MY_TEST_TABLE).set("id").to(2L).set("name")
            .to("two").build());
        transaction.buffer(Mutation.newInsertBuilder(MY_TEST_TABLE).set("id").to(3L).set("name")
            .to("three").build());
        return null;
      }
    });
    // Assert that the records where written successfully
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of(String.format("select count(*) from %s", MY_TEST_TABLE)))) {
      rs.next();
      assertEquals(3L, rs.getLong(0));
    }
  }

  @Test
  public void test2_ReadData() {
    long count = 0;
    try (ResultSet rs = getDatabaseClient().singleUse().read(MY_TEST_TABLE, KeySet.all(),
        Arrays.asList("id", "name"))) {
      while (rs.next()) {
        count++;
        assertEquals(count, rs.getLong("id"));
        assertEquals(EnglishNumberToWords.convert(count), rs.getString("name"));
      }
    }
    assertEquals(3L, count);
  }

  @Test
  public void test3_SelectDataUpperCase() {
    long count = 0;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of(String.format("SELECT ID, NAME FROM %s", MY_TEST_TABLE.toUpperCase())))) {
      while (rs.next()) {
        count++;
        assertEquals(count, rs.getLong("id"));
        assertEquals(EnglishNumberToWords.convert(count), rs.getString("name"));
      }
    }
    assertEquals(3L, count);
  }

  @Test
  public void test4_ReadDataUpperCase() {
    long count = 0;
    try (ResultSet rs = getDatabaseClient().singleUse().read(MY_TEST_TABLE.toUpperCase(),
        KeySet.all(), Arrays.asList("ID", "NAME"))) {
      while (rs.next()) {
        count++;
        assertEquals(count, rs.getLong("id"));
        assertEquals(EnglishNumberToWords.convert(count), rs.getString("name"));
      }
    }
    assertEquals(3L, count);
  }

}
