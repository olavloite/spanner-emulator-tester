package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;
import io.github.olavloite.spanner.emulator.interleaved.DeleteCascadeTest;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

/**
 * Test class for testing queries that force the use of a secondary index. These query hints are
 * ignored by the emulator, but the emulator does accept these kind of queries.
 * 
 * @author loite
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryWithForceIndexTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(DeleteCascadeTest.class);
  private static final long RECORD_COUNT = 100L;

  @BeforeClass
  public static void before() {
    log.info("Starting to create table with index");
    executeDdl(
        "CREATE TABLE person (person_id INT64 NOT NULL, FIRST_NAME STRING(100), LAST_NAME STRING(100) NOT NULL) PRIMARY KEY (PERSON_ID)");
    executeDdl("CREATE index idx_person_last_name on person (last_name)");
    log.info("Finished creating table with index");
  }

  @Test
  public void test1_InsertData() {
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        for (long id = 1; id <= RECORD_COUNT; id++) {
          transaction
              .buffer(Mutation.newInsertBuilder("person").set("person_id").to(id).set("first_name")
                  .to(new StringBuilder(EnglishNumberToWords.convert(id)).reverse().toString())
                  .set("last_name").to(EnglishNumberToWords.convert(id)).build());
        }
        return null;
      }
    });
    assertEquals(RECORD_COUNT, getRecordCount());
  }

  private long getRecordCount() {
    long res = 0L;
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select count(*) from person"))) {
      assertTrue(rs.next());
      res = rs.getLong(0);
      assertFalse(rs.next());
    }
    return res;
  }

  @Test
  public void test2_SelectWithForceIndex() {
    ReadOnlyTransaction tx = getDatabaseClient().readOnlyTransaction();
    long count = 0L;
    try (ResultSet rs = tx.executeQuery(Statement
        .of("select * from person@{FORCE_INDEX=idx_person_last_name} order by last_name"))) {
      String prevLastName = "";
      while (rs.next()) {
        assertTrue(prevLastName.compareTo(rs.getString("LAST_NAME")) <= 0);
        prevLastName = rs.getString("LAST_NAME");
        count++;
      }
    }
    assertEquals(getRecordCount(), count);
  }

  @Test
  public void test3_SelectWithForceIndex_InvalidIndexName() {
    ReadOnlyTransaction tx = getDatabaseClient().readOnlyTransaction();
    long count = 0L;
    // TODO: The emulator should throw an exception if an invalid index name is specified
    try (ResultSet rs = tx.executeQuery(
        Statement.of("select * from person@{FORCE_INDEX=non_existent_index} order by last_name"))) {
      String prevLastName = "";
      while (rs.next()) {
        assertTrue(prevLastName.compareTo(rs.getString("LAST_NAME")) <= 0);
        prevLastName = rs.getString("LAST_NAME");
        count++;
      }
    }
    assertEquals(getRecordCount(), count);
  }

}
