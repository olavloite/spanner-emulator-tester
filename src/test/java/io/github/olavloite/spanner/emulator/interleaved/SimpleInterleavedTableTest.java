package io.github.olavloite.spanner.emulator.interleaved;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleInterleavedTableTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(SimpleInterleavedTableTest.class);

  @BeforeClass
  public static void createTables() {
    log.info("Creating tables");
    executeDdl(
        "create table test_parent (parent_id int64 not null, name string(100)) primary key (parent_id)");
    executeDdl(
        "create table test_child (parent_id int64 not null, child_id int64 not null, child_name string(100)) primary key (parent_id, child_id), interleave in parent test_parent");
    log.info("Finished creating tables");
  }

  @AfterClass
  public static void dropTables() {
    log.info("Dropping tables");
    executeDdl("drop table test_child");
    executeDdl("drop table test_parent");
    log.info("Finished dropping tables");
  }

  @Test
  public void test1_InsertData() {
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.newInsertBuilder("test_parent").set("parent_id").to(1L)
            .set("name").to("parent one").build());
        transaction.buffer(Mutation.newInsertBuilder("test_parent").set("parent_id").to(2L)
            .set("name").to("parent two").build());

        transaction.buffer(Mutation.newInsertBuilder("test_child").set("parent_id").to(1L)
            .set("child_id").to(1L).set("child_name").to("child 1-1").build());
        transaction.buffer(Mutation.newInsertBuilder("test_child").set("parent_id").to(1L)
            .set("child_id").to(2L).set("child_name").to("child 1-2").build());
        transaction.buffer(Mutation.newInsertBuilder("test_child").set("parent_id").to(2L)
            .set("child_id").to(1L).set("child_name").to("child 2-2").build());
        transaction.buffer(Mutation.newInsertBuilder("test_child").set("parent_id").to(2L)
            .set("child_id").to(2L).set("child_name").to("child 2-2").build());
        return null;
      }
    });
    // Check the inserted data
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select count(*) from test_parent"))) {
      assertTrue(rs.next());
      assertEquals(2L, rs.getLong(0));
    }
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select count(*) from test_child"))) {
      assertTrue(rs.next());
      assertEquals(4L, rs.getLong(0));
    }
  }

  public void test2_ReadData() {
    // Check that the correct data is returned in the correct order (primary key order)
    long counter = 1;
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select * from test_parent"))) {
      while (rs.next()) {
        assertEquals(counter, rs.getLong(0));
        counter++;
      }
    }
    long counter1 = 1;
    long counter2 = 1;
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select * from test_child"))) {
      while (rs.next()) {
        assertEquals(counter1, rs.getLong(0));
        assertEquals(counter2, rs.getLong(1));
        if (counter2 % 2 == 0)
          counter1++;
        else
          counter2++;
      }
    }
  }

}
