package io.github.olavloite.spanner.emulator.interleaved;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

/**
 * Test class for interleaved tables with and without the ON DELETE CASCADE option
 * 
 * @author loite
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DeleteCascadeTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(DeleteCascadeTest.class);

  @BeforeClass
  public static void before() {
    log.info("Starting to create interleaved tables");
    executeDdl(
        "CREATE TABLE TREE (TREE_ID INT64 NOT NULL, NAME STRING(100) NOT NULL) PRIMARY KEY (TREE_ID)");
    executeDdl(
        "CREATE TABLE NODE_LEVEL1 (TREE_ID INT64 NOT NULL, NODE_LEVEL1_ID INT64 NOT NULL, NODE_LEVEL1_NAME STRING(100)) PRIMARY KEY (TREE_ID, NODE_LEVEL1_ID), INTERLEAVE IN PARENT TREE");
    executeDdl(
        "CREATE TABLE NODE_LEVEL2 (TREE_ID INT64 NOT NULL, NODE_LEVEL1_ID INT64 NOT NULL, NODE_LEVEL2_ID INT64 NOT NULL, NODE_LEVEL2_NAME STRING(100)) PRIMARY KEY (TREE_ID, NODE_LEVEL1_ID, NODE_LEVEL2_ID), INTERLEAVE IN PARENT NODE_LEVEL1 ON DELETE CASCADE");
    log.info("Finished creating interleaved tables");
  }

  @Test
  public void test1_InsertData() {
    List<Mutation> mutations = new ArrayList<>();
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        mutations.add(
            Mutation.newInsertBuilder("tree").set("tree_id").to(1L).set("name").to("1").build());
        mutations.add(
            Mutation.newInsertBuilder("tree").set("tree_id").to(2L).set("name").to("1").build());

        mutations.add(Mutation.newInsertBuilder("node_level1").set("tree_id").to(1L)
            .set("node_level1_id").to(1L).set("node_level1_name").to("1.1").build());
        mutations.add(Mutation.newInsertBuilder("node_level1").set("tree_id").to(1L)
            .set("node_level1_id").to(2L).set("node_level1_name").to("1.2").build());
        mutations.add(Mutation.newInsertBuilder("node_level1").set("tree_id").to(1L)
            .set("node_level1_id").to(3L).set("node_level1_name").to("1.3").build());
        mutations.add(Mutation.newInsertBuilder("node_level1").set("tree_id").to(2L)
            .set("node_level1_id").to(1L).set("node_level1_name").to("2.1").build());
        mutations.add(Mutation.newInsertBuilder("node_level1").set("tree_id").to(2L)
            .set("node_level1_id").to(2L).set("node_level1_name").to("2.2").build());
        mutations.add(Mutation.newInsertBuilder("node_level1").set("tree_id").to(2L)
            .set("node_level1_id").to(3L).set("node_level1_name").to("2.3").build());

        mutations.add(
            Mutation.newInsertBuilder("node_level2").set("tree_id").to(1L).set("node_level1_id")
                .to(1L).set("node_level2_id").to(1L).set("node_level2_name").to("1.1.1").build());
        mutations.add(
            Mutation.newInsertBuilder("node_level2").set("tree_id").to(1L).set("node_level1_id")
                .to(1L).set("node_level2_id").to(2L).set("node_level2_name").to("1.1.2").build());
        mutations.add(
            Mutation.newInsertBuilder("node_level2").set("tree_id").to(1L).set("node_level1_id")
                .to(1L).set("node_level2_id").to(3L).set("node_level2_name").to("1.1.3").build());
        mutations.add(
            Mutation.newInsertBuilder("node_level2").set("tree_id").to(1L).set("node_level1_id")
                .to(2L).set("node_level2_id").to(1L).set("node_level2_name").to("1.2.1").build());
        mutations.add(
            Mutation.newInsertBuilder("node_level2").set("tree_id").to(1L).set("node_level1_id")
                .to(2L).set("node_level2_id").to(2L).set("node_level2_name").to("1.2.2").build());
        mutations.add(
            Mutation.newInsertBuilder("node_level2").set("tree_id").to(1L).set("node_level1_id")
                .to(2L).set("node_level2_id").to(3L).set("node_level2_name").to("1.2.3").build());
        mutations.add(
            Mutation.newInsertBuilder("node_level2").set("tree_id").to(2L).set("node_level1_id")
                .to(1L).set("node_level2_id").to(1L).set("node_level2_name").to("2.1.1").build());
        transaction.buffer(mutations);
        return null;
      }
    });
    // Check that the rows were successfully inserted
    assertEquals(Long.valueOf(mutations.size()), getRecordCount());
  }

  private Long getRecordCount() {
    Long res = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select count(*) from (select tree_id from tree "
            + "union all select tree_id from node_level1 "
            + "union all select tree_id from node_level2) all_nodes"))) {
      assertTrue(rs.next());
      res = rs.getLong(0);
      assertFalse(rs.next());
    }
    return res;
  }

  @Test
  public void test2_DeleteWithCascade() {
    // Try to delete a level1-node. That should work as level2 is defined as ON DELETE CASCADE
    // First get the current count
    long initialRecordCount = getRecordCount();
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.delete("node_level1", Key.of(2L, 1L)));
        return null;
      }
    });
    long recordCountAfterDelete = getRecordCount();
    // It should now be two less: "2.1" and "2.1.1" should now be deleted
    assertEquals(initialRecordCount - 2, recordCountAfterDelete);
  }

  @Test
  public void test3_DeleteWithCascadeMultiple() {
    // Try to delete a level1-node. That should work as level2 is defined as ON DELETE CASCADE
    // First get the current count
    long initialRecordCount = getRecordCount();
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.delete("node_level1", Key.of(1L, 1L)));
        return null;
      }
    });
    long recordCountAfterDelete = getRecordCount();
    // It should now be two less: "1.1" and all its childen (1.1.1, 1.1.2, 1.1.3) should now be
    // deleted
    assertEquals(initialRecordCount - 4, recordCountAfterDelete);
  }

  @Test
  public void test4_DeleteWithoutCascade() {
    ErrorCode code = null;
    // Try to delete a tree-node. That should not work as level1 is NOT defined as ON DELETE CASCADE
    // First get the current count
    long initialRecordCount = getRecordCount();
    try {
      getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
        @Override
        public Void run(TransactionContext transaction) throws Exception {
          transaction.buffer(Mutation.delete("tree", Key.of(1L)));
          return null;
        }
      });
    } catch (SpannerException e) {
      log.error(String.format("An expected error occurred: %s", e.getMessage()), e);
      code = e.getErrorCode();
    }
    long recordCountAfterDelete = getRecordCount();
    // It should not have changed
    assertEquals(initialRecordCount, recordCountAfterDelete);
    // Check the error code
    assertNotNull(code);
  }

}
