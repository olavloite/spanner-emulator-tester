package io.github.olavloite.spanner.emulator.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

/**
 * Test class for different data definition language scripts
 * 
 * @author loite
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DdlScriptTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(DdlScriptTest.class);

  @Test
  public void test1_SimpleDdlScript() {
    executeDdl(Arrays.asList(
        "create table test (id int64 not null, name string(100) not null) primary key (id)",
        "create index idx_test_name on test (name)"));
    // verify the existence of both the table and the index
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement
            .newBuilder("select * from information_schema.tables where table_name=@table_name")
            .bind("table_name").to("test").build())) {
      assertTrue(rs.next());
      assertEquals("test", rs.getString("TABLE_NAME"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement
            .newBuilder("select * from information_schema.indexes where index_name=@index_name")
            .bind("index_name").to("idx_test_name").build())) {
      assertTrue(rs.next());
      assertEquals("idx_test_name", rs.getString("INDEX_NAME"));
      assertEquals("test", rs.getString("TABLE_NAME"));
      assertFalse(rs.next());
    }
    // drop table and index
    executeDdl(Arrays.asList("drop index idx_test_name", "drop table test"));
  }

  @Test
  public void test2_InvalidDdlScript() {
    ErrorCode code = null;
    // DDL statements are in the wrong order
    try {
      executeDdl(Arrays.asList("create table foo (bar int64, baz date) primary key (bar)",
          "create index idx_test_name on test (name)",
          "create table test (id int64 not null, name string(100) not null) primary key (id)"));
    } catch (SpannerException e) {
      log.info(String.format("An expected exception occurred: %s", e.getMessage()), e);
      code = e.getErrorCode();
    }
    assertNotNull(code);
    // Check that the first table (bar) has also not been created, even though that statement was
    // valid. The entire script should be regarded as one transaction.
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement
            .newBuilder("select * from information_schema.tables where table_name=@table_name")
            .bind("table_name").to("bar").build())) {
      assertFalse(rs.next());
    }
  }

}
