package io.github.olavloite.spanner.emulator.sql;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class SqlTest extends AbstractSpannerTest {

  @BeforeClass
  public static void beforeClass() {
    createNumberTable();
    insertTestNumbers(1000L);
  }

  @Test
  public void testSimpleSelect() {
    long count = 0L;
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select * from number"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(1000L, count);
  }

  @Test
  public void testSimpleSubSelect() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select * from (select * from number order by number desc) limit 10 offset 100"))) {
      while (rs.next()) {
        assertEquals(1000L - 100L - count, rs.getLong("number"));
        count++;
      }
    }
    assertEquals(10L, count);
  }

  @Test
  public void testDoubleSubSelect() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from (select * from (select * from number where number.number>=200) order by number desc) limit 10 offset 100"))) {
      while (rs.next()) {
        assertEquals(1000L - 100L - count, rs.getLong("number"));
        count++;
      }
    }
    assertEquals(10L, count);
  }

  @Test
  public void testSubSelectJoins() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
    // @formatter:off
        "select * from (select * from number order by number.number desc) n1\n"
        + "inner join  (select * from number order by number.number asc ) n2 on n2.number=n1.number\n"
        + "limit 10 offset 100"))) {
    // @formatter:on
      while (rs.next()) {
        assertEquals(1000L - 100L - count, rs.getLong(0));
        count++;
      }
    }
    assertEquals(10L, count);
  }

}
