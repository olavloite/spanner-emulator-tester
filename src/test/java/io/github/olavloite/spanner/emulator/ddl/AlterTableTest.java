package io.github.olavloite.spanner.emulator.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class AlterTableTest extends AbstractSpannerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() {
    executeDdl(
        "create table test (id int64, col1 bool, col2 bytes(16), col3 float64, col4 int64, col5 string(100), col6 date, col7 timestamp) primary key (id)");
  }

  @After
  public void after() {
    executeDdl("drop table test");
  }

  @Test
  public void testMakeEmptyColsNotNullAndThenNullableAgain() {
    // change all columns into not null columns
    assertTrue(isNullable("col1"));
    executeDdl("alter table test alter column col1 bool not null");
    assertFalse(isNullable("col1"));

    assertTrue(isNullable("col2"));
    executeDdl("alter table test alter column col2 bytes(16) not null");
    assertFalse(isNullable("col2"));

    assertTrue(isNullable("col3"));
    executeDdl("alter table test alter column col3 float64 not null");
    assertFalse(isNullable("col3"));

    assertTrue(isNullable("col4"));
    executeDdl("alter table test alter column col4 int64 not null");
    assertFalse(isNullable("col4"));

    assertTrue(isNullable("col5"));
    executeDdl("alter table test alter column col5 string(100) not null");
    assertFalse(isNullable("col5"));

    assertTrue(isNullable("col6"));
    executeDdl("alter table test alter column col6 date not null");
    assertFalse(isNullable("col6"));

    assertTrue(isNullable("col7"));
    executeDdl("alter table test alter column col7 timestamp not null");
    assertFalse(isNullable("col7"));

    // and now change them back into nullable columns
    assertFalse(isNullable("col1"));
    executeDdl("alter table test alter column col1 bool");
    assertTrue(isNullable("col1"));

    assertFalse(isNullable("col2"));
    executeDdl("alter table test alter column col2 bytes(16)");
    assertTrue(isNullable("col2"));

    assertFalse(isNullable("col3"));
    executeDdl("alter table test alter column col3 float64");
    assertTrue(isNullable("col3"));

    assertFalse(isNullable("col4"));
    executeDdl("alter table test alter column col4 int64");
    assertTrue(isNullable("col4"));

    assertFalse(isNullable("col5"));
    executeDdl("alter table test alter column col5 string(100)");
    assertTrue(isNullable("col5"));

    assertFalse(isNullable("col6"));
    executeDdl("alter table test alter column col6 date");
    assertTrue(isNullable("col6"));

    assertFalse(isNullable("col7"));
    executeDdl("alter table test alter column col7 timestamp");
    assertTrue(isNullable("col7"));
  }

  @Test
  public void testChangeDataTypeFromStringToBytes() {
    assertEquals("STRING(100)", getDataType("col5"));
    executeDdl("alter table test alter column col5 BYTES(200)");
    assertEquals("BYTES(200)", getDataType("col5"));
  }

  @Test
  public void testChangeDataTypeFromBytesToString() {
    assertEquals("BYTES(16)", getDataType("col2"));
    executeDdl("alter table test alter column col2 STRING(16)");
    assertEquals("STRING(16)", getDataType("col2"));
  }

  @Test
  public void testReduceStringLength() {
    assertEquals("STRING(100)", getDataType("col5"));
    executeDdl("alter table test alter column col5 STRING(50)");
    assertEquals("STRING(50)", getDataType("col5"));
  }

  @Test
  public void testReduceBytesLength() {
    assertEquals("BYTES(16)", getDataType("col2"));
    executeDdl("alter table test alter column col2 BYTES(8)");
    assertEquals("BYTES(8)", getDataType("col2"));
  }

  @Test
  public void testIncreaseStringLength() {
    assertEquals("STRING(100)", getDataType("col5"));
    executeDdl("alter table test alter column col5 STRING(200)");
    assertEquals("STRING(200)", getDataType("col5"));
  }

  @Test
  public void testIncreaseBytesLength() {
    assertEquals("BYTES(16)", getDataType("col2"));
    executeDdl("alter table test alter column col2 BYTES(32)");
    assertEquals("BYTES(32)", getDataType("col2"));
  }

  @Test
  public void testMakeNonEmptyColsNotNull() {
    // insert a row with mostly null values
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transaction.buffer(Mutation.newInsertBuilder("test").set("id").to(1L).set("col1")
            .to(Boolean.TRUE).build());
        return null;
      }
    });
    // try to make one of the columns not null
    thrown.expect(SpannerException.class);
    executeDdl("alter table test alter column col3 float64 not null");
  }

  private boolean isNullable(String col) {
    Statement statement = Statement.newBuilder(
        "select IS_NULLABLE from information_schema.columns where table_name=@table_name and column_name=@column_name")
        .bind("table_name").to("test").bind("column_name").to(col).build();
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(statement)) {
      if (rs.next()) {
        return rs.getString("IS_NULLABLE").equals("YES");
      } else {
        throw new IllegalArgumentException(String.format("Unknown column name %s", col));
      }
    }
  }

  private String getDataType(String col) {
    Statement statement = Statement.newBuilder(
        "select SPANNER_TYPE from information_schema.columns where table_name=@table_name and column_name=@column_name")
        .bind("table_name").to("test").bind("column_name").to(col).build();
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(statement)) {
      if (rs.next()) {
        return rs.getString("SPANNER_TYPE");
      } else {
        throw new IllegalArgumentException(String.format("Unknown column name %s", col));
      }
    }
  }

}
