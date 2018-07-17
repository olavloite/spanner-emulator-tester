package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.sql.SQLException;
import java.util.function.BiFunction;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

/**
 * Test class for testing the sql functions supported by Google Cloud Spanner
 * 
 * @author loite
 *
 */
public class FunctionsTest extends AbstractSpannerTest {
  private static final long NUMBER_OF_ROWS = 3000L;

  @BeforeClass
  public static void before() {
    createNumberTable();
    insertTestNumbers(NUMBER_OF_ROWS);
  }

  @Test
  public void castBoolToString() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast(number.number=1 as string) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals("true", rs.getString("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast(number.number=2 as string) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals("false", rs.getString("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castIntToFloat64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast(number.number as float64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(1.0D, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
  }

  @Test
  public void castFloatToInt64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select cast((number.number/2.0) as int64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(1L, rs.getLong("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castStringToBool() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('true' as bool) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.TRUE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('FALSE' as bool) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.FALSE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
    boolean exception = false;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast('invalid value' as bool) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.FALSE, rs.getBoolean("test"));
      assertFalse(rs.next());
    } catch (SpannerException e) {
      // expected exception
      exception = true;
      assertTrue(e.getMessage().contains("invalid value"));
    }
    assertTrue(exception);
  }

  @Test
  public void castInt64ToBool() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast(number.number as bool) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.TRUE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast((number.number-1) as bool) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.FALSE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castFloat64ToString() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select cast((number.number / 2.0) as string) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals("0.5", rs.getString("test").substring(0, 3));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castBoolToInt64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast(number.number=1 as int64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(1L, rs.getLong("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast(number.number=0 as int64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(0L, rs.getLong("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castStringToFloat64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('2.5' as float64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(2.5D, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast('0.00001' as float64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(0.00001D, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('NaN' as float64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.NaN, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('inf' as float64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.POSITIVE_INFINITY, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('+inf' as float64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.POSITIVE_INFINITY, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('-inf' as float64) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
  }

  @Test
  public void castStringToBytes() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('@' as bytes) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals(ByteArray.copyFrom("@"), rs.getBytes("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castBytesToString() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select cast(b'\\xc2\\xa9' as string) as test from number where number.number=1"))) {
      assertTrue(rs.next());
      assertEquals("©", rs.getString("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testTimestampAddHours() throws SQLException {
    testTimestampAddSub("ADD", "HOUR", (start, number) -> {
      return start + number * 60 * 60;
    }, (start, number) -> {
      return start;
    });
  }

  @Test
  public void testTimestampAddMinutes() throws SQLException {
    testTimestampAddSub("ADD", "MINUTE", (start, number) -> {
      return start + number * 60;
    }, (start, number) -> {
      return start;
    });
  }

  @Test
  public void testTimestampAddSeconds() throws SQLException {
    testTimestampAddSub("ADD", "SECOND", (start, number) -> {
      return start + number;
    }, (start, number) -> {
      return start;
    });
  }

  @Test
  public void testTimestampAddMilliseconds() throws SQLException {
    testTimestampAddSub("ADD", "MILLISECOND", (start, number) -> {
      return start + number / 1000;
    }, (start, number) -> {
      return start + ((int) (number % 1000L)) * 1000000;
    });
  }

  @Test
  public void testTimestampAddMicrosecond() throws SQLException {
    testTimestampAddSub("ADD", "MICROSECOND", (start, number) -> {
      return start;
    }, (start, number) -> {
      return start + number.intValue() * 1000;
    });
  }

  @Test
  public void testTimestampAddNanosecond() throws SQLException {
    testTimestampAddSub("ADD", "NANOSECOND", (start, number) -> {
      return start;
    }, (start, number) -> {
      // The emulator does not store nanosecond values, it only has microsecond precision
      return start + (number.intValue() / 1000) * 1000;
    });
  }

  @Test
  public void testTimestampSubHours() throws SQLException {
    testTimestampAddSub("SUB", "HOUR", (start, number) -> {
      return start - number * 60 * 60;
    }, (start, number) -> {
      return start;
    });
  }

  @Test
  public void testTimestampSubMinutes() throws SQLException {
    testTimestampAddSub("SUB", "MINUTE", (start, number) -> {
      return start - number * 60;
    }, (start, number) -> {
      return start;
    });
  }

  private void testTimestampAddSub(String function, String unit,
      BiFunction<Long, Long, Long> calculateSecondsFunction,
      BiFunction<Integer, Long, Integer> calculateNanosFunction) throws SQLException {
    Timestamp start = Timestamp.parseTimestamp("2008-12-25T15:30:00Z");
    String sql = String.format(
        "SELECT number.number, TIMESTAMP_%s(TIMESTAMP \"2008-12-25 15:30:00 UTC\", INTERVAL number.number %s) as hours FROM number",
        function, unit);
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(sql))) {
      while (rs.next()) {
        assertNotNull(rs.getTimestamp("hours"));
        Timestamp ts = Timestamp.ofTimeSecondsAndNanos(
            calculateSecondsFunction.apply(start.getSeconds(), rs.getLong("number")),
            calculateNanosFunction.apply(start.getNanos(), rs.getLong("number")));
        assertEquals(ts, rs.getTimestamp("hours"));
      }
    }
  }

  @Test
  public void testSelectWithExtract() throws SQLException {
    // @formatter:off
      String sql = "SELECT\r\n" + 
              "  timestamp,\r\n" + 
              "  EXTRACT(NANOSECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(MICROSECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(MILLISECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(SECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(MINUTE FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(HOUR FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(DAYOFWEEK FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(DAY FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(DAYOFYEAR FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(WEEK FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(ISOYEAR FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT(ISOWEEK FROM timestamp) AS isoweek,\r\n" + 
              "  EXTRACT(YEAR FROM timestamp) AS year,\r\n" + 
              "  EXTRACT(WEEK FROM timestamp) AS week,\r\n" + 
              "  EXTRACT(DATE FROM timestamp) AS isoyear\r\n" + 
              "FROM (\r\n" + 
              "    SELECT TIMESTAMP '2005-01-03 12:34:56' AS timestamp UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2007-12-31' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2009-01-01' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2009-12-31' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2017-01-02' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2017-05-26'\r\n" + 
              "  ) AS Timestamps\r\n" + 
              "ORDER BY timestamp";
      // @formatter:on
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(sql))) {
      while (rs.next()) {
        assertNotNull(rs.getTimestamp(0));
        for (int i = 1; i < rs.getColumnCount() - 1; i++) {
          assertNotNull(rs.getDouble(i));
        }
        assertNotNull(rs.getDate(rs.getColumnCount() - 1));
      }
    }
  }

  @Test
  public void testSelectWithExtractAndSpaces() throws SQLException {
    // @formatter:off
      String sql = "SELECT\r\n" + 
              "  timestamp,\r\n" + 
              "  EXTRACT( NANOSECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( MICROSECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( MILLISECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( SECOND FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( MINUTE FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( HOUR FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( DAYOFWEEK FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( DAY FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( DAYOFYEAR FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( WEEK FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( ISOYEAR FROM timestamp) AS isoyear,\r\n" + 
              "  EXTRACT( ISOWEEK FROM timestamp) AS isoweek,\r\n" + 
              "  EXTRACT( YEAR FROM timestamp) AS year,\r\n" + 
              "  EXTRACT( WEEK FROM timestamp) AS week,\r\n" + 
              "  EXTRACT( DATE FROM timestamp) AS isoyear\r\n" + 
              "FROM (\r\n" + 
              "    SELECT TIMESTAMP '2005-01-03 12:34:56' AS timestamp UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2007-12-31' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2009-01-01' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2009-12-31' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2017-01-02' UNION ALL\r\n" + 
              "    SELECT TIMESTAMP '2017-05-26'\r\n" + 
              "  ) AS Timestamps\r\n" + 
              "ORDER BY timestamp";
      // @formatter:on
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(sql))) {
      while (rs.next()) {
        assertNotNull(rs.getTimestamp(0));
        for (int i = 1; i < rs.getColumnCount() - 1; i++) {
          assertNotNull(rs.getDouble(i));
        }
        assertNotNull(rs.getDate(rs.getColumnCount() - 1));
      }
    }
  }

}
