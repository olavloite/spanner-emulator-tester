package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
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
  private static final long NUMBER_OF_ROWS = 100L;

  @BeforeClass
  public static void before() {
    createNumberTable();
    insertTestNumbers(NUMBER_OF_ROWS);
  }

  @Test
  public void castBoolToString() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast(number=1 as string) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals("true", rs.getString("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast(number=2 as string) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals("false", rs.getString("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castIntToFloat64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast(number as float64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(1.0D, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
  }

  @Test
  public void castFloatToInt64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast((number/2.0) as int64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(1L, rs.getLong("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castStringToBool() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('true' as bool) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.TRUE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('FALSE' as bool) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.FALSE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
    boolean exception = false;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('invalid value' as bool) as test from number where number=1"))) {
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
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast(number as bool) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.TRUE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast((number-1) as bool) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Boolean.FALSE, rs.getBoolean("test"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castFloat64ToString() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast((number / 2.0) as string) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals("0.5", rs.getString("test").substring(0, 3));
      assertFalse(rs.next());
    }
  }

  @Test
  public void castBoolToInt64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast(number=1 as int64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(1L, rs.getLong("test"));
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast(number=0 as int64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(0L, rs.getLong("test"));
      assertFalse(rs.next());
    }
  }


  @Test
  public void castStringToFloat64() {
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('2.5' as float64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(2.5D, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('0.00001' as float64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(0.00001D, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('NaN' as float64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.NaN, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('inf' as float64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.POSITIVE_INFINITY, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('+inf' as float64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.POSITIVE_INFINITY, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(
        Statement.of("select cast('-inf' as float64) as test from number where number=1"))) {
      assertTrue(rs.next());
      assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble("test"), 0.0D);
      assertFalse(rs.next());
    }
  }

}
