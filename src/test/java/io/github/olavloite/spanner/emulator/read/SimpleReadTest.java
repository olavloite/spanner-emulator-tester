package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

public class SimpleReadTest extends AbstractSpannerTest {
  private static final long NUMBER_OF_ROWS = 100L;

  @BeforeClass
  public static void before() {
    createNumberTable();
    insertTestNumbers(NUMBER_OF_ROWS);
  }

  @AfterClass
  public static void after() {
    dropNumberTable();
  }

  @Test
  public void testReadAll() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().read("number", KeySet.all(),
        Arrays.asList("number", "name"))) {
      while (rs.next()) {
        count++;
        assertEquals(count, rs.getLong(0));
        assertEquals(EnglishNumberToWords.convert(count), rs.getString(1));
      }
    }
    assertEquals(NUMBER_OF_ROWS, count);
  }

  @Test
  public void testReadOne() {
    long key = NUMBER_OF_ROWS / 2;
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().read("number",
        KeySet.singleKey(Key.of(key)), Arrays.asList("number", "name"))) {
      while (rs.next()) {
        assertEquals(key, rs.getLong(0));
        assertEquals(EnglishNumberToWords.convert(key), rs.getString(1));
        count++;
      }
    }
    assertEquals(1L, count);
  }

  @Test
  public void testReadRange() {
    long key = NUMBER_OF_ROWS / 2;
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().read("number",
        KeySet.range(KeyRange.closedClosed(Key.of(key - 1), Key.of(key + 1))),
        Arrays.asList("number", "name"))) {
      while (rs.next()) {
        assertEquals(key + count - 1, rs.getLong(0));
        assertEquals(EnglishNumberToWords.convert(key + count - 1), rs.getString(1));
        count++;
      }
    }
    assertEquals(3L, count);
  }

  @Test
  public void testReadPrefixRange() {
    long key = 4L;
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().read("number",
        KeySet.prefixRange(Key.of(key)), Arrays.asList("number", "name"))) {
      while (rs.next()) {
        assertEquals(key, rs.getLong(0));
        assertEquals(EnglishNumberToWords.convert(key), rs.getString(1));
        count++;
      }
    }
    assertEquals(1L, count);
  }

}
