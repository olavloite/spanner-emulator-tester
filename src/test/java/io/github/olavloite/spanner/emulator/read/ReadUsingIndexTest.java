package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

public class ReadUsingIndexTest extends AbstractSpannerTest {
  private static final long NUMBER_OF_ROWS = 100L;

  @BeforeClass
  public static void before() {
    createNumberTable();
    createIndexOnNumberName();
    insertTestNumbers(NUMBER_OF_ROWS);
  }

  @AfterClass
  public static void after() {
    dropIndexNumberName();
    dropNumberTable();
  }

  @Test
  public void testReadAll() {
    long count = 0L;
    List<String> numbers = new ArrayList<String>((int) NUMBER_OF_ROWS);
    for (long i = 1L; i <= NUMBER_OF_ROWS; i++) {
      numbers.add(EnglishNumberToWords.convert(i));
    }
    Collections.sort(numbers);
    try (ResultSet rs = getDatabaseClient().singleUse().readUsingIndex("number", "idx_number_name",
        KeySet.all(), Arrays.asList("number", "name"))) {
      while (rs.next()) {
        assertEquals(numbers.get((int) count), rs.getString(1));
        count++;
      }
    }
    assertEquals(NUMBER_OF_ROWS, count);
  }

  @Test
  public void testReadOne() {
    long number = NUMBER_OF_ROWS / 2;
    String key = EnglishNumberToWords.convert(number);
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().readUsingIndex("number", "idx_number_name",
        KeySet.singleKey(Key.of(key)), Arrays.asList("number", "name"))) {
      while (rs.next()) {
        assertEquals(number, rs.getLong(0));
        assertEquals(key, rs.getString(1));
        count++;
      }
    }
    assertEquals(1L, count);
  }

  @Test
  public void testReadPrefixRange() {
    String key = "fifty";
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().readUsingIndex("number", "idx_number_name",
        KeySet.prefixRange(Key.of(key)), Arrays.asList("number", "name"))) {
      while (rs.next()) {
        assertEquals(50L, rs.getLong(0));
        assertTrue(rs.getString(1).startsWith("fifty"));
        count++;
      }
    }
    assertEquals(1L, count);
  }

}
