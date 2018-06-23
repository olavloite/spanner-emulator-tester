package io.github.olavloite.spanner.emulator.knownissues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

/**
 * Class showing known issues regarding the text 'hash join'
 * 
 * @author loite
 *
 */
public class HashJoinFailsTest extends AbstractSpannerTest {
  @Rule
  public ExpectedException expected = ExpectedException.none();

  @BeforeClass
  public static void before() {
    createNumberTable();
    insertTestNumbers(100L);
  }

  @Test
  public void testHashJoin() {
    // This works
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select n1.number as `hashjoin` from number n1 inner hash join number n2 on n1.number=n2.number"))) {
      assertTrue(rs.next());
      assertEquals(1L, rs.getLong("hashjoin"));
    }
    // And this works
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select n1.number as `hash join` from number n1 inner join number n2 on n1.number=n2.number"))) {
      assertTrue(rs.next());
      assertEquals(1L, rs.getLong("hash join"));
    }
    // This does not when running on the emulator
    if (isRunningOnEmulator()) {
      try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
          "select n1.number as `hash join` from number n1 inner hash join number n2 on n1.number=n2.number"))) {
        assertTrue(rs.next());
        // the column is automatically renamed to 'join'
        assertEquals(1L, rs.getLong("join"));
        // this column won't be found
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Field not found: hash join");
        assertEquals(1L, rs.getLong("hash join"));
      }
    }
  }

}
