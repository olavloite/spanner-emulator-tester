package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class ArrayTest extends AbstractSpannerTest {

  @Test
  public void testSelectArrayFromSubQuery() {
    // @formatter:off
    String sql = "SELECT ARRAY\n" + 
        "  (SELECT 1 UNION ALL\n" + 
        "   SELECT 2 UNION ALL\n" + 
        "   SELECT 3) AS new_array";
    // @formatter:on
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(sql))) {
      while (rs.next()) {
        assertEquals(1L, rs.getLongArray("new_array")[0]);
        assertEquals(2L, rs.getLongArray("new_array")[1]);
        assertEquals(3L, rs.getLongArray("new_array")[2]);
      }
    }
  }

  @Test
  public void testSelectArrayFromStructSubQuery() {
    // @formatter:off
    String sql = "SELECT\n" + 
        "  ARRAY\n" + 
        "    (SELECT AS STRUCT 1, 2, 3\n" + 
        "     UNION ALL SELECT AS STRUCT 4, 5, 6) AS new_array";
    // @formatter:on
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(sql))) {
      while (rs.next()) {
        // TODO: The emulator returns these structs as strings
        assertNotNull(rs.getStringList(0));
      }
    }
  }

  @Test
  public void testSelectArrayOffsetIndex() {
    String[][] expected = new String[][] {{"apples", "bananas", "pears", "grapes"},
        {"coffee", "tea", "milk"}, {"cake", "pie"}};

    // @formatter:off
    String sql = "SELECT list, list[OFFSET(1)] as offset_1, list[ORDINAL(1)] as ordinal_1\n" + 
        "FROM (\n" + 
        "  SELECT [\"apples\", \"bananas\", \"pears\", \"grapes\"] as list\n" + 
        "  UNION ALL SELECT [\"coffee\", \"tea\", \"milk\" ] as list\n" + 
        "  UNION ALL SELECT [\"cake\", \"pie\"] as list) AS items";
    // @formatter:on
    int count = 0;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(sql))) {
      while (rs.next()) {
        assertEquals(expected[count][1], rs.getString("offset_1"));
        assertEquals(expected[count][0], rs.getString("ordinal_1"));
        count++;
      }
    }
    assertEquals(3, count);
  }

}
