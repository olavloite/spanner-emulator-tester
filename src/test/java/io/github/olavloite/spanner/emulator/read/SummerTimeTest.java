package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.sql.SQLException;
import java.util.function.BiFunction;
import org.junit.Test;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class SummerTimeTest extends AbstractSpannerTest {

  @Test
  public void testTimestampAddHours() throws SQLException {
    testTimestampAddSub("ADD", "HOUR", (start, number) -> {
      return start + number * 60 * 60;
    }, (start, number) -> {
      return start;
    });
  }

  private void testTimestampAddSub(String function, String unit,
      BiFunction<Long, Long, Long> calculateSecondsFunction,
      BiFunction<Integer, Long, Integer> calculateNanosFunction) throws SQLException {
    Timestamp start = Timestamp.parseTimestamp("2009-03-08T01:30:00Z");
    String sql = String.format(
        "SELECT 1 as number, TIMESTAMP_%s(TIMESTAMP \"2009-03-08 01:30:00 UTC\", INTERVAL 1 %s) as hours",
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

}
