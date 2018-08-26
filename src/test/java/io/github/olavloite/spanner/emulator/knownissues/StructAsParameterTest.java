package io.github.olavloite.spanner.emulator.knownissues;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

/**
 * Cloud Spanner has extensive support for the use of structs in select statements, including
 * passing a struct as a parameter to a query. This is not supported by the emulator.
 * 
 * @author loite
 *
 */
public class StructAsParameterTest extends AbstractSpannerTest {
  @Rule
  public ExpectedException expected = ExpectedException.none();

  @BeforeClass
  public static void before() {
    createNumberTable();
    insertTestNumbers(100L);
  }

  @Test
  public void testStruct() {
    // This works
    try (
        ResultSet rs = getDatabaseClient().singleUse()
            .executeQuery(Statement.newBuilder(
                "select struct(n1.number, n1.name) as `str` from number n1 where n1.number=@number")
                .bind("number").to(1L).build())) {
      int count = 0;
      while (rs.next()) {
        assertEquals("(1,one)", rs.getString("str"));
        count++;
      }
      assertEquals(1, count);
    }
    // This does not when running on the emulator
    if (isRunningOnEmulator()) {
      expected.expect(SpannerException.class);
      expected.expectMessage("Invalid argument type 'struct'");
    }
    Struct number = Struct.newBuilder().set("number").to(1L).build();
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.newBuilder(
        "select struct(n1.number, n1.name) as `str` from number n1 where struct(n1.number)=@number")
        .bind("number").to(number).build())) {
      int count = 0;
      while (rs.next()) {
        assertEquals("1,one", rs.getString("str"));
        count++;
      }
      assertEquals(100, count);
    }
  }

}
