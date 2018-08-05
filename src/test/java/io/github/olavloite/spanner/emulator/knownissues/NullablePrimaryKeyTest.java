package io.github.olavloite.spanner.emulator.knownissues;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.cloud.spanner.SpannerException;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class NullablePrimaryKeyTest extends AbstractSpannerTest {
  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void testCreateTableWithNullableStringAsPrimaryKey() {
    // This does not work when running on the emulator. Only INT64 can be used as a nullable primary
    // key value
    if (isRunningOnEmulator()) {
      expected.expect(SpannerException.class);
      expected.expectMessage("COALESCE");
    }
    executeDdl("create table test (id string(100)) primary key (id)");
  }

}
