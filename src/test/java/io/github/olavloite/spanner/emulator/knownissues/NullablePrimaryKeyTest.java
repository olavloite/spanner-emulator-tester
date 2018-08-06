package io.github.olavloite.spanner.emulator.knownissues;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class NullablePrimaryKeyTest extends AbstractSpannerTest {
  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void testCreateTableWithNullableStringAsPrimaryKey() {
    executeDdl("create table test (id string(100)) primary key (id)");
    executeDdl("drop table test");
  }

}
