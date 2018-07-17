package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RegEgNotTest {

  @Test
  public void testRegExNot() {
    assertEquals("array['one','two','three']",
        "['one','two','three']".replaceAll("(?is)(\\[\\s*'.*?\\])", "array$1"));
    assertEquals("array[1,2,3]", "[1,2,3]".replaceAll("(?is)(\\[\\s*\\d+.*?\\])", "array$1"));
    assertEquals("[offset(1)]", "[offset(1)]".replaceAll("(?is)(\\[\\s*'.*?\\])", "array$1"));
    assertEquals("[offset(1)]", "[offset(1)]".replaceAll("(?is)(\\[\\s*\\d+.*?\\])", "array$1"));
  }

}
