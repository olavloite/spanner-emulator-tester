package io.github.olavloite.spanner.emulator.ddl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

/**
 * Tables in Cloud Spanner are required to have a primary key. A primary key column is however
 * allowed to contain null values, and Cloud Spanner treats null values differently from many other
 * databases: Two null values are considered to be equal in Cloud Spanner, while most other database
 * systems consider two null values not to be equal, as null is defined as 'undefined/unknown' and
 * could in theory be anything. This means that if you define a table with a primary key that has
 * one or more nullable columns in Cloud Spanner, you must consider any null value you enter into
 * one of those columns as an actual value. This means that if you have a primary key consisting of
 * two columns that are both nullable, the following combinations of values for your primary key are
 * allowed:
 * 
 * <pre>
 * id1  | id2
 * 1    | 1
 * 1    | 2
 * null | null
 * null | 1
 * 1    | null
 * 2    | null
 * </pre>
 * 
 * The following contents would however not be allowed:
 * 
 * <pre>
 * id1  | id2
 * 1    | null
 * 1    | null
 * </pre>
 * 
 * My personal recommendation would be to never allow null values in a primary key, as the behavior
 * of this might differ between different database systems. It also seems like a bad design choice
 * to have a primary key that might contain unknown values.
 * 
 * @author loite
 *
 */
public class CreateTableWithNullablePK extends AbstractSpannerTest {

  @Test
  public void createTableWithInt64NullablePK() {
    assertFalse(tableExists("foo"));
    executeDdl("create table foo (id int64, bar string(100)) primary key (id)");
    assertTrue(tableExists("foo"));
    executeDdl("drop table foo");
    assertFalse(tableExists("foo"));
  }

  @Test
  public void createTableWithStringNullablePK() {
    assertFalse(tableExists("foo"));
    executeDdl("create table foo (id string(100), bar string(100)) primary key (id)");
    assertTrue(tableExists("foo"));
    executeDdl("drop table foo");
    assertFalse(tableExists("foo"));
  }

  @Test
  public void createTableWithStringStringNullablePK() {
    assertFalse(tableExists("foo"));
    executeDdl(
        "create table foo (id1 string(100), id2 string(100), bar string(100)) primary key (id1, id2)");
    assertTrue(tableExists("foo"));
    executeDdl("drop table foo");
    assertFalse(tableExists("foo"));
  }

  @Test
  public void createTableWithStringNullableStringNotNullablePK() {
    assertFalse(tableExists("foo"));
    executeDdl(
        "create table foo (id1 string(100), id2 string(100) not null, bar string(100)) primary key (id1, id2)");
    assertTrue(tableExists("foo"));
    executeDdl("drop table foo");
    assertFalse(tableExists("foo"));
  }

}
