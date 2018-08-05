package io.github.olavloite.spanner.emulator.ddl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class DropTest extends AbstractSpannerTest {

  private static final String INDEX_TEST_TABLE = "test_index_table";

  @BeforeClass
  public static void createIndexTestTable() {
    executeDdl(
        String.format("create table %s (id int64 not null, name string(100)) primary key (id)",
            INDEX_TEST_TABLE));
  }

  @AfterClass
  public static void dropIndexTestTable() {
    executeDdl(String.format("drop table %s ", INDEX_TEST_TABLE));
  }

  @Test
  public void testDropTable() {
    String tableName = "test_drop_table";
    executeDdl(String.format(
        "create table %s (id int64 not null, name string(100)) primary key (id)", tableName));
    assertTrue(tableExists(tableName));
    executeDdl(String.format("drop table %s", tableName));
    assertFalse(tableExists(tableName));
  }

  @Test
  public void testDropTableWithQuotes() {
    String tableName = "test_drop_table_with_quotes";
    executeDdl(String.format(
        "create table `%s` (id int64 not null, name string(100)) primary key (id)", tableName));
    assertTrue(tableExists(tableName));
    executeDdl(String.format("drop table `%s`", tableName));
    assertFalse(tableExists(tableName));
  }

  @Test
  public void testDropTableDifferingCases() {
    String tableName = "test_drop_table_differing_cases";
    executeDdl(String.format(
        "create table %s (id int64 not null, name string(100)) primary key (id)", tableName));
    assertTrue(tableExists(tableName));
    executeDdl(String.format("drop table %s", tableName.toUpperCase()));
    assertFalse(tableExists(tableName));
  }

  @Test
  public void testDropTableDifferingCasesAndQuotes() {
    String tableName = "test_drop_table_differing_cases_and_quotes";
    executeDdl(String.format(
        "create table `%s` (id int64 not null, name string(100)) primary key (id)", tableName));
    assertTrue(tableExists(tableName));
    executeDdl(String.format("drop table `%s`", tableName.toUpperCase()));
    assertFalse(tableExists(tableName));
  }

  @Test
  public void testDropIndex() {
    String indexName = "test_drop_index";
    executeDdl(String.format("create index %s on %s (name)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testDropIndexWithQuotes() {
    String indexName = "test_drop_index_with_quotes";
    executeDdl(String.format("create index `%s` on `%s` (`name`)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    executeDdl(String.format("drop index `%s`", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testDropIndexDifferingCases() {
    String indexName = "test_drop_index_differing_cases";
    executeDdl(String.format("create index %s on %s (name)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    executeDdl(String.format("drop index %s", indexName.toUpperCase()));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testDropIndexDifferingCasesAndWithQuotes() {
    String indexName = "test_drop_index_differing_cases_and_quotes";
    executeDdl(String.format("create index `%s` on `%s` (`name`)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    executeDdl(String.format("drop index `%s`", indexName.toUpperCase()));
    assertFalse(indexExists(indexName));
  }

}
