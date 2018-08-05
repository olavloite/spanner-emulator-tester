package io.github.olavloite.spanner.emulator.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;

public class CreateIndexTest extends AbstractSpannerTest {

  private static final String INDEX_TEST_TABLE = "index_test_table";

  @BeforeClass
  public static void createIndexTestTable() {
    executeDdl(String.format(
        "create table %s (id int64 not null, name string(100), begin_timestamp timestamp, valid bool not null) primary key (id)",
        INDEX_TEST_TABLE));
  }

  @AfterClass
  public static void dropIndexTestTable() {
    executeDdl(String.format("drop table %s ", INDEX_TEST_TABLE));
  }

  @Test
  public void testCreateIndex() {
    String indexName = "test_create_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format("create index %s on %s (name)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertFalse(isIndexUnique(indexName));
    assertFalse(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "name"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateUniqueIndex() {
    String indexName = "test_create_unique_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format("create unique index %s on %s (name)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertTrue(isIndexUnique(indexName));
    assertFalse(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "name"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateIndexDesc() {
    String indexName = "test_create_index_desc";
    assertFalse(indexExists(indexName));
    executeDdl(String.format("create index %s on %s (name desc)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertFalse(isIndexUnique(indexName));
    assertFalse(isIndexNullFiltered(indexName));
    assertEquals("DESC", getIndexColumnSortOrder(indexName, "name"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateIndexDescQuoted() {
    String indexName = "test_create_index_desc_quoted";
    assertFalse(indexExists(indexName));
    executeDdl(
        String.format("create index `%s` on `%s` (`name` desc)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertFalse(isIndexUnique(indexName));
    assertFalse(isIndexNullFiltered(indexName));
    assertEquals("DESC", getIndexColumnSortOrder(indexName, "name"));
    executeDdl(String.format("drop index `%s`", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateIndexDescMultiple() {
    String indexName = "test_create_index_desc_multiple";
    assertFalse(indexExists(indexName));
    executeDdl(String.format("create index %s on %s (name desc, begin_timestamp asc, valid desc)",
        indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertFalse(isIndexUnique(indexName));
    assertFalse(isIndexNullFiltered(indexName));
    assertEquals("DESC", getIndexColumnSortOrder(indexName, "name"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "begin_timestamp"));
    assertEquals("DESC", getIndexColumnSortOrder(indexName, "valid"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateNullFilteredIndex() {
    String indexName = "test_create_null_filtered_index";
    assertFalse(indexExists(indexName));
    executeDdl(
        String.format("create null_filtered index %s on %s (name)", indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertFalse(isIndexUnique(indexName));
    assertTrue(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "name"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateNullFilteredIndexMultiple() {
    String indexName = "test_create_null_filtered_multiple_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format("create null_filtered index %s on %s (name, begin_timestamp, valid)",
        indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertFalse(isIndexUnique(indexName));
    assertTrue(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "name"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "begin_timestamp"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "valid"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateUniqueNullFilteredIndex() {
    String indexName = "test_create_unique_null_filtered_index";
    assertFalse(indexExists(indexName));
    executeDdl(String.format("create unique null_filtered index %s on %s (name)", indexName,
        INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertTrue(isIndexUnique(indexName));
    assertTrue(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "name"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

  @Test
  public void testCreateUniqueNullFilteredIndexMultiple() {
    String indexName = "test_create_unique_null_filtered_multiple_index";
    assertFalse(indexExists(indexName));
    executeDdl(
        String.format("create unique null_filtered index %s on %s (name, begin_timestamp, valid)",
            indexName, INDEX_TEST_TABLE));
    assertTrue(indexExists(indexName));
    assertTrue(isIndexUnique(indexName));
    assertTrue(isIndexNullFiltered(indexName));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "name"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "begin_timestamp"));
    assertEquals("ASC", getIndexColumnSortOrder(indexName, "valid"));
    executeDdl(String.format("drop index %s", indexName));
    assertFalse(indexExists(indexName));
  }

}
