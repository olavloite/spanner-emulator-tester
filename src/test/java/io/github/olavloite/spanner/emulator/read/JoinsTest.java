package io.github.olavloite.spanner.emulator.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;
import io.github.olavloite.spanner.emulator.util.EnglishNumberToWords;

/**
 * Test class testing the different types and options of Joins in Cloud Spanner.
 * 
 * @author loite
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JoinsTest extends AbstractSpannerTest {
  private static final Log log = LogFactory.getLog(JoinsTest.class);
  private static final long PERSON_RECORD_COUNT = 10L;
  private static final long ADDRESS_RECORD_WITH_PERSON_COUNT = 30L;
  private static final long ADDRESS_RECORD_WITHOUT_PERSON_COUNT = 10L;

  private static final long PERSON_TABLE_COLUMN_COUNT = 3;
  private static final long ADDRESS_TABLE_COLUMN_COUNT = 7;

  @BeforeClass
  public static void before() {
    log.info("Starting to create tables for join tests");
    executeDdl(Arrays.asList(
        "CREATE TABLE person (person_id INT64 NOT NULL, FIRST_NAME STRING(100), LAST_NAME STRING(100) NOT NULL) PRIMARY KEY (PERSON_ID)",
        "create table address (address_id int64 not null, street string(100) not null, house_number int64, zipcode string(10) not null, begin_date date not null, end_date date, person_id int64) primary key (address_id)",
        "create index idx_address_person on address (person_id)"));
    log.info("Finished creating tables for join tests");
  }

  @Test
  public void test01_InsertData() {
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        for (long id = 1; id <= PERSON_RECORD_COUNT; id++) {
          transaction
              .buffer(Mutation.newInsertBuilder("person").set("person_id").to(id).set("first_name")
                  .to(new StringBuilder(EnglishNumberToWords.convert(id)).reverse().toString())
                  .set("last_name").to(EnglishNumberToWords.convert(id)).build());
        }
        for (long id = 1; id <= ADDRESS_RECORD_WITH_PERSON_COUNT; id++) {
          transaction
              .buffer(Mutation.newInsertBuilder("address").set("address_id").to(id).set("street")
                  .to(new StringBuilder(EnglishNumberToWords.convert(id)).reverse().toString())
                  .set("house_number").to(id % 20).set("zipcode").to("1234TE").set("begin_date")
                  .to(Date.fromYearMonthDay(2010, 1, 1)).set("end_date")
                  .to(id > PERSON_RECORD_COUNT ? Date.fromYearMonthDay(2018, 1, 1) : null)
                  .set("person_id").to(((id - 1) % PERSON_RECORD_COUNT) + 1).build());
        }
        for (long id = ADDRESS_RECORD_WITH_PERSON_COUNT + 1; id <= ADDRESS_RECORD_WITH_PERSON_COUNT
            + ADDRESS_RECORD_WITHOUT_PERSON_COUNT; id++) {
          transaction
              .buffer(Mutation.newInsertBuilder("address").set("address_id").to(id).set("street")
                  .to(new StringBuilder(EnglishNumberToWords.convert(id)).reverse().toString())
                  .set("house_number").to(id % 20).set("zipcode").to("1234TE").set("begin_date")
                  .to(Date.fromYearMonthDay(2010, 1, 1)).set("end_date")
                  .to(id > PERSON_RECORD_COUNT ? Date.fromYearMonthDay(2018, 1, 1) : null)
                  .set("person_id").to((Long) null).build());
        }
        return null;
      }
    });
    assertEquals(PERSON_RECORD_COUNT, getPersonRecordCount());
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT + ADDRESS_RECORD_WITHOUT_PERSON_COUNT,
        getAddressRecordCount());
  }

  @Test
  public void test02_SelectStarFromAddressInnerJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select * from address inner join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        assertEquals(PERSON_TABLE_COLUMN_COUNT + ADDRESS_TABLE_COLUMN_COUNT, rs.getColumnCount());
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
  }

  @Test
  public void test03_SelectStarFromAddressLeftOuterJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from address left outer join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT + ADDRESS_RECORD_WITHOUT_PERSON_COUNT, count);
  }

  @Test
  public void test04_SelectStarFromAddressLeftJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select * from address left join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT + ADDRESS_RECORD_WITHOUT_PERSON_COUNT, count);
  }

  @Test
  public void test05_SelectStarFromAddressFullOuterJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from address full outer join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT + ADDRESS_RECORD_WITHOUT_PERSON_COUNT, count);
  }

  @Test
  public void test06_SelectStarFromAddressFullJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select * from address full join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT + ADDRESS_RECORD_WITHOUT_PERSON_COUNT, count);
  }

  @Test
  public void test07_SelectStarFromAddressRightOuterJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from address right outer join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
  }

  @Test
  public void test08_SelectStarFromAddressRightJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement
        .of("select * from address right join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
  }

  @Test
  public void test09_SelectStarFromAddressCrossJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from address cross join person"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals((ADDRESS_RECORD_WITH_PERSON_COUNT + ADDRESS_RECORD_WITHOUT_PERSON_COUNT)
        * PERSON_RECORD_COUNT, count);
  }

  @Test
  public void test10_SelectStarFromAddressCommaPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from address, person"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals((ADDRESS_RECORD_WITH_PERSON_COUNT + ADDRESS_RECORD_WITHOUT_PERSON_COUNT)
        * PERSON_RECORD_COUNT, count);
  }

  @Test
  public void test11_SelectStarFromAddressInnerJoinPersonUsing() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select * from address inner join person using (person_id)"))) {
      while (rs.next()) {
        assertEquals(PERSON_TABLE_COLUMN_COUNT + ADDRESS_TABLE_COLUMN_COUNT - 1,
            rs.getColumnCount());
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
  }

  @Test
  public void test12_SelectStarFromAddressInnerHashJoinPerson() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from address inner hash join person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
  }

  @Test
  public void test13_SelectStarFromAddressInnerJoinPersonWithHashJoinHint() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from address inner join@{JOIN_TYPE=HASH_JOIN} person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
  }

  @Test
  public void test14_SelectStarFromAddressInnerJoinPersonWithForceJoinOrderHint() {
    long count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from address inner join@{FORCE_JOIN_ORDER=true} person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
    count = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse().executeQuery(Statement.of(
        "select * from address inner join@{FORCE_JOIN_ORDER   \n=\ttrue} person on address.person_id=person.person_id"))) {
      while (rs.next()) {
        count++;
      }
    }
    assertEquals(ADDRESS_RECORD_WITH_PERSON_COUNT, count);
  }

  private long getPersonRecordCount() {
    long res = 0L;
    try (ResultSet rs =
        getDatabaseClient().singleUse().executeQuery(Statement.of("select count(*) from person"))) {
      assertTrue(rs.next());
      res = rs.getLong(0);
      assertFalse(rs.next());
    }
    return res;
  }

  private long getAddressRecordCount() {
    long res = 0L;
    try (ResultSet rs = getDatabaseClient().singleUse()
        .executeQuery(Statement.of("select count(*) from address"))) {
      assertTrue(rs.next());
      res = rs.getLong(0);
      assertFalse(rs.next());
    }
    return res;
  }

}
