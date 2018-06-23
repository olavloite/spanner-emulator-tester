package io.github.olavloite.spanner.emulator;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;

public class ResultSetTest extends AbstractSpannerTest {

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Before
  public void before() {
    createNumberTable();
  }

  @After
  public void after() {
    dropNumberTable();
  }

  private ResultSet rs;

  @Test()
  public void testSelectOutsideTransaction() {
    getDatabaseClient().readWriteTransaction().run(new TransactionCallable<Void>() {
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        rs = transaction.executeQuery(Statement.of("select * from number"));
        return null;
      }
    });
    expected.expect(SpannerException.class);
    rs.next();
  }

}
