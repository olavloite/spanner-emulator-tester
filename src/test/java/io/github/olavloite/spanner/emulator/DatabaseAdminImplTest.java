package io.github.olavloite.spanner.emulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import io.github.olavloite.spanner.emulator.util.CloudSpannerOAuthUtil;

public class DatabaseAdminImplTest {
  private static InstanceAdminClient instanceAdminClient;
  private static DatabaseAdminClient databaseAdminClient;

  private static InstanceId id1;
  private static InstanceId id2;

  @BeforeClass
  public static void setup() throws InterruptedException, ExecutionException {
    String credentialsPath = AbstractSpannerTest.getKeyFile();
    GoogleCredentials credentials = CloudSpannerOAuthUtil.getCredentialsFromFile(credentialsPath);
    SpannerOptions options =
        SpannerOptions.newBuilder().setProjectId(AbstractSpannerTest.getProject())
            .setHost(AbstractSpannerTest.getHost()).setCredentials(credentials).build();
    Spanner spanner = options.getService();
    instanceAdminClient = spanner.getInstanceAdminClient();
    databaseAdminClient = spanner.getDatabaseAdminClient();

    // Create a new test instance
    id1 = InstanceId.of(AbstractSpannerTest.getProject(),
        "test-instance-" + new Random().nextInt(1000000));
    OperationFuture<Instance, CreateInstanceMetadata> operation = instanceAdminClient
        .createInstance(InstanceInfo.newBuilder(id1).setDisplayName("Test Instance")
            .setInstanceConfigId(
                InstanceConfigId.of(AbstractSpannerTest.getProject(), "regional-europe-west1"))
            .setNodeCount(1).build());
    operation.get();
    assertTrue(operation.isDone());
    id2 = InstanceId.of(AbstractSpannerTest.getProject(),
        "test-instance-2-" + new Random().nextInt(1000000));
    operation = instanceAdminClient
        .createInstance(InstanceInfo.newBuilder(id2).setDisplayName("Test Instance 2")
            .setInstanceConfigId(
                InstanceConfigId.of(AbstractSpannerTest.getProject(), "regional-europe-west1"))
            .setNodeCount(1).build());
    operation.get();
    assertTrue(operation.isDone());
  }

  @AfterClass
  public static void teardown() {
    clearCurrentInstanceAndDatabases();
  }

  private static void clearCurrentInstanceAndDatabases() {
    Iterator<Instance> iterator = instanceAdminClient.listInstances().iterateAll().iterator();
    while (iterator.hasNext()) {
      Instance instance = iterator.next();
      if (instance.getId().equals(id1) || instance.getId().equals(id2)) {
        Page<Database> databases =
            databaseAdminClient.listDatabases(instance.getId().getInstance());
        databases.iterateAll().forEach(d -> d.drop());
        instanceAdminClient.deleteInstance(instance.getId().getInstance());
      }
    }
  }

  @Test
  public void testDatabaseAdmin() {
    // Drop all current databases to have a clean start
    dropAllDatabases();
    // Create a couple of databases
    testCreateDatabase();
    testCreateDatabase2();
    testCreateDatabaseWithDDL();
    // Try to get the database
    testGetDatabase();
    // Drop all created databases
    dropAllDatabases();
  }

  private void dropAllDatabases() {
    Iterator<Instance> iterator = instanceAdminClient.listInstances().iterateAll().iterator();
    while (iterator.hasNext()) {
      Instance instance = iterator.next();
      if (instance.getId().equals(id1) || instance.getId().equals(id2)) {
        Page<Database> databases =
            databaseAdminClient.listDatabases(instance.getId().getInstance());
        databases.iterateAll().forEach(d -> d.drop());
      }
    }
  }

  private void testCreateDatabase() {
    try {
      OperationFuture<Database, CreateDatabaseMetadata> operation = databaseAdminClient
          .createDatabase(id1.getInstance(), "test-database", Collections.emptyList());
      assertNotNull(operation);
      assertTrue(operation.getName()
          .startsWith(String.format("projects/%s/instances/%s/databases/test-database/operations/",
              AbstractSpannerTest.getProject(), id1.getInstance())));
      Database database = operation.get();
      assertNotNull(database);
      assertEquals(String.format("projects/%s/instances/%s/databases/test-database",
          AbstractSpannerTest.getProject(), id1.getInstance()), database.getId().getName());
    } catch (InterruptedException | ExecutionException e) {
      throw SpannerExceptionFactory.newSpannerException(e);
    }
  }

  private void testCreateDatabase2() {
    try {
      OperationFuture<Database, CreateDatabaseMetadata> operation = databaseAdminClient
          .createDatabase(id2.getInstance(), "test-database", Collections.emptyList());
      assertNotNull(operation);
      assertTrue(operation.getName()
          .startsWith(String.format("projects/%s/instances/%s/databases/test-database/operations/",
              AbstractSpannerTest.getProject(), id2.getInstance())));
      Database database = operation.get();
      assertNotNull(database);
      assertEquals(String.format("projects/%s/instances/%s/databases/test-database",
          AbstractSpannerTest.getProject(), id2.getInstance()), database.getId().getName());
    } catch (InterruptedException | ExecutionException e) {
      throw SpannerExceptionFactory.newSpannerException(e);
    }
  }

  private void testCreateDatabaseWithDDL() {
    try {
      OperationFuture<Database, CreateDatabaseMetadata> operation =
          databaseAdminClient.createDatabase(id1.getInstance(), "test-database-with-ddl", Arrays
              .asList("create table foo (id int64 not null, name string(100)) primary key (id)"));
      assertTrue(operation.getName().startsWith(
          String.format("projects/%s/instances/%s/databases/test-database-with-ddl/operations/",
              AbstractSpannerTest.getProject(), id1.getInstance())));
    } catch (InterruptedException | ExecutionException e) {
      throw SpannerExceptionFactory.newSpannerException(e);
    }
  }

  private void testGetDatabase() {
    assertEquals(
        String.format("projects/%s/instances/%s/databases/test-database",
            AbstractSpannerTest.getProject(), id1.getInstance()),
        databaseAdminClient.getDatabase(id1.getInstance(), "test-database").getId().getName());
    assertEquals(
        String.format("projects/%s/instances/%s/databases/test-database",
            AbstractSpannerTest.getProject(), id2.getInstance()),
        databaseAdminClient.getDatabase(id2.getInstance(), "test-database").getId().getName());
  }

}
