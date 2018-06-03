package io.github.olavloite.spanner.emulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import io.github.olavloite.spanner.emulator.util.CloudSpannerOAuthUtil;

public class DatabaseAdminImplTest {
  private static InstanceAdminClient instanceAdminClient;
  private static DatabaseAdminClient databaseAdminClient;

  @BeforeClass
  public static void setup() {
    String credentialsPath = "emulator.json";
    GoogleCredentials credentials = CloudSpannerOAuthUtil.getCredentialsFromFile(credentialsPath);
    SpannerOptions options = SpannerOptions.newBuilder().setProjectId("test-project")
        .setHost(AbstractSpannerTest.getHost()).setCredentials(credentials).build();
    Spanner spanner = options.getService();
    instanceAdminClient = spanner.getInstanceAdminClient();
    databaseAdminClient = spanner.getDatabaseAdminClient();

    // First delete anything that might be there
    clearCurrentInstanceAndDatabases();
    // Then create a new test instance
    Operation<Instance, CreateInstanceMetadata> operation = instanceAdminClient
        .createInstance(InstanceInfo.newBuilder(InstanceId.of("test-project", "test-instance"))
            .setDisplayName("Test Instance")
            .setInstanceConfigId(InstanceConfigId.of("test-project", "europe-west1"))
            .setNodeCount(1).build());
    assertTrue(operation.isDone());
    operation = instanceAdminClient
        .createInstance(InstanceInfo.newBuilder(InstanceId.of("test-project", "test-instance-2"))
            .setDisplayName("Test Instance 2")
            .setInstanceConfigId(InstanceConfigId.of("test-project", "europe-west1"))
            .setNodeCount(1).build());
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
      Page<Database> databases = databaseAdminClient.listDatabases(instance.getId().getInstance());
      databases.iterateAll().forEach(d -> d.drop());
      instanceAdminClient.deleteInstance(instance.getId().getInstance());
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
  }

  private void dropAllDatabases() {
    Iterator<Instance> iterator = instanceAdminClient.listInstances().iterateAll().iterator();
    while (iterator.hasNext()) {
      Page<Database> databases =
          databaseAdminClient.listDatabases(iterator.next().getId().getInstance());
      databases.iterateAll().forEach(d -> d.drop());
    }
  }

  private void testCreateDatabase() {
    Operation<Database, CreateDatabaseMetadata> operation = databaseAdminClient
        .createDatabase("test-instance", "test-database", Collections.emptyList());
    assertNotNull(operation);
    assertTrue(operation.getName().startsWith(
        "projects/test-project/instances/test-instance/databases/test-database/operations/"));
    operation = operation.waitFor();
    Database database = operation.getResult();
    assertNotNull(database);
    assertEquals("projects/test-project/instances/test-instance/databases/test-database",
        database.getId().getName());
  }

  private void testCreateDatabase2() {
    Operation<Database, CreateDatabaseMetadata> operation = databaseAdminClient
        .createDatabase("test-instance-2", "test-database", Collections.emptyList());
    assertNotNull(operation);
    assertTrue(operation.getName().startsWith(
        "projects/test-project/instances/test-instance-2/databases/test-database/operations/"));
    operation = operation.waitFor();
    Database database = operation.getResult();
    assertNotNull(database);
    assertEquals("projects/test-project/instances/test-instance-2/databases/test-database",
        database.getId().getName());
  }

  private void testCreateDatabaseWithDDL() {
    Operation<Database, CreateDatabaseMetadata> operation = databaseAdminClient.createDatabase(
        "test-instance", "test-database-with-ddl",
        Arrays.asList("create table foo (id int64 not null, name string(100)) primary key (id)"));
    assertTrue(operation.getName().startsWith(
        "projects/test-project/instances/test-instance/databases/test-database-with-ddl/operations/"));
  }

  private void testGetDatabase() {
    assertEquals("projects/test-project/instances/test-instance/databases/test-database",
        databaseAdminClient.getDatabase("test-instance", "test-database").getId().getName());
    assertEquals("projects/test-project/instances/test-instance-2/databases/test-database",
        databaseAdminClient.getDatabase("test-instance-2", "test-database").getId().getName());
  }

}
