package io.github.olavloite.spanner.emulator.integration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import io.github.olavloite.spanner.emulator.AbstractSpannerTest;
import io.github.olavloite.spanner.emulator.util.CloudSpannerOAuthUtil;

public abstract class AbstractSpannerEmulatorTest {

  private static final String DATABASE_ID = "test-database";

  private static Spanner spanner;
  private static String projectId;
  private static String instanceId;
  private static DatabaseId databaseId;
  private static String credentialsPath;

  protected static Spanner getSpanner() {
    return spanner;
  }

  protected static DatabaseId getDatabaseId() {
    return databaseId;
  }

  protected DatabaseClient getDatabaseClient() {
    return spanner.getDatabaseClient(getDatabaseId());
  }

  protected DatabaseAdminClient getDatabaseAdminClient() {
    return spanner.getDatabaseAdminClient();
  }

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    createSpanner();
    createInstance();
    createDatabase();
  }

  private static void createSpanner() {
    // generate a unique instance id for this test run
    Random rnd = new Random();
    instanceId = "test-instance-" + rnd.nextInt(1000000);
    credentialsPath = "emulator.json";
    projectId = AbstractSpannerTest.PROJECT_ID;
    GoogleCredentials credentials = CloudSpannerOAuthUtil.getCredentialsFromFile(credentialsPath);
    Builder builder = SpannerOptions.newBuilder();
    builder.setProjectId(projectId);
    builder.setCredentials(credentials);
    builder.setHost(AbstractSpannerTest.getHost());

    SpannerOptions options = builder.build();
    spanner = options.getService();
  }

  @AfterClass
  public static void teardown() {
    cleanUpDatabase();
    cleanUpInstance();
  }

  private static void createInstance() {
    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    InstanceConfig config = instanceAdminClient.getInstanceConfig("regional-europe-west1");
    Instance instance = instanceAdminClient.newInstanceBuilder(InstanceId.of(projectId, instanceId))
        .setDisplayName("Test Instance").setInstanceConfigId(config.getId()).setNodeCount(1)
        .build();
    Operation<Instance, CreateInstanceMetadata> createInstance =
        instanceAdminClient.createInstance(instance);
    createInstance = createInstance.waitFor();
  }

  private static void createDatabase() {
    Operation<Database, CreateDatabaseMetadata> createDatabase =
        spanner.getDatabaseAdminClient().createDatabase(instanceId, DATABASE_ID, Arrays.asList());
    createDatabase = createDatabase.waitFor();
    databaseId = DatabaseId.of(InstanceId.of(projectId, instanceId), DATABASE_ID);
  }

  private static void cleanUpInstance() {
    spanner.getInstanceAdminClient().deleteInstance(instanceId);
  }

  private static void cleanUpDatabase() {
    spanner.getDatabaseAdminClient().dropDatabase(instanceId, DATABASE_ID);
  }

}
