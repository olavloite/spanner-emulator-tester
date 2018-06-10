package io.github.olavloite.spanner.emulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.InstanceInfo.InstanceField;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.Lists;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import com.google.spanner.admin.instance.v1.UpdateInstanceMetadata;
import io.github.olavloite.spanner.emulator.util.CloudSpannerOAuthUtil;

public class InstanceAdminImplTest {
  private static InstanceAdminClient instanceAdminClient;
  private List<InstanceId> createdInstances = new ArrayList<>();

  @BeforeClass
  public static void setup() {
    String credentialsPath = "emulator.json";
    GoogleCredentials credentials = CloudSpannerOAuthUtil.getCredentialsFromFile(credentialsPath);
    SpannerOptions options = SpannerOptions.newBuilder().setProjectId("test-project")
        .setCredentials(credentials).setHost(AbstractSpannerTest.getHost()).build();
    Spanner spanner = options.getService();
    instanceAdminClient = spanner.getInstanceAdminClient();
  }

  @Test
  public void testInstanceAdmin() {
    assertNotNull(instanceAdminClient);
    // TODO: Reinstate this test once projects can be created programmatically
    // deleteAllInstances();
    // Assert no instances at startup
    // testInitialNoInstances();
    // Assert instance configs
    testInstanceConfigs();
    // Create a new instance
    testCreateInstance();
    // Update instance
    testUpdateInstance();
    // Try to create an instance with the same name
    testCreateInstanceExists();
    // Try to update a non existent instance
    testUpdateInstanceNotExists();
    // Try to create another instance
    testCreateAnotherInstance();
    // Try to get an instance that does not exist
    testGetInstanceNotExists();
    // Remove all instances
    deleteAllCreatedInstances();
  }

  /**
   * Reinstate once projects can be created programmatically
   */
  @SuppressWarnings("unused")
  private void deleteAllInstances() {
    Page<Instance> instances = instanceAdminClient.listInstances();
    instances.iterateAll()
        .forEach(i -> instanceAdminClient.deleteInstance(i.getId().getInstance()));
  }

  private void deleteAllCreatedInstances() {
    Page<Instance> instances = instanceAdminClient.listInstances();
    instances.iterateAll().forEach(i -> {
      if (createdInstances.contains(i.getId())) {
        instanceAdminClient.deleteInstance(i.getId().getInstance());
      }
    });
  }

  /**
   * Reinstate this test once a project can be created programmatically
   */
  @SuppressWarnings("unused")
  private void testInitialNoInstances() {
    Page<Instance> instances = instanceAdminClient.listInstances();
    assertNotNull(instances);
    List<Instance> instanceList = Lists.newArrayList(instances.iterateAll().iterator());
    assertEquals(0, instanceList.size());
  }

  private void testInstanceConfigs() {
    Page<InstanceConfig> configs = instanceAdminClient.listInstanceConfigs();
    assertNotNull(configs);
    List<InstanceConfig> configList = Lists.newArrayList(configs.iterateAll().iterator());
    assertEquals(12, configList.size());

    assertEquals("Belgium",
        instanceAdminClient.getInstanceConfig("regional-europe-west1").getDisplayName());
    assertEquals("Montréal",
        instanceAdminClient.getInstanceConfig("regional-northamerica-northeast1").getDisplayName());
    assertEquals("North America, Europe, and Asia",
        instanceAdminClient.getInstanceConfig("nam-eur-asia1").getDisplayName());

    boolean exception = false;
    try {
      instanceAdminClient.getInstanceConfig("norway");
    } catch (SpannerException e) {
      exception = true;
      assertEquals(ErrorCode.NOT_FOUND, e.getErrorCode());
    }
    assertTrue(exception);
  }

  private void testCreateInstance() {
    InstanceId id = InstanceId.of("test-project", "test-instance-" + new Random().nextInt(1000000));
    Operation<Instance, CreateInstanceMetadata> operation = instanceAdminClient
        .createInstance(InstanceInfo.newBuilder(id).setDisplayName("Test Instance")
            .setInstanceConfigId(InstanceConfigId.of("test-project", "regional-europe-west1"))
            .setNodeCount(1).build());
    createdInstances.add(id);
    assertNotNull(operation);
    assertTrue(operation.getName().startsWith(
        String.format("projects/test-project/instances/%s/operations/", id.getInstance())));
    // check that the instance was created
    assertEquals(1,
        Lists.newArrayList(instanceAdminClient.listInstances().iterateAll().iterator()).stream()
            .filter(i -> createdInstances.contains(i.getId())).collect(Collectors.toList()).size());
    assertEquals("Test Instance",
        instanceAdminClient.getInstance(id.getInstance()).getDisplayName());
    assertEquals(1, instanceAdminClient.getInstance(id.getInstance()).getNodeCount());
  }

  private void testGetInstanceNotExists() {
    boolean exception = false;
    try {
      instanceAdminClient.getInstance("test-instance-not-exists");
    } catch (SpannerException e) {
      exception = true;
      assertEquals(ErrorCode.NOT_FOUND, e.getErrorCode());
    }
    assertTrue(exception);
  }

  private void testUpdateInstance() {
    InstanceId id = createdInstances.get(0);
    Operation<Instance, UpdateInstanceMetadata> operation = instanceAdminClient
        .updateInstance(InstanceInfo.newBuilder(id).setDisplayName("Test Instance 2")
            .setInstanceConfigId(InstanceConfigId.of("test-project", "europe-west1"))
            .setNodeCount(2).build(), InstanceField.DISPLAY_NAME, InstanceField.NODE_COUNT);
    assertNotNull(operation);
    assertTrue(operation.getName().startsWith(
        String.format("projects/test-project/instances/%s/operations/", id.getInstance())));
    // check that the instance was updated
    assertEquals("Test Instance 2",
        instanceAdminClient.getInstance(id.getInstance()).getDisplayName());
    assertEquals(2, instanceAdminClient.getInstance(id.getInstance()).getNodeCount());
  }

  private void testCreateInstanceExists() {
    boolean exception = false;
    try {
      InstanceId id = createdInstances.get(0);
      instanceAdminClient.createInstance(InstanceInfo.newBuilder(id).setDisplayName("Test Instance")
          .setInstanceConfigId(InstanceConfigId.of("test-project", "europe-west1")).setNodeCount(1)
          .build());
    } catch (SpannerException e) {
      exception = true;
      assertEquals(ErrorCode.ALREADY_EXISTS, e.getErrorCode());
    }
    assertTrue(exception);
  }

  private void testUpdateInstanceNotExists() {
    boolean exception = false;
    try {
      instanceAdminClient
          .updateInstance(InstanceInfo.newBuilder(InstanceId.of("test-project", "test-instance2"))
              .setDisplayName("Test Instance 2")
              .setInstanceConfigId(InstanceConfigId.of("test-project", "europe-west1"))
              .setNodeCount(2).build(), InstanceField.DISPLAY_NAME, InstanceField.NODE_COUNT);
    } catch (SpannerException e) {
      exception = true;
      assertEquals(ErrorCode.NOT_FOUND, e.getErrorCode());
    }
    assertTrue(exception);
  }

  private void testCreateAnotherInstance() {
    InstanceId id =
        InstanceId.of("test-project", "another-test-instance-" + new Random().nextInt(1000000));
    Operation<Instance, CreateInstanceMetadata> operation = instanceAdminClient
        .createInstance(InstanceInfo.newBuilder(id).setDisplayName("Another Test Instance")
            .setInstanceConfigId(InstanceConfigId.of("test-project", "europe-west1"))
            .setNodeCount(1).build());
    createdInstances.add(id);
    assertNotNull(operation);
    assertTrue(operation.getName().startsWith(
        String.format("projects/test-project/instances/%s/operations/", id.getInstance())));
    // check that the instance was created
    assertEquals(2,
        Lists.newArrayList(instanceAdminClient.listInstances().iterateAll().iterator()).stream()
            .filter(i -> createdInstances.contains(i.getId())).collect(Collectors.toList()).size());
    assertEquals("Another Test Instance",
        instanceAdminClient.getInstance(id.getInstance()).getDisplayName());
    assertEquals(1, instanceAdminClient.getInstance(id.getInstance()).getNodeCount());
  }

}
