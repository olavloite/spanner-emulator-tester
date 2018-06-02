package io.github.olavloite.spanner.emulator.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;

public class CloudSpannerOAuthUtil {
  public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

  public static final HttpTransportFactory HTTP_TRANSPORT_FACTORY =
      new DefaultHttpTransportFactory();

  static class DefaultHttpTransportFactory implements HttpTransportFactory {

    @Override
    public HttpTransport create() {
      return HTTP_TRANSPORT;
    }
  }

  private CloudSpannerOAuthUtil() {}

  public static GoogleCredentials getCredentialsFromFile(String credentialsPath) {
    if (credentialsPath == null || credentialsPath.length() == 0)
      throw new IllegalArgumentException("credentialsPath may not be null or empty");
    GoogleCredentials credentials = null;
    File credentialsFile = new File(credentialsPath);
    if (!credentialsFile.isFile()) {
      throw new RuntimeException(
          String.format("Error reading credential file %s: File does not exist", credentialsPath));
    }
    try (InputStream credentialsStream = new FileInputStream(credentialsFile)) {
      credentials = GoogleCredentials.fromStream(credentialsStream,
          CloudSpannerOAuthUtil.HTTP_TRANSPORT_FACTORY);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return credentials;
  }

  public static String getServiceAccountProjectId(String credentialsPath) {
    String project = null;
    if (credentialsPath != null) {
      try (InputStream credentialsStream = new FileInputStream(credentialsPath)) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        IOUtils.copy(credentialsStream, output);
        JSONObject json = new JSONObject(new JSONTokener(output.toString("UTF-8")));
        project = json.getString("project_id");
      } catch (IOException | JSONException ex) {
        // ignore
      }
    }
    return project;
  }

}
