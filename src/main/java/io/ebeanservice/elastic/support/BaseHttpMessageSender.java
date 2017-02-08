package io.ebeanservice.elastic.support;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Basic implementation for sending the JSON payload to the ElasticSearch Bulk API.
 */
public class BaseHttpMessageSender implements IndexMessageSender {

  public static final Logger logger = LoggerFactory.getLogger("org.avaje.ebean.ELQ");

  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  public static final MediaType TEXT = MediaType.parse("text/plain; charset=utf-8");

  private final OkHttpClient client;

  private final String baseUrl;

  private final String bulkUrl;

  public BaseHttpMessageSender(String baseUrl, boolean allowAllCertificates) {
    this.baseUrl = normaliseBaseUrl(baseUrl);
    this.bulkUrl = deriveBulkUrl(this.baseUrl);
    this.client = OkClientBuilder.build(allowAllCertificates);
  }

  protected String normaliseBaseUrl(String baseUrl) {
    if (baseUrl == null) return null;
    return baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
  }

  /**
   * Return the Bulk API URL given the base URL.
   */
  protected String deriveBulkUrl(String baseUrl) {

    if (baseUrl == null) return null;
    return baseUrl + "_bulk";
  }

  @Override
  public void indexAlias(String aliasJson) throws IOException {

    String url = baseUrl + "_aliases";
    Response response = postJson(url, aliasJson);
    String responseBody = responseDebug("POST", url, response);

    int code = response.code();
    if (code != 200) {
      throw new IOException("Unexpected http code:" + code + " for indexAlias " + aliasJson + " response:" + responseBody);
    }
  }

  @Override
  public void indexSettings(String indexName, String settingsJson) throws IOException {

    String url = baseUrl + indexName + "/_settings";
    Response response = putJson(true, url, settingsJson);
    String responseBody = responseDebug("POST", url, response);

    int code = response.code();
    if (code != 200) {
      throw new IOException("Unexpected http code:" + code + " for _settings " + settingsJson + " response:" + responseBody);
    }
  }

  @Override
  public boolean indexExists(String indexName) throws IOException {

    String url = baseUrl + indexName;
    Request request = new Request.Builder()
        .url(url)
        .head().build();

    Response response = client.newCall(request).execute();

    int code = response.code();
    switch (code) {
      case 200:
        return true;
      case 404:
        return false;
      default:
        throw new IOException("Unexpected http code " + code);
    }
  }

  @Override
  public boolean indexDelete(String indexName) throws IOException {

    String url = baseUrl + indexName;
    Request request = new Request.Builder().url(url).delete().build();

    Response response = client.newCall(request).execute();
    responseDebug("DELETE", url, response);

    int code = response.code();
    switch (code) {
      case 200:
        return true;
      case 404:
        return false;
      default:
        throw new IOException("Unexpected http code:" + code + " for indexDelete " + indexName);
    }
  }


  @Override
  public void indexCreate(String indexName, String settingsJson) throws IOException {

    String url = baseUrl + indexName;
    Response response = putJson(true, url, settingsJson);
    String responseBody = responseDebug("PUT", url, response);

    int code = response.code();
    if (code != 200) {
      throw new IOException("Unexpected http code:" + code + " for indexDelete " + indexName + " response:" + responseBody);
    }
  }

  @Override
  public IndexMessageResponse postQuery(boolean scroll, String indexNameType, String jsonQuery) throws IOException {

    String scrollSuffix = (scroll) ? "?scroll=1m" : "";
    String url = baseUrl + indexNameType + "/_search" + scrollSuffix;

    Response response = postJson(url, jsonQuery);
    String responseBody = responseDebug("POST", url, response);

    return new IndexMessageResponse(response.code(), responseBody);
  }


  @Override
  public IndexMessageResponse getScroll(String scrollId) throws IOException {

    String url = baseUrl + "_search/scroll";

    String jsonQuery = "{\"scroll\":\"1m\",\"scroll_id\":\"" + scrollId + "\"}";

    Response response = postJson(url, jsonQuery);
    String responseBody = responseDebug("POST", url, response);

    return new IndexMessageResponse(response.code(), responseBody);
  }

  @Override
  public IndexMessageResponse clearScrollIds(Set<String> scrollIds) throws IOException {

    String url = baseUrl + "/_search/scroll";

    StringBuilder sb = new StringBuilder(200);
    for (String scrollId : scrollIds) {
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(scrollId);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("clearScrollIds: {}", sb.toString());
    }

    Request request = new Request.Builder()
        .url(url)
        .delete(RequestBody.create(TEXT, sb.toString()))
        .build();

    Response response = client.newCall(request).execute();
    String responseBody = responseDebug("DELETE", url, response);
    return new IndexMessageResponse(response.code(), responseBody);
  }

  @Override
  public IndexMessageResponse getDocSource(String indexNameType, String docId) throws IOException {

    String url = baseUrl + indexNameType + "/" + docId + "/_source";

    Request request = new Request.Builder().url(url).get().build();
    if (logger.isDebugEnabled()) {
      logger.debug("getDocSource: {}", url);
    }

    Response response = client.newCall(request).execute();
    String responseBody = responseDebug("GET", url, response);
    return new IndexMessageResponse(response.code(), responseBody);
  }

  @Override
  public String postBulk(String json) throws IOException {

    Response response = postJson(false, bulkUrl, json);
    return response.body().string();
  }

  private Response putJson(boolean debug, String url, String json) throws IOException {

    if (debug && logger.isDebugEnabled()) {
      logger.debug("PUT url:{} json:{}", url, json);
    }

    RequestBody body = RequestBody.create(JSON, json);
    Request request = new Request.Builder().url(url)
        .put(body)
        .build();

    return client.newCall(request).execute();
  }

  private Response postJson(String url, String json) throws IOException {
    return postJson(true, url, json);
  }

  private Response postJson(boolean debug, String url, String json) throws IOException {

    if (debug && logger.isDebugEnabled()) {
      logger.debug("POST url:{} json:{}", url, json);
    }

    RequestBody body = RequestBody.create(JSON, json);
    Request request = new Request.Builder().url(url)
        .post(body)
        .build();

    return client.newCall(request).execute();
  }

  private String responseDebug(String method, String url, Response response) throws IOException {
    String responseBody = response.body().string();
    if (logger.isDebugEnabled()) {
      logger.debug("{} url:{} response: {}", method, url, responseBody);
    }
    return responseBody;
  }
}
