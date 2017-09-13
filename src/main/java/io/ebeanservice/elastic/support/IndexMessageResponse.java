package io.ebeanservice.elastic.support;

/**
 * Wrapper for a response code and body.
 */
public class IndexMessageResponse {

  private final int code;

  private final String body;

  public IndexMessageResponse(int code, String body) {
    this.code = code;
    this.body = body;
  }

  public int getCode() {
    return code;
  }

  public String getBody() {
    return body;
  }
}
