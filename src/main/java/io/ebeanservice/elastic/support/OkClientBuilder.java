package io.ebeanservice.elastic.support;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

class OkClientBuilder {

  /**
   * Build a OkHttpClient potentially allowing all SSL certificates.
   */
  static OkHttpClient build(boolean allowAllCertificates, String username, String pwd) {

    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    if (username != null) {
      builder.authenticator((route, response) -> {
        String basicAuth = Credentials.basic(username, pwd);
        Request request = response.request();
        request.newBuilder().addHeader("Authorization", basicAuth).build();
        return request;
      });
    }

    if (!allowAllCertificates) {
      return builder.build();
    }

    try {
      SSLContext sslContext = SSLContext.getInstance("SSL");

      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
      SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
      builder.sslSocketFactory(sslSocketFactory, trustAllX09);
      builder.hostnameVerifier((s, sslSession) -> true);
      return builder.build();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final X509TrustManager trustAllX09 = new X509TrustManager() {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[]{};
    }
  };

  private static final TrustManager[] trustAllCerts = new TrustManager[] { trustAllX09 };


}
