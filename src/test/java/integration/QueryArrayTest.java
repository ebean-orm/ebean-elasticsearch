package integration;

import io.ebean.Query;
import org.example.domain.Contact;
import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class QueryArrayTest extends BaseTest {

  @Test
  public void arrayIsNotEmpty() {

    Query<Contact> query = server.find(Contact.class)
        .where()
        .arrayIsNotEmpty("uids")
        .setUseDocStore(true);

    query.findList();
    assertThat(query.getGeneratedSql()).contains("{\"query\":{\"filtered\":{\"filter\":{\"exists\":{\"field\":\"uids\"}}}}}");
  }

  @Test
  public void arrayIsEmpty() {

    Query<Contact> query = server.find(Contact.class)
        .where()
        .arrayIsEmpty("uids")
        .setUseDocStore(true);

    query.findList();
    assertThat(query.getGeneratedSql()).contains("{\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"uids\"}}]}}}}}");
  }

  @Test
  public void arrayContains() {

    Query<Contact> query = server.find(Contact.class)
        .where()
        .arrayContains("someTags", "red")
        .setUseDocStore(true);

    query.findList();
    assertThat(query.getGeneratedSql()).contains("{\"query\":{\"filtered\":{\"filter\":{\"term\":{\"someTags\":\"red\"}}}}}");
  }

  @Test
  public void arrayContains_many() {

    Query<Contact> query = server.find(Contact.class)
        .where()
        .arrayContains("someTags", "red", "green")
        .setUseDocStore(true);

    query.findList();
    assertThat(query.getGeneratedSql()).contains("{\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[{\"term\":{\"someTags\":\"red\"}},{\"term\":{\"someTags\":\"green\"}}]}}}}}");
  }

  @Test
  public void arrayNotContains_many() {

    Query<Contact> query = server.find(Contact.class)
        .where()
        .arrayNotContains("tags", "red", "green")
        .setUseDocStore(true);

    query.findList();
    assertThat(query.getGeneratedSql()).contains("{\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must_not\":[{\"term\":{\"tags\":\"red\"}},{\"term\":{\"tags\":\"green\"}}]}}}}}");
  }

  @Test
  public void arrayNotContains_zippy() {

    Query<Contact> query = server.find(Contact.class)
        .where()
        .arrayNotContains("tags", "red", "green", "zippy")
        .setUseDocStore(true);

    query.findList();
    assertThat(query.getGeneratedSql()).contains("{\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must_not\":[{\"term\":{\"tags\":\"red\"}},{\"term\":{\"tags\":\"green\"}},{\"term\":{\"tags\":\"zippy\"}}]}}}}}");
  }
}
