package integration;

import org.example.domain.Content;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class UidTest extends BaseTest {

  Content content = new Content();

  @Test
  public void insert() {

    content.setAuthor("Magento");
    content.setTitle("How to integration Elastic Search");
    content.setContent("Some good JSON support is needed");
    content.save();

    sleepToPropagate();

    Content found = server.find(Content.class)
        .setId(content.getId())
        .setUseDocStore(true)
        .findUnique();

    assertEquals(found.getAuthor(), content.getAuthor());
    assertEquals(found.getContent(), content.getContent());
  }

  @Test(dependsOnMethods = "insert")
  public void find() {

    List<Content> newContent = server.find(Content.class)
        .where().eq("status", Content.Status.NEW)
        .setUseDocStore(true)
        .findList();

    assertThat(newContent).extracting("id").contains(content.getId());
  }
}
