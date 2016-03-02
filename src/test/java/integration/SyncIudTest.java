package integration;

import com.avaje.ebean.Ebean;
import org.example.domain.Country;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

@Test
public class SyncIudTest extends BaseTest {

  public void insert() {

    Country country = new Country();
    country.setCode("SA");
    country.setName("South Africa");
    country.save();

    // give time to propagate
    sleepToPropagate(1500);

    Country sa = fetchSaFromDocStore();
    assertNotNull(sa);
  }

  @Test(dependsOnMethods = "insert")
  public void update() {

    Country sa = fetchSaFromDocStore();
    sa.setName("Sud Africa");
    sa.save();

    // give time to propagate
    sleepToPropagate(1100);

    Country confirm = fetchSaFromDocStore();
    assertEquals(confirm.getName(), sa.getName());
  }



  @Test(dependsOnMethods = "update")
  public void delete() {

    Ebean.delete(Country.class, "SA");
    // give time to propagate
    sleepToPropagate(1100);

    Country confirm = fetchSaFromDocStore();

    assertNull(confirm);
  }

  private Country fetchSaFromDocStore() {
    return Ebean.find(Country.class)
        .where().idEq("SA")
        .setUseCache(false)
        .setUseDocStore(true)
        .findUnique();
  }
}
