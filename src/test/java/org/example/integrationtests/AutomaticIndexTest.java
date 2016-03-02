package org.example.integrationtests;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import org.example.domain.Country;
import org.testng.annotations.Test;

import static org.assertj.core.api.StrictAssertions.assertThat;

@Test(enabled = false)
public class AutomaticIndexTest {

  public void index_country_when_inserted() throws InterruptedException {

    EbeanServer server = Ebean.getDefaultServer();

    Country country = new Country();
    country.setCode("JU");
    country.setName("Jumboland");

    server.save(country);

    Thread.sleep(2000);

    Country ju = server.docStore().getById(Country.class, "JU");
    assertThat(ju).isNotNull();
    assertThat(ju.getCode()).isEqualTo(country.getCode());
    assertThat(ju.getName()).isEqualTo(country.getName());
  }

  public void index_country_when_updated() throws InterruptedException {

    EbeanServer server = Ebean.getDefaultServer();

    Country country = new Country();
    country.setCode("KU");
    country.setName("Kumboland");

    server.save(country);

    Thread.sleep(100);

    country.setName("Kumbo");
    server.save(country);

    Thread.sleep(2000);

    Country ju = server.docStore().getById(Country.class, country.getCode());
    assertThat(ju).isNotNull();
    assertThat(ju.getCode()).isEqualTo(country.getCode());
    assertThat(ju.getName()).isEqualTo("Kumbo");
  }

  public void delete_country_when_deleted() throws InterruptedException {

    EbeanServer server = Ebean.getDefaultServer();

    Country country = new Country();
    country.setCode("LU");
    country.setName("Lumboland");

    server.save(country);

    Thread.sleep(100);

    server.delete(country);

    Thread.sleep(2000);

    Country ju = server.docStore().getById(Country.class, country.getCode());
    assertThat(ju).isNull();
  }

}
