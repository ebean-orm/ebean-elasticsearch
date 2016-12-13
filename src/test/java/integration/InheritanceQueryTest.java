package integration;

import io.ebean.Query;
import org.example.domain.Vehicle;
import org.example.domain.VehicleCar;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class InheritanceQueryTest extends BaseTest {

  @Test
  public void findAll() {

    Query<Vehicle> query = server.find(Vehicle.class)
        .setUseDocStore(true);

    List<Vehicle> list = query.findList();

    assertEquals(query.getGeneratedSql(), "{\"query\":{\"match_all\":{}}}");
    assertThat(list.size()).isGreaterThan(4);
  }

  @Test
  public void where_licenseNumberEq() {

    Query<Vehicle> query = server.find(Vehicle.class)
        .setUseDocStore(true)
        .where().eq("licenseNumber", "T42")
        .query();

    List<Vehicle> list = query.findList();

    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"term\":{\"licenseNumber\":\"T42\"}}}}}");
    assertEquals(list.size(), 1);
  }

  @Test
  public void where_dtypeEq() {

    Query<Vehicle> query = server.find(Vehicle.class)
        .setUseDocStore(true)
        .where().eq("dtype", "C")
        .query();

    List<Vehicle> list = query.findList();

    assertEquals(list.size(), 3);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"term\":{\"dtype\":\"C\"}}}}}");
  }

  @Test
  public void find_subType() {

    Query<VehicleCar> query = server.find(VehicleCar.class)
        .setUseDocStore(true)
        .order().desc("driver");

    List<VehicleCar> list = query.findList();

    assertEquals(list.size(), 3);
    assertEquals(query.getGeneratedSql(), "{\"sort\":[{\"driver.raw\":{\"order\":\"desc\"}}],\"query\":{\"filtered\":{\"filter\":{\"term\":{\"dtype\":\"C\"}}}}}");
  }

  @Test
  public void find_subType_when_hasPredicate() {

    Query<VehicleCar> query = server.find(VehicleCar.class)
        .setUseDocStore(true)
        .where().istartsWith("driver", "mari")
        .order().desc("driver");

    List<VehicleCar> list = query.findList();

    assertEquals(list.size(), 1);
    assertEquals(query.getGeneratedSql(), "{\"sort\":[{\"driver.raw\":{\"order\":\"desc\"}}],\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[{\"prefix\":{\"driver\":\"mari\"}},{\"term\":{\"dtype\":\"C\"}}]}}}}}");
  }

}
