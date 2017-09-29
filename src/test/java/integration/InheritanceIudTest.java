package integration;

import org.example.domain.TruckRef;
import org.example.domain.Vehicle;
import org.example.domain.VehicleTruck;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class InheritanceIudTest extends BaseTest {

  VehicleTruck found;

  @Test
  public void insert() {

    VehicleTruck truck = new VehicleTruck();
    truck.setCapacity(30.0);
    truck.setLicenseNumber("T30");
    truck.save();

    sleepToPropagate();

    found = (VehicleTruck)server.find(Vehicle.class)
        .setUseDocStore(true)
        .where().eq("licenseNumber", "T30")
        .findOne();

    assertNotNull(found);
  }

  @Test(dependsOnMethods = "insert")
  public void update() {

    found.setCapacity(31.0);
    found.save();

    sleepToPropagate();

    VehicleTruck found = findTruck();

    assertNotNull(found);
    assertEquals(found.getCapacity(), 31.0);
  }


  @Test(dependsOnMethods = "update")
  public void delete() {

    Vehicle toDelete = findTruck();
    toDelete.delete();

    sleepToPropagate();

    VehicleTruck notFound = findTruck();
    assertNull(notFound);
  }


  /**
   * FIXME: Inheritance effectively broken with ES 5.x
   */
  @Test(enabled = false)
  public void update_assocOne() {

    TruckRef truckRef = server.find(TruckRef.class, 1);
    truckRef.setSomething("other");
    server.save(truckRef);

    sleepToPropagate();

    VehicleTruck truck = server.find(VehicleTruck.class)
        .where().eq("truckRef.id", 1)
        .setUseDocStore(true)
        .findOne();

    assertEquals(truck.getTruckRef().getSomething(), "other");
  }


  private VehicleTruck findTruck() {
    return (VehicleTruck)server.find(Vehicle.class)
        .setUseDocStore(true)
        .where().eq("licenseNumber", "T30")
        .findOne();
  }
}
