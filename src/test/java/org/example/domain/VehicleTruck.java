package org.example.domain;

import io.ebean.annotation.DocEmbedded;

import javax.persistence.CascadeType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.ManyToOne;

@Entity
@Inheritance
@DiscriminatorValue("T")
public class VehicleTruck extends Vehicle {

  Double capacity;

  @DocEmbedded(doc = "*")
  @ManyToOne(cascade = {CascadeType.PERSIST})
  TruckRef truckRef;

  public Double getCapacity() {
    return capacity;
  }

  public void setCapacity(Double capacity) {
    this.capacity = capacity;
  }

  public TruckRef getTruckRef() {
    return truckRef;
  }

  public void setTruckRef(TruckRef truckRef) {
    this.truckRef = truckRef;
  }
}
