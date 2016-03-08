package org.example.domain;

import com.avaje.ebean.annotation.DocSortable;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;

@Entity
@Inheritance
@DiscriminatorValue("C")
public class VehicleCar extends Vehicle {

  @DocSortable
  String driver;

  String notes;

  public String getDriver() {
    return driver;
  }

  public String getNotes() {
    return notes;
  }

  public void setNotes(String notes) {
    this.notes = notes;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }
}

