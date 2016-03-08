package org.example.domain;

import com.avaje.ebean.annotation.DocCode;
import com.avaje.ebean.annotation.DocStore;

import javax.persistence.DiscriminatorColumn;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import java.sql.Date;

@DocStore
@Entity
@Inheritance
@DiscriminatorColumn(length = 3)
public abstract class Vehicle extends BasicDomain {

  @DocCode
  String licenseNumber;

  Date registrationDate;

  public String getLicenseNumber() {
    return licenseNumber;
  }

  public void setLicenseNumber(String licenseNumber) {
    this.licenseNumber = licenseNumber;
  }

  public Date getRegistrationDate() {
    return registrationDate;
  }

  public void setRegistrationDate(Date registrationDate) {
    this.registrationDate = registrationDate;
  }
}
