package org.example.domain;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.validation.constraints.Size;

/**
 * Address entity bean.
 */
@Entity
@Table(name = "o_address")
public class Address extends BasicDomain {

  @Size(max = 100)
  String line1;

  @Size(max = 100)
  String line2;

  @Size(max = 100)
  String city;

  @ManyToOne
  Country country;


  public String toString() {
    return id + " " + line1 + " " + line2 + " " + city + " " + country;
  }

  public String getLine1() {
    return line1;
  }

  public void setLine1(String line1) {
    this.line1 = line1;
  }

  public String getLine2() {
    return line2;
  }

  public void setLine2(String line2) {
    this.line2 = line2;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public Country getCountry() {
    return country;
  }

  public void setCountry(Country country) {
    this.country = country;
  }
}
