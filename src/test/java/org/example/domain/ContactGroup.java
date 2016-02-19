package org.example.domain;

import javax.persistence.Entity;

@Entity
public class ContactGroup extends BasicDomain {

  String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
