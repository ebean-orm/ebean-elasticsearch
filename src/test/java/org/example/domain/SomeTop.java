package org.example.domain;

import javax.persistence.Entity;
import javax.persistence.Inheritance;
import java.sql.Date;

@Entity
@Inheritance
public abstract class SomeTop extends BasicDomain {

  String name;

  Date date;

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public String getName() {

    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
