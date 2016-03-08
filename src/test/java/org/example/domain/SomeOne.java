package org.example.domain;

import javax.persistence.Entity;
import javax.persistence.Inheritance;

@Entity
@Inheritance
public class SomeOne extends SomeTop {

  String level;

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }
}
