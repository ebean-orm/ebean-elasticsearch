package org.example.domain;

import javax.persistence.Entity;
import javax.persistence.Inheritance;

@Entity
@Inheritance
public class SomeTwo extends SomeTop {

  long score;

  public long getScore() {
    return score;
  }

  public void setScore(long score) {
    this.score = score;
  }
}
