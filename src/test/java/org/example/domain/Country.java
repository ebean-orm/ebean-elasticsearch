package org.example.domain;

import com.avaje.ebean.annotation.CacheStrategy;
import com.avaje.ebean.annotation.CacheTuning;
import com.avaje.ebean.annotation.DocStore;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.Size;

/**
 * Country entity bean.
 */
@DocStore
@CacheStrategy(readOnly = true, warmingQuery = "order by name")
@CacheTuning(maxSize = 500)
@Entity
@Table(name = "o_country")
public class Country {

  @Id
  @Size(max = 2)
  String code;

  @Size(max = 60)
  String name;

  public String toString() {
    return code;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
