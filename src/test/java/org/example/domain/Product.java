package org.example.domain;

import com.avaje.ebean.annotation.DocStore;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.validation.constraints.Size;

/**
 * Product entity bean.
 */
@DocStore
@Entity
@Table(name = "o_product")
public class Product extends BasicDomain {

  @Size(max = 20)
  String sku;

  String name;

  public String getSku() {
    return sku;
  }

  public void setSku(String sku) {
    this.sku = sku;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
