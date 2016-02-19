package org.example.domain;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.sql.Timestamp;

@Entity
@Table(name = "or_order_ship")
public class OrderShipment extends BasicDomain {

  @ManyToOne
  private Order order;

  private Timestamp shipTime = new Timestamp(System.currentTimeMillis());

  public Timestamp getShipTime() {
    return shipTime;
  }

  public void setShipTime(Timestamp shipTime) {
    this.shipTime = shipTime;
  }

  public Order getOrder() {
    return order;
  }

  public void setOrder(Order order) {
    this.order = order;
  }

}
