package org.example.domain;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * Order Detail entity bean.
 */
@Entity
@Table(name = "o_order_detail")
public class OrderDetail extends BasicDomain {

  @ManyToOne(optional = false)
  Order order;

  Integer orderQty;

  Integer shipQty;

  Double unitPrice;

  @ManyToOne
  Product product;

  public OrderDetail() {
  }

  public OrderDetail(Product product, Integer orderQty, Double unitPrice) {
    this.product = product;
    this.orderQty = orderQty;
    this.unitPrice = unitPrice;
  }

  public Order getOrder() {
    return order;
  }

  public void setOrder(Order order) {
    this.order = order;
  }

  public Integer getOrderQty() {
    return orderQty;
  }

  public void setOrderQty(Integer orderQty) {
    this.orderQty = orderQty;
  }

  public Integer getShipQty() {
    return shipQty;
  }

  public void setShipQty(Integer shipQty) {
    this.shipQty = shipQty;
  }

  public Double getUnitPrice() {
    return unitPrice;
  }

  public void setUnitPrice(Double unitPrice) {
    this.unitPrice = unitPrice;
  }

  public Product getProduct() {
    return product;
  }

  public void setProduct(Product product) {
    this.product = product;
  }
}
