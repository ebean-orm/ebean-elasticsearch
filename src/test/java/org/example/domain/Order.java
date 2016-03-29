package org.example.domain;

import com.avaje.ebean.annotation.DocCode;
import com.avaje.ebean.annotation.DocEmbedded;
import com.avaje.ebean.annotation.DocMapping;
import com.avaje.ebean.annotation.DocProperty;
import com.avaje.ebean.annotation.DocStore;
import com.avaje.ebean.annotation.EnumValue;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.sql.Date;
import java.util.List;

/**
 * Order entity bean.
 */
@DocStore(
    mapping = {
        @DocMapping(name = "status", options = @DocProperty(code = true)),
        @DocMapping(name = "customer.name", options = @DocProperty(sortable = true))
    }
)

@Entity
@Table(name = "o_order")
public class Order extends BasicDomain {

  public enum Status {
    @EnumValue("NEW")
    NEW,

    @EnumValue("APP")
    APPROVED,

    @EnumValue("SHP")
    SHIPPED,

    @EnumValue("COM")
    COMPLETE
  }

  @DocCode
  Status status;

  Date orderDate;

  Date shipDate;

  @NotNull
  @ManyToOne
  @DocEmbedded(doc = "id,status,name,billingAddress(*,country(*)")
  Customer customer;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "order")
  @DocEmbedded(doc = "*,product(id,sku)")
  List<OrderDetail> details;

  //@DocStoreEmbedded(doc = "*")
  @OneToMany(cascade = CascadeType.ALL, mappedBy = "order")
  List<OrderShipment> shipments;

  public Order() {

  }

  public String toString() {
    return "order id:" + id;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public Date getOrderDate() {
    return orderDate;
  }

  public void setOrderDate(Date orderDate) {
    this.orderDate = orderDate;
  }

  public Date getShipDate() {
    return shipDate;
  }

  public void setShipDate(Date shipDate) {
    this.shipDate = shipDate;
  }

  public Customer getCustomer() {
    return customer;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  public List<OrderDetail> getDetails() {
    return details;
  }

  public void setDetails(List<OrderDetail> details) {
    this.details = details;
  }

  public List<OrderShipment> getShipments() {
    return shipments;
  }

  public void setShipments(List<OrderShipment> shipments) {
    this.shipments = shipments;
  }
}
