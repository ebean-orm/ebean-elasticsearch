package org.example.domain;

import com.avaje.ebean.annotation.DocCode;
import com.avaje.ebean.annotation.DocMapping;
import com.avaje.ebean.annotation.DocStore;
import com.avaje.ebean.annotation.DocStoreEmbedded;
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
//(doc = "*,details(*,product(id,name,sku)")//, customer(id,name)
@DocStore(
    mapping = {
        @DocMapping(name = "status", code = true),
        @DocMapping(name = "customer.name", sortable = true)
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
  @DocStoreEmbedded(doc = "+lazy,id,status,name,billingAddress(*,country(*)")
  Customer customer;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "order")
  @DocStoreEmbedded(doc = "*,product(id,sku,+lazy)")
  List<OrderDetail> details;

  //@DocStoreEmbedded(doc = "*")
  @OneToMany(cascade = CascadeType.ALL, mappedBy = "order")
  List<OrderShipment> shipments;

  public Order() {

  }

  public String toString() {
    return "id:"+id;
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
