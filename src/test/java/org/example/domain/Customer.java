package org.example.domain;

import com.avaje.ebean.annotation.DbEnumValue;
import com.avaje.ebean.annotation.DocEmbedded;
import com.avaje.ebean.annotation.DocSortable;
import com.avaje.ebean.annotation.DocStore;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * Customer entity bean.
 */
@DocStore
@Entity
@Table(name = "o_customer")
public class Customer extends BasicDomain {

  public enum Status {
    NEW("N"),
    ACTIVE("A"),
    INACTIVE("I");

    String dbValue;

    Status(String dbValue) {
      this.dbValue = dbValue;
    }

    @DbEnumValue
    public String getValue() {
      return dbValue;
    }
  }

  Status status;

  @DocSortable
  @NotNull
  @Size(max = 40)
  String name;

  @Size(max = 100)
  String smallNote;

  Date anniversary;

  @DocEmbedded(doc = "*,country(*)")
  @ManyToOne(cascade = CascadeType.ALL)
  Address billingAddress;

  @DocEmbedded(doc = "*,country(*)")
  @ManyToOne(cascade = CascadeType.ALL)
  Address shippingAddress;

  @OneToMany(mappedBy = "customer")
  List<Order> orders;

  @OneToMany(mappedBy = "customer", cascade = CascadeType.ALL)
  List<Contact> contacts;

  public String toString() {
    return id + " " + status + " " + name;
  }

  public void addContact(Contact contact) {
    if (contacts == null) {
      contacts = new ArrayList<Contact>();
    }
    contacts.add(contact);
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSmallNote() {
    return smallNote;
  }

  public void setSmallNote(String smallNote) {
    this.smallNote = smallNote;
  }

  public Date getAnniversary() {
    return anniversary;
  }

  public void setAnniversary(Date anniversary) {
    this.anniversary = anniversary;
  }

  public Address getBillingAddress() {
    return billingAddress;
  }

  public void setBillingAddress(Address billingAddress) {
    this.billingAddress = billingAddress;
  }

  public Address getShippingAddress() {
    return shippingAddress;
  }

  public void setShippingAddress(Address shippingAddress) {
    this.shippingAddress = shippingAddress;
  }

  public List<Order> getOrders() {
    return orders;
  }

  public void setOrders(List<Order> orders) {
    this.orders = orders;
  }

  public List<Contact> getContacts() {
    return contacts;
  }

  public void setContacts(List<Contact> contacts) {
    this.contacts = contacts;
  }
}
