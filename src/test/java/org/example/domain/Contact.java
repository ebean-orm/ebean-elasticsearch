package org.example.domain;

import com.avaje.ebean.annotation.CacheStrategy;
import com.avaje.ebean.annotation.DocEmbedded;
import com.avaje.ebean.annotation.DocStore;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import java.util.List;

@DocStore
@Entity
@CacheStrategy(naturalKey = "email")
public class Contact extends BasicDomain {

  String firstName;
  String lastName;

  String phone;
  String mobile;
  String email;

  @DocEmbedded(doc = "id,name")
  @ManyToOne(optional = false)
  Customer customer;

  @ManyToOne(optional = true)
  ContactGroup group;

  @OneToMany
  List<ContactNote> notes;

  public Contact(String firstName, String lastName) {
    this.firstName = firstName;
    this.lastName = lastName;
  }

  public Contact() {
  }

  public String toString() {
    return "contact:"+id+" "+firstName+" "+lastName;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getPhone() {
    return phone;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public String getMobile() {
    return mobile;
  }

  public void setMobile(String mobile) {
    this.mobile = mobile;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public Customer getCustomer() {
    return customer;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  public ContactGroup getGroup() {
    return group;
  }

  public void setGroup(ContactGroup group) {
    this.group = group;
  }

  public List<ContactNote> getNotes() {
    return notes;
  }

  public void setNotes(List<ContactNote> notes) {
    this.notes = notes;
  }
}
