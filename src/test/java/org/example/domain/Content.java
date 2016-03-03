package org.example.domain;

import com.avaje.ebean.annotation.DocStore;

import javax.persistence.Entity;
import javax.validation.constraints.Size;

@DocStore
@Entity
public class Content extends BaseUuidDomain {

  public enum Status {
    NEW,
    PUBLISHED
  }

  Status status = Status.NEW;

  @Size(max = 100)
  String title;

  @Size(max = 100)
  String author;

  @Size(max = 1000)
  String content;

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }
}
