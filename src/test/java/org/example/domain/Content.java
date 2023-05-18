package org.example.domain;

import io.ebean.annotation.DocMapping;
import io.ebean.annotation.DocProperty;
import io.ebean.annotation.DocSortable;
import io.ebean.annotation.DocStore;

import javax.persistence.Entity;
import javax.validation.constraints.Size;

@DocStore(mapping = {
    @DocMapping(name = "shortNotes",
        options = @DocProperty(includeInAll = false,
            options = DocProperty.Option.POSITIONS, store = true,
            analyzer = "english", searchAnalyzer = "english", copyTo = "other", norms = false))
})
@Entity
public class Content extends BaseUuidDomain {

  public enum Status {
    NEW,
    PUBLISHED
  }

  @DocProperty(store = true)
  Status status = Status.NEW;

  @Size(max = 100)
  String title;

  @DocSortable()
  @DocProperty(sortable = true, store = true)
  @Size(max = 100)
  String author;

  @Size(max = 1000)
  String content;

  @Size(max = 1000)
  String shortNotes;

  @Size(max = 10)
  String other;

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

  public String getShortNotes() {
    return shortNotes;
  }

  public void setShortNotes(String shortNotes) {
    this.shortNotes = shortNotes;
  }
}
