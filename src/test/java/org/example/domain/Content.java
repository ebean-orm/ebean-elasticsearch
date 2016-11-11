package org.example.domain;

import com.avaje.ebean.annotation.DocMapping;
import com.avaje.ebean.annotation.DocProperty;
import com.avaje.ebean.annotation.DocSortable;
import com.avaje.ebean.annotation.DocStore;

import javax.persistence.Entity;
import javax.validation.constraints.Size;

@DocStore(mapping = {
    @DocMapping(name = "other", options = @DocProperty(enabled = false)),
    @DocMapping(name = "shortNotes",
        options = @DocProperty(boost = 1.5f, includeInAll = false,
            options = DocProperty.Option.POSITIONS, store = true,
            analyzer = "english", searchAnalyzer = "english", copyTo = "other", norms = false))
})
@Entity
public class Content extends BaseUuidDomain {

  public enum Status {
    NEW,
    PUBLISHED
  }

  Status status = Status.NEW;

  @Size(max = 100)
  String title;

  @DocSortable()
  @DocProperty(sortable = true, boost = 2f, store = true)
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
