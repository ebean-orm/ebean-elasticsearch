package com.avaje.ebeanservice.elastic.search;

import com.avaje.ebean.PagedList;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * PagedList implementation with an already supplied total row count and list.
 */
public class HitsPagedList<T> implements PagedList<T> {

  private final int totalRowCount;

  private final List<T> list;

  private final int firstRow;

  private final int maxRows;

  private final int pageIndex;

  /**
   * Construct with firstRow/maxRows.
   */
  public HitsPagedList(int firstRow, int maxRows, List<T> list, long totalCount) {
    this.maxRows = maxRows;
    this.firstRow = firstRow;
    this.totalRowCount = totalCount > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)totalCount;
    this.list = list;
    this.pageIndex = 0;
  }

  public List<T> getList() {
    return list;
  }

  @Override
  public int getTotalCount() {
    return totalRowCount;
  }

  public int getTotalRowCount() {
    return totalRowCount;
  }

  public void loadCount() {
  }

  public void loadRowCount() {
  }

  public Future<Integer> getFutureCount() {
    return new FutInt<Integer>(this.totalRowCount);
  }

  public Future<Integer> getFutureRowCount() {
    return getFutureCount();
  }

  public int getTotalPageCount() {

    int rowCount = getTotalRowCount();
    if (rowCount == 0) {
      return 0;
    } else {
      return ((rowCount - 1) / maxRows) + 1;
    }
  }

  public boolean hasNext() {
    return (firstRow + maxRows) < getTotalRowCount();
  }

  public boolean hasPrev() {
    return firstRow > 0;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public int getPageSize() {
    return maxRows;
  }

  public String getDisplayXtoYofZ(String to, String of) {

    int first = firstRow + 1;
    int last = firstRow + list.size();
    int total = getTotalRowCount();

    return first + to + last + of + total;
  }

  private static class FutInt<V> implements Future<V> {

    private final V value;

    private FutInt(V value) {
      this.value = value;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return true;
    }

    @Override
    public boolean isCancelled() {
      return true;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return value;
    }
  }

}
