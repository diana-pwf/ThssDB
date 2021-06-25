package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }

  public Row(Row row){
    this.entries = new ArrayList<>();
    for(Entry e : row.getEntries()){
      this.entries.add(new Entry(e.value));
    }
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null)
      return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries)
      sj.add(e.toString());
    return sj.toString();
  }

  public ArrayList<String> toStringList() {
    if (entries == null)
      return new ArrayList<>();
    ArrayList<String> resultList = new ArrayList<>();
    for (Entry e : entries)
      resultList.add(e.toString());
    return resultList;
  }
}
