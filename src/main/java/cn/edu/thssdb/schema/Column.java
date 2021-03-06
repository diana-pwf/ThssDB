package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  public String getName() { return name; }

  public ColumnType getType(){
    return type;
  }

  public void setPrimary(){
    primary = 1;
    notNull = true;
  }

  public boolean isPrimary(){ return primary == 1; }

  public boolean isNotNull(){ return notNull == true; }

  public int getMaxLength() { return maxLength; }


  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }
}
