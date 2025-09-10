package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.Map;

public class ODBSizeStatement extends OSimpleExecStatement {

  public boolean indexes = false;
  public boolean classes = false;
  public boolean clusters = false;

  public ODBSizeStatement(int id) {
    super(id);
  }

  public ODBSizeStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    toGenericStatement(builder);
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("DB SIZE");
    if (classes) {
      builder.append(" -classes");
    }
    if (indexes) {
      builder.append(" -indexes");
    }
    if (clusters) {
      builder.append(" -clusters");
    }
  }

  @Override
  public OResultSet executeSimple(OCommandContext ctx) {
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    final OStorage storage = database.getStorage();

    final OInternalResultSet rs = new OInternalResultSet();
    try {
      long databaseDiskSize = 0;
      long databaseRecordSize = 0;

      for (OClass clazz : database.getMetadata().getImmutableSchemaSnapshot().getClasses()) {
        long classDiskSize = 0;
        long classRecordSize = 0;
        for (OIndex index : clazz.getIndexes()) {
          long diskSize = index.getInternal().getFileSize();
          classDiskSize += diskSize;
          if (indexes) {
            addRow(
                rs,
                clazz.getName(),
                index.getName(),
                "index",
                0,
                diskSize,
                index.getInternal().size());
          }
        }
        for (int id : clazz.getClusterIds()) {
          if (id != -1) {
            long clusterDiskSize = storage.getClusterFileSizeById(id);
            long clusterRecordSize = storage.getClusterRecordsSizeById(id);
            classDiskSize += clusterDiskSize;
            classRecordSize += clusterRecordSize;
            if (clusters) {
              addRow(
                  rs,
                  clazz.getName(),
                  database.getClusterNameById(id),
                  "cluster",
                  clusterRecordSize,
                  clusterDiskSize,
                  database.countClusterElements(id));
            }
          }
        }
        databaseDiskSize += classDiskSize;
        databaseRecordSize += classRecordSize;
        if (classes) {
          addRow(
              rs,
              clazz.getName(),
              clazz.getName(),
              "class",
              classRecordSize,
              classDiskSize,
              database.countClass(clazz.getName()));
        }
      }

      addRow(
          rs,
          null,
          database.getName(),
          "database",
          storage.getSize(),
          databaseDiskSize,
          storage.countRecords());
      return rs;
    } catch (Exception x) {
      throw OException.wrapException(new OCommandExecutionException("Cannot execute HA STATUS"), x);
    }
  }

  private static void addRow(
      OInternalResultSet rs,
      String clazz,
      String name,
      String type,
      long recordSize,
      long fileSize,
      long count) {
    OResultInternal row = new OResultInternal();

    row.setProperty("class", clazz);
    row.setProperty("name", name);
    row.setProperty("type", type);
    row.setProperty("record_size", recordSize);
    row.setProperty("file_size", fileSize);
    row.setProperty("count", count);

    rs.add(row);
  }
}
/* JavaCC - OriginalChecksum=c8ab1b0172e8cdbea2078efe2c629e6a (do not edit this line) */
