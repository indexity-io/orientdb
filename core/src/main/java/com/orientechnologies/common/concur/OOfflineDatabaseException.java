package com.orientechnologies.common.concur;

public class OOfflineDatabaseException extends OOfflineNodeException {
  public OOfflineDatabaseException(OOfflineDatabaseException exception) {
    super(exception);
  }

  public OOfflineDatabaseException(String message) {
    super(message);
  }
}
