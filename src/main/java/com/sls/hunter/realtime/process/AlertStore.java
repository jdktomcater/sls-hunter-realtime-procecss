package com.sls.hunter.realtime.process;

import jakarta.annotation.PreDestroy;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class AlertStore {
  private final Connection db;
  private final FileChannel lockChannel;
  private final FileLock lock;
  AlertStore(AlertSettings config) throws Exception {
    Path path=Path.of(config.stateFile).toAbsolutePath();
    Files.createDirectories(path.getParent());
    lockChannel=FileChannel.open(Path.of(path+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
    lock=lockChannel.tryLock();
    if(lock==null) throw new IllegalStateException("State file is already in use");
    db=DriverManager.getConnection("jdbc:sqlite:"+path);
    try(var s=db.createStatement()) {
      s.execute("CREATE TABLE IF NOT EXISTS windows (family TEXT, window TEXT, revision INTEGER NOT NULL, PRIMARY KEY(family,window))");
      s.execute("CREATE TABLE IF NOT EXISTS active (family TEXT, window TEXT, signal TEXT, PRIMARY KEY(family,window,signal))");
      s.execute("CREATE TABLE IF NOT EXISTS delivered (family TEXT, window TEXT, revision INTEGER, signal TEXT, PRIMARY KEY(family,window,revision,signal))");
    }
  }
  long revision(String family,String window) throws SQLException {
    try(var s=db.prepareStatement("SELECT revision FROM windows WHERE family=? AND window=?")) { s.setString(1,family); s.setString(2,window); try(var r=s.executeQuery()) {return r.next()?r.getLong(1):0;} }
  }
  Set<String> active(String family,String window) throws SQLException {
    Set<String> ids=new HashSet<>();
    try(var s=db.prepareStatement("SELECT signal FROM active WHERE family=? AND window=?")) { s.setString(1,family); s.setString(2,window); try(var r=s.executeQuery()) {while(r.next()) ids.add(r.getString(1));} } return ids;
  }
  boolean delivered(String family,String window,long rev,String id) throws SQLException {
    try(var s=db.prepareStatement("SELECT 1 FROM delivered WHERE family=? AND window=? AND revision=? AND signal=?")) { key(s,family,window,rev,id); try(var r=s.executeQuery()) {return r.next();} }
  }
  void markDelivered(String family,String window,long rev,String id) throws SQLException {
    try(var s=db.prepareStatement("INSERT INTO delivered VALUES(?,?,?,?)")) {key(s,family,window,rev,id); s.executeUpdate();}
  }
  private static void key(PreparedStatement s,String family,String window,long rev,String id) throws SQLException {s.setString(1,family);s.setString(2,window);s.setLong(3,rev);s.setString(4,id);}
  void finish(String family,String window,long rev,Set<String> ids) throws SQLException {
    db.setAutoCommit(false);
    try {
      try(var s=db.prepareStatement("DELETE FROM active WHERE family=? AND window=?")) {s.setString(1,family);s.setString(2,window);s.executeUpdate();}
      try(var s=db.prepareStatement("INSERT INTO active VALUES(?,?,?)")) {for(String id:ids){s.setString(1,family);s.setString(2,window);s.setString(3,id);s.addBatch();} s.executeBatch();}
      try(var s=db.prepareStatement("INSERT OR REPLACE INTO windows VALUES(?,?,?)")) {s.setString(1,family);s.setString(2,window);s.setLong(3,rev);s.executeUpdate();}
      try(var s=db.prepareStatement("DELETE FROM delivered WHERE family=? AND window=?")) {s.setString(1,family);s.setString(2,window);s.executeUpdate();}
      db.commit();
    } catch(SQLException ex) {db.rollback();throw ex;} finally {db.setAutoCommit(true);}
  }
  @PreDestroy public void close() throws Exception {try {db.close();} finally {try {lock.release();} finally {lockChannel.close();}}}
}
