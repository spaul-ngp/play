package com.psl.core.assimilator.file;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.psl.core.assimilator.ConnectionConfig;
import com.psl.sc.exception.HdfsConnectionException;
import com.psl.sc.exception.manager.ExceptionManager;
import com.psl.sc.exception.type.ExceptionSource;
import com.psl.sc.exception.type.ExceptionType;

/**
 * This class is responsible to perform the processing of files.
 * 
 * @author saju_paul, dipika_khera
 * @project assimilator
 * @date Jul 15, 2016
 * @since 1.0
 */
@Component
public class FileProcessor {

  private static final String HADOOP_JOB_UGI = "hadoop.job.ugi";
  private static final String FS_DEFAULT_PATH_PREFIX = "fs.defaultFS";
  private static final int REPLICATION_FACTOR = 1;
  boolean importStatus = false;

  private static final Logger LOGGER = LogManager.getLogger(FileProcessor.class.getName());

  @Autowired
  private ConnectionConfig connectionConfig;
  private Configuration config;

  public FileProcessor() {

  }

  /**
   * @param connectionConfig
   */
  public FileProcessor(ConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
    initialize(connectionConfig);
  }

  /**
   * Initialize HDFS configurations.
   * 
   * @param connConfig An instance of {@link ConnectionConfig}
   */
  public void initialize(ConnectionConfig connConfig) {
    config = new Configuration();
    config.set(FS_DEFAULT_PATH_PREFIX, connConfig.getFileSystemPathPrefix());
    config.set(HADOOP_JOB_UGI, connConfig.getJobUserName());
  }

  /**
   * Upload source file on HDFS directory. Established a secure connection with HDFS database &
   * internally calls copyFileToHdfs method.
   * 
   * @param srcfilePath An absolute path of source file
   * @param dstFilePath Target directory path
   * @return Status of uploading file on HDFS
   * @throws IOException If an input/output exception occurs
   */
  public boolean importFileToHDFS(final String srcfilePath, final String dstFilePath)
      throws IOException {
    LOGGER
        .log(Level.INFO, String.format("Moving file : %s to path : %s", srcfilePath, dstFilePath));
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(connectionConfig.getJobUserName());
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Path file = new Path(dstFilePath);
          copyFileToHdfs(srcfilePath, file);
          return null;
        }

      });
    } catch (InterruptedException e) {
      ExceptionManager.manageException(ExceptionSource.HDFS, ExceptionType.ENV_ACCESS_ERR, e);
    }
    return importStatus;
  }

  /**
   * Create HDFS connection & call copy method to write file on HDFS.
   * 
   * @param srcfilePath An absolute path of source file
   * @param file Target file
   * @return Status of uploading file on HDFS
   * @throws IOException If an input/output exception occurs
   */
  private boolean copyFileToHdfs(String srcfilePath, Path file) throws IOException {
    try (FileSystem hdfs = FileSystem.get(config);
        FileInputStream fs = new FileInputStream(srcfilePath);
        FSDataOutputStream outputStream = hdfs.create(file, (short) REPLICATION_FACTOR);
        InputStream inputStream = new BufferedInputStream(fs);) {

      copy(outputStream, inputStream);
      importStatus = true;
    } catch (ConnectException | AccessControlException | RemoteException e) {
      LOGGER.log(
          Level.FATAL,
          String.format("Failed to connect Hadoop server : %s",
              connectionConfig.getFileSystemPathPrefix()), e);
      importStatus = false;
      ExceptionManager.manageException(
          ExceptionSource.HDFS,
          ExceptionType.HDFS_NOT_AVAILABLE,
          new HdfsConnectionException("Failed to connect Hadoop server"
              + connectionConfig.getFileSystemPathPrefix(), e));
    }
    return importStatus;

  }

  /**
   * Write file from source to target location.
   * 
   * @param outputStream Destination stream
   * @param inputStream Source stream
   * @throws IOException If an input/output exception occurs
   */
  private void copy(FSDataOutputStream outputStream, InputStream inputStream) throws IOException {
    final int len = 4096;
    byte[] buf = new byte[4096];
    int offset = 0, bytesRead = 0;

    do {
      bytesRead = inputStream.read(buf, offset, len);
      outputStream.write(buf);
      Arrays.fill(buf, (byte) 0);
    } while (bytesRead == len);
    outputStream.flush();
  }
}
