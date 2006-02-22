/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;

import org.jboss.jms.perf.framework.data.Datapoint;
import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.Measurement;
import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.logging.Logger;

/**
 * 
 * A HSQLDBPersistenceManager.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class HSQLDBPersistenceManager implements PersistenceManager
{
   private static final Logger log = Logger.getLogger(HSQLDBPersistenceManager.class);
      
   protected Connection conn;
   
   protected String dbURL;

   public PerformanceTest getPerformanceTest(String name)
   {
      try
      {
         PreparedStatement ps = conn.prepareStatement("SELECT ID, NAME FROM PERFORMANCE_TEST WHERE NAME=?");
         ps.setString(1, name);
         ResultSet rs = ps.executeQuery();
         long id;
         PerformanceTest pt = null;
         if (rs.next())
         {
            id = rs.getLong(1);
            pt = new PerformanceTest(name);
         }
         else
         {
            return null;
         }
         rs.close();
         ps.close();
         
         ps = conn.prepareStatement("SELECT ID, RUN_DATE, PROVIDER FROM EXECUTION WHERE PERFORMANCE_TEST_ID=?");
         ps.setLong(1, id);
         rs = ps.executeQuery();
         while (rs.next())
         {
            long executionId = rs.getLong(1);
            Timestamp timeStamp = rs.getTimestamp(2);
            Date runDate = new Date(timeStamp.getTime());
            String provider = rs.getString(3);
            Execution ex = new Execution(pt, runDate, provider);
            log.info("Found execution");
            
            PreparedStatement ps2 = conn.prepareStatement("SELECT ID, NAME FROM DATAPOINT WHERE EXECUTION_ID=?");
            ps2.setLong(1, executionId);
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next())
            {
               long datapointId = rs2.getLong(1);
               String datapointName = rs2.getString(2);
               Datapoint datapoint = new Datapoint(datapointName);
               
               ex.addDatapoint(datapoint);
               
               PreparedStatement ps3 = conn.prepareStatement("SELECT DIMENSION_NAME, VALUE FROM MEASUREMENT WHERE DATAPOINT_ID=?");
               ps3.setLong(1, datapointId);
               ResultSet rs3 = ps3.executeQuery();
               while (rs3.next())
               {
                  String dimensionName = rs3.getString(1);
                  double value = rs3.getDouble(2);
                  Measurement measurement = new Measurement(dimensionName, new Double(value));
                  datapoint.addMeasurement(measurement);
               }
               rs3.close();
               ps3.close();
               
            }
            rs2.close();
            ps2.close();
         }
         rs.close();
         ps.close();
         return pt;
      }
      catch (SQLException e)
      {
         log.error("Failed to get benchmark", e);
         return null;
      }    
   }
   
   public void saveExecution(Execution exec)
   {
      PreparedStatement ps = null;
      ResultSet rs = null;
      
      log.info("Saving execution");
      
      try
      {      
         ps = conn.prepareStatement("SELECT ID, NAME FROM PERFORMANCE_TEST WHERE NAME=?");
         ps.setString(1, exec.getPerformanceTest().getName());
         rs = ps.executeQuery();
         long id;      
         if (rs.next())
         {
            id = rs.getLong(1);
            rs.close();
            ps.close();
         }
         else
         {
            //performance test does not exist
            
            log.info("New perf test");
            
            rs.close();
            ps.close();
            
            ps = conn.prepareStatement("INSERT INTO PERFORMANCE_TEST (NAME) VALUES (?)");
            ps.setString(1, exec.getPerformanceTest().getName());
            ps.executeUpdate();
            ps.close();
            
            ps = conn.prepareStatement("CALL IDENTITY()");
            rs = ps.executeQuery();
            rs.next();
            id = rs.getLong(1);
            rs.close();
            ps.close();
            
            log.info("Identity is" + id);

         }
         
         ps = conn.prepareStatement("INSERT INTO EXECUTION (PERFORMANCE_TEST_ID, RUN_DATE, PROVIDER) VALUES (?,?,?)");
         ps.setLong(1, id);
         ps.setTimestamp(2, new java.sql.Timestamp(exec.getDate().getTime()));
         ps.setString(3, exec.getProviderName());
         ps.executeUpdate();
         ps.close();
         
         ps = conn.prepareStatement("CALL IDENTITY()");
         rs = ps.executeQuery();
         rs.next();
         long executionId = rs.getLong(1);
         rs.close();
         ps.close();
         
         Iterator iter = exec.getDatapoints().iterator();
         
         while (iter.hasNext())
         {
            Datapoint m = (Datapoint)iter.next();
            ps = conn.prepareStatement("INSERT INTO DATAPOINT (EXECUTION_ID, NAME) VALUES (?,?)");
            ps.setLong(1, executionId);
            ps.setString(2, m.getName());
            ps.executeUpdate();
            ps.close();
            
            ps = conn.prepareStatement("CALL IDENTITY()");
            rs = ps.executeQuery();
            rs.next();
            long datapointId = rs.getLong(1);
            rs.close();
            ps.close();
            
            Iterator iter2 = m.getMeasurements().iterator();
            while (iter2.hasNext())
            {
               Measurement measurement = (Measurement)iter2.next();
                              
               ps = conn.prepareStatement("INSERT INTO MEASUREMENT (DATAPOINT_ID, DIMENSION_NAME, VALUE) VALUES (?,?,?)");
               ps.setLong(1, datapointId);
               ps.setString(2, measurement.getDimensionName());
               ps.setDouble(3, ((Double)measurement.getValue()).doubleValue());
               ps.executeUpdate();
               ps.close();
               
            }
         }
         conn.commit();
         
         log.info("Saved execution");
      }
      catch (SQLException e)
      {
         log.error("Failed to save execution", e);
      }
      finally
      {
         try
         {
            if (rs != null) rs.close();
            if (ps != null) ps.close();
         }
         catch (SQLException e)
         {
            log.error("Failed to close resources", e);
         }
      }
   }
   
   public HSQLDBPersistenceManager(String dbURL)
   {
      this.dbURL = dbURL;
   }
   
   public void start()
   {
      try
      {
         Class.forName("org.hsqldb.jdbcDriver");
         conn = DriverManager.getConnection(dbURL, "sa", "");
      }
      catch (Exception e)
      {
         log.error("Failed to get db connection", e);
      }
      createSchema();
   }
   
   public void stop()
   {
      try
      {                  
         Statement statement = conn.createStatement();
                  
         statement.executeQuery("SHUTDOWN COMPACT");
         
         conn.close();         
      }
      catch (SQLException e)
      {
         log.error("Failed to shutdown db", e);
      }
   }
   
   public void deleteAllResults()
   {
      try
      {
         conn.createStatement().executeQuery("DELETE FROM MEASUREMENT");
         conn.createStatement().executeQuery("DELETE FROM DATAPOINT");
         conn.createStatement().executeQuery("DELETE FROM EXECUTION");
         conn.createStatement().executeQuery("DELETE FROM PERFORMANCE_TEST");
         conn.commit();
      }
      catch (SQLException e)
      {
         log.error("Failed to close db connection", e);
      }
      
   }
   
   public void createSchema()
   { 
      try
      {
         String sql = "CREATE TABLE PERFORMANCE_TEST (ID IDENTITY, NAME VARCHAR)";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         log.debug("Failed to create table", e);
      }
      
      try
      {
         String sql = "CREATE TABLE EXECUTION (ID IDENTITY, PERFORMANCE_TEST_ID INTEGER, RUN_DATE TIMESTAMP, PROVIDER VARCHAR, FOREIGN KEY (PERFORMANCE_TEST_ID) REFERENCES PERFORMANCE_TEST (ID))";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         log.debug("Failed to create table", e);
      }
      
      try
      {
         String sql = "CREATE TABLE DATAPOINT (ID IDENTITY, EXECUTION_ID INTEGER, NAME VARCHAR, FOREIGN KEY (EXECUTION_ID) REFERENCES EXECUTION (ID))";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         log.debug("Failed to create table", e);
      }
      
      try
      {
         String sql = "CREATE TABLE MEASUREMENT (ID IDENTITY, DATAPOINT_ID INTEGER, DIMENSION_NAME VARCHAR, VALUE DOUBLE, FOREIGN KEY (DATAPOINT_ID) REFERENCES DATAPOINT (ID))";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         log.debug("Failed to create table", e);
      }
      
      log.info("Created tables");
   }
 
}
