/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf.framework.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.jms.perf.framework.data.Benchmark;
import org.jboss.test.messaging.jms.perf.framework.data.Execution;
import org.jboss.test.messaging.jms.perf.framework.data.Measurement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class JDBCPersistenceManager implements PersistenceManager
{
   private static final Logger log = Logger.getLogger(JDBCPersistenceManager.class);
   
   
   protected Connection conn;
   
   protected String dbURL;

   public Benchmark getBenchmark(String name)
   {

      try
      {
         PreparedStatement ps = conn.prepareStatement("SELECT ID, NAME FROM BENCHMARK WHERE NAME=?");
         ps.setString(1, name);
         ResultSet rs = ps.executeQuery();
         long id;
         Benchmark bm = null;
         if (rs.next())
         {
            id = rs.getLong(1);
            bm = new Benchmark(name);
         }
         else
         {
            //log.error("Cannot find benchmark with name: " + name);
            return null;
         }
         rs.close();
         ps.close();
         
         ps = conn.prepareStatement("SELECT ID, RUN_DATE, PROVIDER FROM EXECUTION WHERE BENCHMARK_ID=?");
         ps.setLong(1, id);
         rs = ps.executeQuery();
         while (rs.next())
         {
            long executionId = rs.getLong(1);
            Timestamp timeStamp = rs.getTimestamp(2);
            Date runDate = new Date(timeStamp.getTime());
            String provider = rs.getString(3);
            Execution ex = new Execution(bm, runDate, provider);            
            
            PreparedStatement ps2 = conn.prepareStatement("SELECT ID, NAME, VALUE FROM MEASUREMENT WHERE EXECUTION_ID=?");
            ps2.setLong(1, executionId);
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next())
            {
               long measurementId = rs2.getLong(1);
               String measurementName = rs2.getString(2);
               double measurementValue = rs2.getDouble(3);
               Measurement measurement = new Measurement(measurementName, measurementValue);
               
               ex.addMeasurement(measurement);
               
               PreparedStatement ps3 = conn.prepareStatement("SELECT NAME, VALUE FROM VARIABLE WHERE MEASUREMENT_ID=?");
               ps3.setLong(1, measurementId);
               ResultSet rs3 = ps3.executeQuery();
               while (rs3.next())
               {
                  String variableName = rs3.getString(1);
                  double variableValue = rs3.getDouble(2);
                  measurement.setVariableValue(variableName, variableValue);
               }
               rs3.close();
               ps3.close();
               
            }
            rs2.close();
            ps2.close();
         }
         rs.close();
         ps.close();
         return bm;
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
      
      try
      {
      
         ps = conn.prepareStatement("SELECT ID, NAME FROM BENCHMARK WHERE NAME=?");
         ps.setString(1, exec.getBenchmark().getName());
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
            //benchmark does not exist
            
            rs.close();
            ps.close();
            
            ps = conn.prepareStatement("INSERT INTO BENCHMARK (NAME) VALUES (?)");
            ps.setString(1, exec.getBenchmark().getName());
            ps.executeUpdate();
            ps.close();
            
            ps = conn.prepareStatement("SELECT MAX(ID) FROM BENCHMARK");
            rs = ps.executeQuery();
            rs.next();
            id = rs.getLong(1);
            rs.close();
            ps.close();

         }
         
         ps = conn.prepareStatement("INSERT INTO EXECUTION (BENCHMARK_ID, RUN_DATE, PROVIDER) VALUES (?,?,?)");
         ps.setLong(1, id);
         ps.setTimestamp(2, new java.sql.Timestamp(exec.getDate().getTime()));
         ps.setString(3, exec.getProvider());
         ps.executeUpdate();
         ps.close();
         
         ps = conn.prepareStatement("SELECT MAX(ID) FROM EXECUTION");
         rs = ps.executeQuery();
         rs.next();
         long executionId = rs.getLong(1);
         rs.close();
         ps.close();
         
         Iterator iter = exec.getMeasurements().iterator();
         
         while (iter.hasNext())
         {
            Measurement m = (Measurement)iter.next();
            ps = conn.prepareStatement("INSERT INTO MEASUREMENT (EXECUTION_ID, NAME, VALUE) VALUES (?,?,?)");
            ps.setLong(1, executionId);
            ps.setString(2, m.getName());
            ps.setDouble(3, m.getValue().doubleValue());
            ps.executeUpdate();
            ps.close();
            
            ps = conn.prepareStatement("SELECT MAX(ID) FROM MEASUREMENT");
            rs = ps.executeQuery();
            rs.next();
            long measurementId = rs.getLong(1);
            rs.close();
            ps.close();
            
            Iterator iter2 = m.getVariables().entrySet().iterator();
            while (iter2.hasNext())
            {
               Map.Entry entry = (Map.Entry)iter2.next();
               String variableName = (String)entry.getKey();
               Double variableValue = (Double)entry.getValue();
               
               ps = conn.prepareStatement("INSERT INTO VARIABLE (MEASUREMENT_ID, NAME, VALUE) VALUES (?,?,?)");
               ps.setLong(1, measurementId);
               ps.setString(2, variableName);
               ps.setDouble(3, variableValue.doubleValue());
               ps.executeUpdate();
               ps.close();
               
            }
         }
         conn.commit();
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
   
   public JDBCPersistenceManager(String dbURL)
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
         conn.close();
      }
      catch (SQLException e)
      {
         log.error("Failed to close db connection", e);
      }
   }
   
   public void deleteAllResults()
   {
      try
      {
         conn.createStatement().executeQuery("DELETE FROM VARIABLE");
         conn.createStatement().executeQuery("DELETE FROM MEASUREMENT");
         conn.createStatement().executeQuery("DELETE FROM EXECUTION");
         conn.createStatement().executeQuery("DELETE FROM BENCHMARK");
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
         String sql = "CREATE TABLE BENCHMARK (ID IDENTITY, NAME VARCHAR)";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         
      }
      
      try
      {
         String sql = "CREATE TABLE EXECUTION (ID IDENTITY, BENCHMARK_ID INTEGER, RUN_DATE TIMESTAMP, PROVIDER VARCHAR, FOREIGN KEY (BENCHMARK_ID) REFERENCES BENCHMARK (ID))";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         
      }
      
      try
      {
         String sql = "CREATE TABLE MEASUREMENT (ID IDENTITY, EXECUTION_ID INTEGER, NAME VARCHAR, VALUE DOUBLE, FOREIGN KEY (EXECUTION_ID) REFERENCES EXECUTION (ID))";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         
      }
      
      try
      {
         String sql = "CREATE TABLE VARIABLE (ID IDENTITY, MEASUREMENT_ID INTEGER, NAME VARCHAR, VALUE DOUBLE, FOREIGN KEY (MEASUREMENT_ID) REFERENCES MEASUREMENT (ID))";
         
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         
      }
   }
   
   
   
   
   
}
