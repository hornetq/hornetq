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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.ThroughputResult;
import org.jboss.jms.perf.framework.Job;
import org.jboss.jms.perf.framework.BaseJob;
import org.jboss.logging.Logger;

/**
 * 
 * A HSQLDBPersistenceManager.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class HSQLDBPersistenceManager implements PersistenceManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(HSQLDBPersistenceManager.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Connection conn;
   protected String dbURL;

   // Constructors --------------------------------------------------

   public HSQLDBPersistenceManager(String dbURL)
   {
      this.dbURL = dbURL;
   }

   // PersitenceManager implementation ------------------------------

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
         conn.createStatement().executeQuery("DELETE FROM EXECUTION");
         conn.createStatement().executeQuery("DELETE FROM PERFORMANCE_TEST");
         conn.commit();
      }
      catch (SQLException e)
      {
         log.error("Failed to close db connection", e);
      }
   }


   public void savePerformanceTest(PerformanceTest pt) throws Exception
   {
      PreparedStatement ps = null;
      ResultSet rs = null;

      try
      {
         ps = conn.prepareStatement("SELECT ID, NAME FROM PERFORMANCE_TEST WHERE NAME=?");

         ps.setString(1, pt.getName());
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
            // performance test does not exist

            log.debug("creating new performance test in database: " + pt);

            rs.close();
            ps.close();

            ps = conn.prepareStatement("INSERT INTO PERFORMANCE_TEST (NAME) VALUES (?)");
            ps.setString(1, pt.getName());
            ps.executeUpdate();
            ps.close();

            ps = conn.prepareStatement("CALL IDENTITY()");
            rs = ps.executeQuery();
            rs.next();
            id = rs.getLong(1);
            rs.close();
            ps.close();
         }

         for(Iterator i = pt.getExecutions().iterator(); i.hasNext(); )
         {
            Execution e = (Execution)i.next();

            ps = conn.prepareStatement("INSERT INTO EXECUTION (PERFORMANCE_TEST_ID, PROVIDER) VALUES (?,?)");
            ps.setLong(1, id);
//         ps.setTimestamp(2, new java.sql.Timestamp(exec.getDate().getTime()));
            ps.setString(2, e.getProviderName());
            ps.executeUpdate();
            ps.close();

            ps = conn.prepareStatement("CALL IDENTITY()");
            rs = ps.executeQuery();
            rs.next();
            long executionId = rs.getLong(1);
            rs.close();
            ps.close();

            int index = 0;
            for(Iterator j = e.iterator(); j.hasNext(); index++)
            {
               for(Iterator m = ((List)j.next()).iterator(); m.hasNext(); )
               {
                  ThroughputResult r = (ThroughputResult)m.next();
                  Job job = r.getJob();
                  ps = conn.prepareStatement("INSERT INTO MEASUREMENT (EXECUTION_ID, MEASUREMENT_INDEX, JOB_TYPE, MESSAGE_COUNT, MESSAGE_SIZE, DURATION, RATE, MEASURED_DURATION, MEASURED_MESSAGE_COUNT) VALUES (?,?,?,?,?,?,?,?,?)");
                  ps.setLong(1, executionId);
                  ps.setInt(2, index);
                  ps.setString(3, job.getType());
                  ps.setInt(4, job.getMessageCount());
                  ps.setInt(5, job.getMessageSize());
                  ps.setLong(6, job.getDuration());
                  ps.setInt(7, job.getRate());
                  ps.setLong(8, r.getTime());
                  ps.setLong(9, r.getMessages());
                  ps.executeUpdate();
                  ps.close();
               }
            }
         }
         conn.commit();

         log.debug("saved test");

      }
      catch (SQLException e)
      {
         log.error("Failed to save test", e);
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

   public List getPerformanceTestNames() throws Exception
   {
      List result = new ArrayList();
      try
      {
         PreparedStatement ps = conn.prepareStatement("SELECT NAME FROM PERFORMANCE_TEST");
         ResultSet rs = ps.executeQuery();
         while(rs.next())
         {
            String name = rs.getString(1);
            result.add(name);
         }
         rs.close();
         ps.close();
      }
      catch (SQLException e)
      {
         log.error("Failed to get benchmark", e);
         return null;
      }
      return result;
   }

   public PerformanceTest getPerformanceTest(String name)
   {
      try
      {
         PreparedStatement ps =
            conn.prepareStatement("SELECT ID, NAME FROM PERFORMANCE_TEST WHERE NAME=?");
         ps.setString(1, name);
         ResultSet rs = ps.executeQuery();
         long id;
         PerformanceTest pt = null;
         if (rs.next())
         {
            id = rs.getLong(1);
            pt = new PerformanceTest(null, name);
         }
         else
         {
            return null;
         }
         rs.close();
         ps.close();

         ps = conn.prepareStatement("SELECT ID, PROVIDER FROM EXECUTION WHERE PERFORMANCE_TEST_ID=?");
         ps.setLong(1, id);
         rs = ps.executeQuery();
         while (rs.next())
         {
            long executionID = rs.getLong(1);
            //Timestamp timeStamp = rs.getTimestamp(2);
            //Date runDate = new Date(timeStamp.getTime());
            String provider = rs.getString(2);
            Execution ex = new Execution(provider);
            pt.addExecution(ex);

            PreparedStatement ps2 = conn.prepareStatement("SELECT MEASUREMENT_INDEX, JOB_TYPE, MESSAGE_COUNT, MESSAGE_SIZE, DURATION, RATE, MEASURED_DURATION, MEASURED_MESSAGE_COUNT FROM MEASUREMENT WHERE EXECUTION_ID=?");
            ps2.setLong(1, executionID);
            ResultSet rs2 = ps2.executeQuery();

            Map measurements = new HashMap();

            while (rs2.next())
            {
               Integer index = new Integer(rs2.getInt(1));

               List l = (List)measurements.get(index);
               if (l == null)
               {
                  l = new ArrayList();
                  measurements.put(index, l);
               }

               String jobType = rs2.getString(2);
               int messageCount = rs2.getInt(3);
               int messageSize = rs2.getInt(4);
               long duration = rs2.getLong(5);
               int rate = rs2.getInt(6);
               long measuredDuration = rs2.getLong(7);
               long measuredMessageCount = rs2.getLong(8);

               Job j = BaseJob.create(jobType);
               j.setMessageCount(messageCount);
               j.setMessageSize(messageSize);
               j.setDuration(duration);
               j.setRate(rate);
               ThroughputResult r = new ThroughputResult(measuredDuration, measuredMessageCount);
               r.setJob(j);
               l.add(r);
            }

            rs2.close();
            ps2.close();

            List indices = new ArrayList(measurements.keySet());
            Collections.sort(indices);
            for(Iterator i = indices.iterator(); i.hasNext();)
            {
               Integer index = (Integer)i.next();
               List l = (List)measurements.get(index);
               ex.addMeasurement(l);
            }
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
         String sql = "CREATE TABLE EXECUTION (ID IDENTITY, PERFORMANCE_TEST_ID INTEGER, PROVIDER VARCHAR, FOREIGN KEY (PERFORMANCE_TEST_ID) REFERENCES PERFORMANCE_TEST (ID))";
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         log.debug("Failed to create table", e);
      }

      try
      {
         String sql = "CREATE TABLE MEASUREMENT (EXECUTION_ID INTEGER, MEASUREMENT_INDEX INTEGER, JOB_TYPE VARCHAR, MESSAGE_COUNT INTEGER, MESSAGE_SIZE INTEGER, DURATION BIGINT, RATE INTEGER, MEASURED_DURATION BIGINT, MEASURED_MESSAGE_COUNT BIGINT, FOREIGN KEY (EXECUTION_ID) REFERENCES EXECUTION (ID))";
         conn.createStatement().executeUpdate(sql);
      }
      catch (SQLException e)
      {
         log.debug("Failed to create table", e);
      }
      log.debug("created tables");
   }


   public void dump() throws Exception
   {
      System.out.println("");
      System.out.println("");

      for(Iterator i = getPerformanceTestNames().iterator(); i.hasNext(); )
      {
         String name = (String)i.next();
         PerformanceTest t = getPerformanceTest(name);
         System.out.println(t);

         for(Iterator j = t.getExecutions().iterator(); j.hasNext(); )
         {
            Execution e = (Execution)j.next();
            System.out.println(e);

            int index = 0;
            for(Iterator k = e.iterator(); k.hasNext(); index ++)
            {
               System.out.println("    " + index + " " + k.next());
            }
         }
      }
   }




   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------


}
