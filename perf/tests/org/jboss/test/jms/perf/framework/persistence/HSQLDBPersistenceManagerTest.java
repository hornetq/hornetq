/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.jms.perf.framework.persistence;

import org.jboss.test.jms.perf.PerformanceFrameworkTestCase;
import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.persistence.HSQLDBPersistenceManager;
import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.Job;
import org.jboss.jms.perf.framework.ThroughputResult;
import org.jboss.jms.perf.framework.SenderJob;
import org.jboss.jms.perf.framework.DrainJob;
import org.jboss.jms.perf.framework.ReceiverJob;
import org.jboss.jms.perf.framework.FillJob;
import org.jboss.jms.perf.framework.Failure;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class HSQLDBPersistenceManagerTest extends PerformanceFrameworkTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(HSQLDBPersistenceManagerTest.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String databaseURL = "jdbc:hsqldb:mem:mydatabase";
   protected HSQLDBPersistenceManager pm;

   // Constructors --------------------------------------------------

   public HSQLDBPersistenceManagerTest(String name)
   {
      super(name);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testPerformanceTestStorage() throws Exception
   {
      PerformanceTest t = new PerformanceTest(null, "One");
      pm.savePerformanceTest(t);

      PerformanceTest tl = pm.getPerformanceTest("One");

      assertEquals("One", tl.getName());
      assertTrue(tl.getExecutions().isEmpty());
   }

   public void testComplexPerformanceTestStorage() throws Exception
   {
      PerformanceTest t;

      t = new PerformanceTest(null, "One");
      pm.savePerformanceTest(t);

      t = new PerformanceTest(null, "Two");

      Execution e = new Execution("JBossMessaging");
      t.addExecution(e);

      Job j = null;
      ThroughputResult r = null;
      List l = null;

      j = new SenderJob();
      j.setMessageCount(1);
      j.setMessageSize(2);
      j.setDuration(3);
      j.setRate(4);
      r = new ThroughputResult(5, 6);
      r.setJob(j);

      e.addMeasurement(r);

      l = new ArrayList();

      j = new SenderJob();
      j.setMessageCount(7);
      j.setMessageSize(8);
      j.setDuration(9);
      j.setRate(10);
      r = new ThroughputResult(11, 12);
      r.setJob(j);
      l.add(r);

      j = new SenderJob();
      j.setMessageCount(13);
      j.setMessageSize(14);
      j.setDuration(15);
      j.setRate(16);
      r = new ThroughputResult(17, 18);
      r.setJob(j);
      l.add(r);

      j = new SenderJob();
      j.setMessageCount(19);
      j.setMessageSize(20);
      j.setDuration(21);
      j.setRate(22);
      r = new ThroughputResult(23, 24);
      r.setJob(j);
      l.add(r);

      e.addMeasurement(l);

      j = new DrainJob();
      j.setMessageCount(25);
      j.setMessageSize(26);
      j.setDuration(27);
      j.setRate(28);
      r = new ThroughputResult(29, 30);
      r.setJob(j);

      e.addMeasurement(r);

      e = new Execution("ActiveMQ");

      t.addExecution(e);

      e = new Execution("JBossMQ");

      t.addExecution(e);

      l = new ArrayList();

      j = new ReceiverJob();
      j.setMessageCount(31);
      j.setMessageSize(32);
      j.setDuration(33);
      j.setRate(34);
      r = new ThroughputResult(35, 36);
      r.setJob(j);
      l.add(r);

      j = new SenderJob();
      j.setMessageCount(37);
      j.setMessageSize(38);
      j.setDuration(39);
      j.setRate(40);
      r = new ThroughputResult(41, 42);
      r.setJob(j);
      l.add(r);

      j = new DrainJob();
      j.setMessageCount(43);
      j.setMessageSize(44);
      j.setDuration(45);
      j.setRate(46);
      r = new ThroughputResult(47, 48);
      r.setJob(j);
      l.add(r);

      j = new FillJob();
      j.setMessageCount(49);
      j.setMessageSize(50);
      j.setDuration(51);
      j.setRate(52);
      r = new ThroughputResult(53, 54);
      r.setJob(j);
      l.add(r);

      e.addMeasurement(l);

      j = new ReceiverJob();
      j.setMessageCount(55);
      j.setMessageSize(56);
      j.setDuration(57);
      j.setRate(58);
      r = new ThroughputResult(59, 60);
      r.setJob(j);

      e.addMeasurement(r);

      j = new SenderJob();
      j.setMessageCount(61);
      j.setMessageSize(62);
      j.setDuration(63);
      j.setRate(64);
      r = new ThroughputResult(65, 66);
      r.setJob(j);

      e.addMeasurement(r);


      j = new DrainJob();
      j.setMessageCount(67);
      j.setMessageSize(68);
      j.setDuration(69);
      j.setRate(70);
      r = new ThroughputResult(71, 72);
      r.setJob(j);

      e.addMeasurement(r);

      j = new FillJob();
      j.setMessageCount(73);
      j.setMessageSize(74);
      j.setDuration(75);
      j.setRate(76);
      r = new ThroughputResult(77, 78);
      r.setJob(j);

      e.addMeasurement(r);

      pm.savePerformanceTest(t);


      // retrieve and check data

      PerformanceTest pt = pm.getPerformanceTest("One");
      assertEquals("One", pt.getName());
      assertTrue(pt.getExecutions().isEmpty());

      PerformanceTest pt2 = pm.getPerformanceTest("Two");
      assertEquals("Two", pt2.getName());

      List executions = pt2.getExecutions();
      assertEquals(3, executions.size());

      // JBossMessaging execution

      Execution execution = null;

      for(Iterator i = executions.iterator(); i.hasNext(); )
      {
         Execution ex = (Execution)i.next();
         if ("JBossMessaging".equals(ex.getProviderName()))
         {
            execution = ex;
            break;
         }
      }

      Iterator measurments = execution.iterator();

      List list = (List)measurments.next();
      assertContains(list, SenderJob.TYPE, 1, 2, 3, 4, 5, 6);
      assertEquals(0, list.size());


      list = (List)measurments.next();
      assertEquals(3, list.size());
      assertContains(list, SenderJob.TYPE, 7, 8, 9, 10, 11, 12);
      assertContains(list, SenderJob.TYPE, 13, 14, 15, 16, 17, 18);
      assertContains(list, SenderJob.TYPE, 19, 20, 21, 22, 23, 24);
      assertEquals(0, list.size());

      list = (List)measurments.next();
      assertContains(list, DrainJob.TYPE, 25, 26, 27, 28, 29, 30);
      assertEquals(0, list.size());

      assertFalse(measurments.hasNext());

      // ActiveMQ execution

      for(Iterator i = executions.iterator(); i.hasNext(); )
      {
         Execution ex = (Execution)i.next();
         if ("ActiveMQ".equals(ex.getProviderName()))
         {
            execution = ex;
            break;
         }
      }

      measurments = execution.iterator();
      assertFalse(measurments.hasNext());

      // JBossMQ excution

      for(Iterator i = executions.iterator(); i.hasNext(); )
      {
         Execution ex = (Execution)i.next();
         if ("JBossMQ".equals(ex.getProviderName()))
         {
            execution = ex;
            break;
         }
      }

      measurments = execution.iterator();

      list = (List)measurments.next();
      assertEquals(4, list.size());
      assertContains(list, ReceiverJob.TYPE, 31, 32, 33, 34, 35, 36);
      assertContains(list, SenderJob.TYPE, 37, 38, 39, 40, 41, 42);
      assertContains(list, DrainJob.TYPE, 43, 44, 45, 46, 47, 48);
      assertContains(list, FillJob.TYPE, 49, 50, 51, 52, 53, 54);
      assertEquals(0, list.size());

      list = (List)measurments.next();
      assertEquals(1, list.size());
      assertContains(list, ReceiverJob.TYPE, 55, 56, 57, 58, 59, 60);
      assertEquals(0, list.size());

      list = (List)measurments.next();
      assertEquals(1, list.size());
      assertContains(list, SenderJob.TYPE, 61, 62, 63, 64, 65, 66);
      assertEquals(0, list.size());

      list = (List)measurments.next();
      assertEquals(1, list.size());
      assertContains(list, DrainJob.TYPE, 67, 68, 69, 70, 71, 72);
      assertEquals(0, list.size());

      list = (List)measurments.next();
      assertEquals(1, list.size());
      assertContains(list, FillJob.TYPE, 73, 74, 75, 76, 77, 78);
      assertEquals(0, list.size());

      assertFalse(measurments.hasNext());

   }

   public void testFailureStorage() throws Exception
   {
      PerformanceTest t = new PerformanceTest(null, "One");
      Execution e = new Execution("someexecution");
      t.addExecution(e);

      Job j = new SenderJob();
      ThroughputResult r = new Failure();
      r.setJob(j);
      e.addMeasurement(r);

      pm.savePerformanceTest(t);

      // retrieve and check data

      PerformanceTest pt = pm.getPerformanceTest("One");
      List executions = pt.getExecutions();
      assertEquals(1, executions.size());
      Execution execution = (Execution)executions.get(0);
      Iterator measurments = execution.iterator();

      List list = (List)measurments.next();
      assertEquals(1, list.size());
      Failure f = (Failure)list.get(0);
      assertEquals(SenderJob.TYPE, f.getJob().getType());

      assertFalse(measurments.hasNext());
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      pm = new HSQLDBPersistenceManager(databaseURL);
      pm.start();
      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      pm.stop();
      pm = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   private static void assertContains(List list,
                                      String jobType,
                                      int messageCount,
                                      int messageSize,
                                      long duration,
                                      int rate,
                                      long time,
                                      long messages)
   {
      for(Iterator i = list.iterator(); i.hasNext(); )
      {
         ThroughputResult tr = (ThroughputResult)i.next();
         Job job = tr.getJob();

         if (jobType.equals(job.getType()) &&
            messageCount == job.getMessageCount() &&
            messageSize == job.getMessageSize() &&
            duration == job.getDuration() &&
            rate == job.getRate() &&
            time == tr.getTime() &&
            messages == tr.getMessages())
         {
            // found
            i.remove();
            return;
         }
      }
      fail("The list doesn't contain specified element!");
   }

   // Inner classes -------------------------------------------------
}
