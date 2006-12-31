/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.Valve;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.JBossQueueBrowser;
import org.jboss.jms.client.JBossMessageProducer;
import org.jboss.jms.client.JBossMessageConsumer;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.state.HierarchicalStateSupport;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.container.FailoverValveInterceptor;
import org.jboss.jms.server.Version;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.MethodInfo;
import org.jboss.aop.util.MethodHashing;
import org.jboss.aop.advice.Interceptor;
import org.jboss.remoting.Client;

import javax.naming.InitialContext;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Iterator;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.Slot;

/**
 * This is not necessarily a clustering test, as the failover valves are installed even for a
 * non-clustered configuration.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1843 $</tt>
 *
 * $Id: JMSTest.java 1843 2006-12-21 23:41:19Z timfox $
 */
public class FailoverValveTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InitialContext ic;
   private ConnectionFactory cf;
   private Queue queue;

   // Constructors ---------------------------------------------------------------------------------

   public FailoverValveTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testCloseValveHierarchy() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         assertTrue(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         assertTrue(((Valve)((JBossSession)session).getDelegate()).isValveOpen());

         MessageProducer prod = session.createProducer(queue);
         assertTrue(((Valve)((JBossMessageProducer)prod).getDelegate()).isValveOpen());

         MessageConsumer cons = session.createConsumer(queue);
         assertTrue(((Valve)((JBossMessageConsumer)cons).getDelegate()).isValveOpen());

         QueueBrowser browser = session.createBrowser(queue);
         assertTrue(((Valve)((JBossQueueBrowser)browser).getDelegate()).isValveOpen());

         ((JBossConnection)conn).getDelegate().closeValve();

         log.debug("top level valve closed");

         assertFalse(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossSession)session).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageProducer)prod).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageConsumer)cons).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossQueueBrowser)browser).getDelegate()).isValveOpen());

         ((JBossConnection)conn).getDelegate().openValve();

         log.debug("top level valve open");

         assertTrue(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossSession)session).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageProducer)prod).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageConsumer)cons).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossQueueBrowser)browser).getDelegate()).isValveOpen());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   public void testCloseValveHierarchy2() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         assertTrue(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());

         Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         assertTrue(((Valve)((JBossSession)session1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossSession)session2).getDelegate()).isValveOpen());

         MessageProducer prod1 = session1.createProducer(queue);
         assertTrue(((Valve)((JBossMessageProducer)prod1).getDelegate()).isValveOpen());

         MessageConsumer cons1 = session1.createConsumer(queue);
         assertTrue(((Valve)((JBossMessageConsumer)cons1).getDelegate()).isValveOpen());

         QueueBrowser browser1 = session1.createBrowser(queue);
         assertTrue(((Valve)((JBossQueueBrowser)browser1).getDelegate()).isValveOpen());

         MessageProducer prod2 = session2.createProducer(queue);
         assertTrue(((Valve)((JBossMessageProducer)prod2).getDelegate()).isValveOpen());

         MessageConsumer cons2 = session2.createConsumer(queue);
         assertTrue(((Valve)((JBossMessageConsumer)cons2).getDelegate()).isValveOpen());

         QueueBrowser browser2 = session2.createBrowser(queue);
         assertTrue(((Valve)((JBossQueueBrowser)browser2).getDelegate()).isValveOpen());

         ((JBossConnection)conn).getDelegate().closeValve();

         log.debug("top level valve closed");

         assertFalse(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossSession)session1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossSession)session2).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageProducer)prod1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageConsumer)cons1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossQueueBrowser)browser1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageProducer)prod2).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageConsumer)cons2).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossQueueBrowser)browser2).getDelegate()).isValveOpen());

         ((JBossConnection)conn).getDelegate().openValve();

         log.debug("top level valve open");

         assertTrue(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossSession)session1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossSession)session2).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageProducer)prod1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageConsumer)cons1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossQueueBrowser)browser1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageProducer)prod2).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageConsumer)cons2).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossQueueBrowser)browser2).getDelegate()).isValveOpen());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testValveOpenByDefault() throws Throwable
   {
      FailoverValveInterceptor valve = new FailoverValveInterceptor();
      SimpleInvocationTargetObject target = new SimpleInvocationTargetObject(valve);

      assertEquals(Boolean.TRUE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(0), valve.invoke(buildInvocation("getActiveThreadsCount", target)));

      // try to open the valve again, it should be a noop
      valve.invoke(buildInvocation("openValve", target));

      assertEquals(Boolean.TRUE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(0), valve.invoke(buildInvocation("getActiveThreadsCount", target)));
   }

   public void testPassThroughOpenValve() throws Throwable
   {
      final FailoverValveInterceptor valve = new FailoverValveInterceptor();
      final SimpleInvocationTargetObject target = new SimpleInvocationTargetObject(valve);

      // send a thread through the open valve
      Thread t = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.invoke(buildInvocation("businessMethod1", target));
            }
            catch(Throwable t)
            {
               log.error("businessMethod1 invocation failed", t);
            }
         }
      }, "Business Thread 1");

      t.start();
      t.join();

      // the thread should have passed through the open valve
      assertEquals(1, target.getBusinessMethod1InvocationCount());
      assertEquals(1, target.getActiveThreadsHighWaterMark());

   }

   public void testFlipValve() throws Throwable
   {
      FailoverValveInterceptor valve = new FailoverValveInterceptor();
      SimpleInvocationTargetObject target = new SimpleInvocationTargetObject(valve);

      valve.invoke(buildInvocation("closeValve", target));

      assertEquals(Boolean.FALSE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(0), valve.invoke(buildInvocation("getActiveThreadsCount", target)));

      // child states also must have been notified to close their corresponding valves
      ParentHierarchicalState state = (ParentHierarchicalState)target.getState();

      for(Iterator i = state.getChildren().iterator(); i.hasNext(); )
      {
         ChildState cs = (ChildState)i.next();
         assertFalse(((Valve)cs.getDelegate()).isValveOpen());
      }

      // try to close the valve again, it should be a noop

      valve.invoke(buildInvocation("closeValve", target));

      assertEquals(Boolean.FALSE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(0), valve.invoke(buildInvocation("getActiveThreadsCount", target)));
   }

   public void testFlipValve2() throws Throwable
   {
      FailoverValveInterceptor valve = new FailoverValveInterceptor();
      SimpleInvocationTargetObject target = new SimpleInvocationTargetObject(valve);

      valve.invoke(buildInvocation("closeValve", target));

      assertEquals(Boolean.FALSE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(0), valve.invoke(buildInvocation("getActiveThreadsCount", target)));

      // child states also must have been notified to close their corresponding valves
      ParentHierarchicalState state = (ParentHierarchicalState)target.getState();

      for(Iterator i = state.getChildren().iterator(); i.hasNext(); )
      {
         ChildState cs = (ChildState)i.next();
         assertFalse(((Valve)cs.getDelegate()).isValveOpen());
      }

      // re-open the valve

      valve.invoke(buildInvocation("openValve", target));

      assertEquals(Boolean.TRUE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(0), valve.invoke(buildInvocation("getActiveThreadsCount", target)));

      // child states also must have been notified to open their corresponding valves
      state = (ParentHierarchicalState)target.getState();

      for(Iterator i = state.getChildren().iterator(); i.hasNext(); )
      {
         ChildState cs = (ChildState)i.next();
         assertTrue(((Valve)cs.getDelegate()).isValveOpen());
      }
   }

   /**
    * Close the valve and send a thread through it. The thread must be put on hold until the valve
    * is opened again.
    */
   public void testCloseValveOneBusinessThread() throws Throwable
   {
      final FailoverValveInterceptor valve = new FailoverValveInterceptor();
      final SimpleInvocationTargetObject target = new SimpleInvocationTargetObject(valve);

      valve.invoke(buildInvocation("closeValve", target));

      assertEquals(Boolean.FALSE, valve.invoke(buildInvocation("isValveOpen", target)));

      log.debug("the valve is closed");

      // smack a thread into the closed valve
      Thread t = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.invoke(buildInvocation("businessMethod1", target));
            }
            catch(Throwable t)
            {
               log.error("businessMethod1 invocation failed", t);
            }
         }
      }, "Business Thread 1");

      t.start();

      // wait for 10 secs for the invocation to reach target object. It shouldn't ...
      long waitTime = 10000;
      log.info("probing target for " + waitTime / 1000 + " seconds ...");
      InvocationToken arrived = target.waitForInvocation(waitTime);

      if (arrived != null)
      {
         fail(arrived.getMethodName() + "() reached target, while it shouldn't have!");
      }


      log.debug("the business thread didn't go through");

      // open the valve

      valve.invoke(buildInvocation("openValve", target));

      // the business invocation should complete almost immediately; wait on mutex to avoid race
      // condition

      arrived = target.waitForInvocation(2000);

      assertEquals("businessMethod1", arrived.getMethodName());
      assertEquals(1, target.getBusinessMethod1InvocationCount());
      assertEquals(1, target.getActiveThreadsHighWaterMark());
   }


   /**
    * Close the valve and send three threads through it. The threads must be put on hold until the
    * valve is opened again.
    */
   public void testCloseValveThreeBusinessThread() throws Throwable
   {
      final FailoverValveInterceptor valve = new FailoverValveInterceptor();
      final SimpleInvocationTargetObject target = new SimpleInvocationTargetObject(valve);

      valve.invoke(buildInvocation("closeValve", target));

      assertEquals(Boolean.FALSE, valve.invoke(buildInvocation("isValveOpen", target)));

      log.debug("the valve is closed");

      // smack thread 1 into the closed valve
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.invoke(buildInvocation("businessMethod1", target));
            }
            catch(Throwable t)
            {
               log.error("businessMethod1 invocation failed", t);
            }
         }
      }, "Business Thread 1").start();

      // smack thread 2 into the closed valve
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.invoke(buildInvocation("businessMethod1", target));
            }
            catch(Throwable t)
            {
               log.error("businessMethod1 invocation failed", t);
            }
         }
      }, "Business Thread 2").start();

      // smack thread 3 into the closed valve
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.invoke(buildInvocation("businessMethod2", target));
            }
            catch(Throwable t)
            {
               log.error("businessMethod2 invocation failed", t);
            }
         }
      }, "Business Thread 3").start();

      // wait for 10 secs for any invocation to reach target object. It shouldn't ...
      long waitTime = 10000;
      log.info("probing target for " + waitTime / 1000 + " seconds ...");
      InvocationToken arrived = target.waitForInvocation(waitTime);

      if (arrived != null)
      {
         fail(arrived.getMethodName() + "() reached target, while it shouldn't have!");
      }

      log.debug("the business threads didn't go through");

      // open the valve

      valve.invoke(buildInvocation("openValve", target));

      // the business invocations should complete almost immediately; wait on mutex to avoid race
      // condition

      arrived = target.waitForInvocation(2000);
      assertNotNull(arrived);

      arrived = target.waitForInvocation(2000);
      assertNotNull(arrived);

      arrived = target.waitForInvocation(2000);
      assertNotNull(arrived);

      // wait for 3 secs for any invocation to reach target object. It shouldn't ...
      waitTime = 3000;
      log.info("probing target for " + waitTime / 1000 + " seconds ...");
      arrived = target.waitForInvocation(waitTime);

      if (arrived != null)
      {
         fail("Extra " + arrived.getMethodName() + "() reached target, " +
            "while it shouldn't have!");
      }

      assertEquals(2, target.getBusinessMethod1InvocationCount());
      assertEquals(1, target.getBusinessMethod2InvocationCount());
      assertEquals(3, target.getActiveThreadsHighWaterMark());
   }

   /**
    * The current standard behavior is that the valve cannot be closed as long as there are
    * active threads. closeValve() will block undefinitely.
    */
   public void testCloseWhileActiveThreads() throws Throwable
   {
      final FailoverValveInterceptor valve = new FailoverValveInterceptor();
      final SimpleInvocationTargetObject target = new SimpleInvocationTargetObject(valve);

      assertEquals(Boolean.TRUE, valve.invoke(buildInvocation("isValveOpen", target)));

      // send a long running thread through the valve
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.invoke(buildInvocation("blockingBusinessMethod", target));
            }
            catch(Throwable t)
            {
               log.error("blockingBusinessMethod invocation failed", t);
            }
         }
      }, "Long running business thread").start();

      // allow blockingBusinessMethod time to block
      Thread.sleep(2000);

      assertEquals(new Integer(1), valve.invoke(buildInvocation("getActiveThreadsCount", target)));

      final Slot closingCompleted = new Slot();

      // from a different thread try to close the valve; this thread will block until we unblock
      // the business method

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               valve.invoke(buildInvocation("closeValve", target));
               closingCompleted.put(Boolean.TRUE);

            }
            catch(Throwable t)
            {
               log.error("blockingBusinessMethod() invocation failed", t);
            }
         }
      }, "Valve closing thread").start();


      // closing shouldn't be completed for a long time .... actually never, if I don't unblock
      // the business method

      // wait for 15 secs for closing. It shouldn't ...
      long waitTime = 15000;
      log.info("probing closing for " + waitTime / 1000 + " seconds ...");

      Boolean closed = (Boolean)closingCompleted.poll(waitTime);

      if (closed != null)
      {
         fail("closeValve() went through, while it shouldn't have!");
      }

      assertEquals(Boolean.TRUE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(1), valve.invoke(buildInvocation("getActiveThreadsCount", target)));

      log.info("valve still open ...");

      // unblock blockingBusinessMethod
      target.unblockBlockingBusinessMethod();

      // valve closing should complete immediately after that
      closed = (Boolean)closingCompleted.poll(1000);
      assertTrue(closed.booleanValue());
      assertEquals(Boolean.FALSE, valve.invoke(buildInvocation("isValveOpen", target)));
      assertEquals(new Integer(0), valve.invoke(buildInvocation("getActiveThreadsCount", target)));
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("TestQueue");

      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      queue = (Queue)ic.lookup("/queue/TestQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("TestQueue");

      ic.close();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   private Invocation buildInvocation(String methodName, Object targetObject)
      throws Exception
   {
      long hash =
         MethodHashing.calculateHash(targetObject.getClass().getMethod(methodName, new Class[0]));
      MethodInfo mi = new MethodInfo(targetObject.getClass(), hash, hash, null);
      MethodInvocation invocation = new MethodInvocation(mi, new Interceptor[0]);
      invocation.setTargetObject(targetObject);
      return invocation;
   }

   // Inner classes --------------------------------------------------------------------------------

   public interface BusinessObject
   {
      void businessMethod1();
      void businessMethod2();
      /**
       * Must be unblocked externally.
       */
      void blockingBusinessMethod() throws InterruptedException;
   }

   public class SimpleInvocationTargetObject
      extends DelegateSupport implements BusinessObject, Valve
   {
      private int businessMethod1InvocationCount;
      private int businessMethod2InvocationCount;

      // LinkedQueue<InvocationToken>
      private LinkedQueue invocationTokens;

      private FailoverValveInterceptor valve;
      private int activeThreadsCountHighWaterMark;

      private Object blockingMethodWaitArea;

      public SimpleInvocationTargetObject(FailoverValveInterceptor valve)
      {
         super();
         setState(new ParentHierarchicalState());
         businessMethod1InvocationCount = 0;
         businessMethod2InvocationCount = 0;
         invocationTokens = new LinkedQueue();
         this.valve = valve;
         activeThreadsCountHighWaterMark = 0;
         blockingMethodWaitArea = new Object();
      }

      protected Client getClient() throws Exception
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public boolean isValveOpen()
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public void closeValve() throws Exception
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public void openValve() throws Exception
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public int getActiveThreadsCount()
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public synchronized void businessMethod1()
      {
         businessMethod1InvocationCount++;
         updateActiveThreadsCountHighWaterMark();
         notifyInvocationWaiter("businessMethod1");
      }

      public synchronized void businessMethod2()
      {
         businessMethod2InvocationCount++;
         updateActiveThreadsCountHighWaterMark();
         notifyInvocationWaiter("businessMethod2");
      }

      public void blockingBusinessMethod() throws InterruptedException
      {
         synchronized(blockingMethodWaitArea)
         {
            log.info("blockingBusinessMethod() blocking ...");
            // block calling thread undefinitely until blockingMethodWaitArea is notified
            blockingMethodWaitArea.wait();
            log.info("blockingBusinessMethod() unblocked");
         }
      }

      public synchronized int getBusinessMethod1InvocationCount()
      {
         return businessMethod1InvocationCount;
      }

      public synchronized int getBusinessMethod2InvocationCount()
      {
         return businessMethod2InvocationCount;
      }

      public synchronized int getActiveThreadsHighWaterMark()
      {
         return activeThreadsCountHighWaterMark;
      }

      /**
       * Block until an invocation arrives into the target. If the invocation arrived prior to
       * calling this method, it returns immediately.
       *
       * @return a token corresponding to the business invocation, or null if the method exited with
       *         timout, without an invocation to arive.
       */
      public InvocationToken waitForInvocation(long timeout) throws InterruptedException
      {
         return (InvocationToken)invocationTokens.poll(timeout);
      }

      public void unblockBlockingBusinessMethod()
      {
         synchronized(blockingMethodWaitArea)
         {
            blockingMethodWaitArea.notify();
         }
      }

      /**
       * Reset the state
       */
      public synchronized void reset()
      {
         businessMethod1InvocationCount = 0;
         businessMethod2InvocationCount = 0;
         activeThreadsCountHighWaterMark = 0;
      }

      /**
       * Notify someone who waits for this invocation to arrive.
       */
      private void notifyInvocationWaiter(String methodName)
      {
         try
         {
            invocationTokens.put(new InvocationToken(methodName));
         }
         catch(InterruptedException e)
         {
            throw new RuntimeException("Failed to deposit notification in queue", e);
         }
      }

      private synchronized void updateActiveThreadsCountHighWaterMark()
      {
         try
         {
            int c =
               ((Integer)valve.invoke(buildInvocation("getActiveThreadsCount", this))).intValue();
            if (c > activeThreadsCountHighWaterMark)
            {
               activeThreadsCountHighWaterMark = c;
            }
         }
         catch(Throwable t)
         {
            throw new RuntimeException("Failed to get getActiveThreadsCount", t);
         }
      }
   }

   private class ParentHierarchicalState extends HierarchicalStateSupport
   {
      private DelegateSupport delegate;
      private HierarchicalState parent;

      ParentHierarchicalState()
      {
         super(null, null);

         children = new HashSet();

         // populate it with a child state
         children.add(new ChildState());
      }

      public DelegateSupport getDelegate()
      {
         return delegate;
      }

      public void setDelegate(DelegateSupport delegate)
      {
         this.delegate = delegate;
      }

      public HierarchicalState getParent()
      {
         return parent;
      }

      public void setParent(HierarchicalState parent)
      {
         this.parent = parent;
      }

      public Version getVersionToUse()
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public void synchronizeWith(HierarchicalState newState) throws Exception
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }
   }

   private class ChildState extends HierarchicalStateSupport
   {
      private DelegateSupport delegate;
      private HierarchicalState parent;

      ChildState()
      {
         super(null, new ChildDelegate());
      }

      public Set getChildren()
      {
         return Collections.EMPTY_SET;
      }

      public DelegateSupport getDelegate()
      {
         return delegate;
      }

      public void setDelegate(DelegateSupport delegate)
      {
         this.delegate = delegate;
      }

      public HierarchicalState getParent()
      {
         return parent;
      }

      public void setParent(HierarchicalState parent)
      {
         this.parent = parent;
      }

      public Version getVersionToUse()
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public void synchronizeWith(HierarchicalState newState) throws Exception
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }
   }

   private class ChildDelegate extends DelegateSupport implements Valve
   {
      private boolean valveOpen;

      ChildDelegate()
      {
         valveOpen = true;
      }

      protected Client getClient() throws Exception
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }

      public synchronized void closeValve() throws Exception
      {
         valveOpen = false;
      }

      public void openValve() throws Exception
      {
         valveOpen = true;
      }

      public boolean isValveOpen()
      {
         return valveOpen;
      }

      public int getActiveThreadsCount()
      {
         throw new RuntimeException("NOT YET IMPLEMENTED");
      }
   }

   private class InvocationToken
   {
      private String methodName;

      public InvocationToken(String methodName)
      {
         this.methodName = methodName;
      }

      public String getMethodName()
      {
         return methodName;
      }
   }

}
