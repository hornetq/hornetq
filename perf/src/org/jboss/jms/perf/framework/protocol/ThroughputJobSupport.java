
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;

import javax.jms.Connection;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.Context;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
abstract class ThroughputJobSupport extends JobSupport
{
   // Constants -----------------------------------------------------

   private transient static final Logger log = Logger.getLogger(ThroughputJobSupport.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected int numConnections;

   /* Number of concurrent session to use - sessions use connections in round-robin fashion */
   protected int numSessions;
   protected int connIndex;
   protected int transactionSize;

   protected Connection[] conns;
   protected Thread[] servitorThreads;
   protected Servitor[] servitors;

   // Constructors --------------------------------------------------

   protected ThroughputJobSupport()
   {
      super();
   }

   // JobSupport overrides ------------------------------------------

   protected final Result doWork(Context context) throws Exception
   {
      try
      {
         boolean failed = false;

         log.debug(this + " runs test with " + numSessions + " parallel sessions");

         for (int i = 0; i < numSessions; i++)
         {
            servitorThreads[i].start();
         }

         for (int i = 0; i < numSessions; i++)
         {
            try
            {
               servitorThreads[i].join();
            }
            catch (InterruptedException e)
            {
               throw new Exception("Thread interrupted");
            }
         }

         long totalTime = 0;
         long totalMessages = 0;

         for (int i = 0; i < numSessions; i++)
         {
            Servitor servitor = servitors[i];
            servitor.deInit();

            if (servitor.isFailed())
            {
               failed = true;
               break;
            }
            else
            {
               totalTime += servitor.getTime();
               totalMessages += servitor.getMessages();
            }
         }

         if (failed)
         {
            throw new Exception("Job failed");
         }

         return new ThroughputResult(totalTime, totalMessages);
      }
      finally
      {
         tearDown();
      }
   }

   // Public --------------------------------------------------------

   public void setNumConnections(int numConnections)
   {
      this.numConnections = numConnections;
   }

   public void setNumSessions(int numSessions)
   {
      this.numSessions = numSessions;
   }

   public void setTransactionSize(int transactionSize)
   {
      this.transactionSize = transactionSize;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract Servitor createServitor(long duration);

   protected void initialize(Context context) throws Exception
   {
      super.initialize(context);

      conns = new Connection[numConnections];

      for (int i = 0; i < numConnections; i++)
      {
         conns[i] = cf.createConnection();
         conns[i].start();
      }

      servitors = new Servitor[numSessions];
      servitorThreads = new Thread[numSessions];


      for (int i = 0; i < numSessions; i++)
      {
         Servitor servitor = createServitor(duration);

         servitor.init();
         servitors[i] = servitor;
         servitorThreads[i] = new Thread(servitors[i]);
      }

      log.debug("initialized " + this);
   }

   protected synchronized Connection getNextConnection()
   {
      return conns[connIndex++ % conns.length];
   }

   private void tearDown() throws Exception
   {
      for (int i = 0; i < numConnections; i++)
      {
         conns[i].close();
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}