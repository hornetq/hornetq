/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;


import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.remoting.Result;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiux@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class DrainJob extends JobSupport
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -4637670167659745878L;
   private static final Logger log = Logger.getLogger(DrainJob.class);
   public static final String TYPE = "DRAIN";

   protected static final long RECEIVE_TIMEOUT = 5000;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String subName;
   protected String clientID;

   // Constructors --------------------------------------------------

   // Job implemenation ---------------------------------------------

   public String getType()
   {
      return TYPE;
   }

   // JobSupport overrides ---------------------------------------------

   protected Result doWork() throws Exception
   {
      Connection conn = null;

      int count = 0;

      try
      {
         log.info("started DRAIN job on " + destinationName);

         conn = cf.createConnection();

         if (clientID != null)
         {
            try
            {
               conn.setClientID(clientID);
            }
            catch (Exception e)
            {
               //Some providers may provide a connection with client id already set
            }
         }

         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer consumer;
         if (subName == null)
         {
            consumer = sess.createConsumer(destination);
         }
         else
         {
            consumer = sess.createDurableSubscriber((Topic)destination, subName);
         }

         if (log.isTraceEnabled()) { log.trace("consumer created " + consumer); }

         while (true)
         {
            Message m = consumer.receiveNoWait();
            if (m == null)
            {
               break;
            }
            count++;
            if (count % 100 == 0)
            {
               log.info("received " + count + " messages");
            }
         }

         log.info("finished DRAIN job, drained " + count + " messages");

         return new ThroughputResult(0, count);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------


   public void setSubName(String subName)
   {
      this.subName = subName;
   }

   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

}
