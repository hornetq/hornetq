/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.jboss.logging.Logger;

/**
 * 
 * A DrainJob.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiux@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class DrainJob extends BaseJob
{
   private static final long serialVersionUID = -4637670167659745878L;

   private static final Logger log = Logger.getLogger(DrainJob.class);

   public static final String TYPE = "DRAIN";

   protected static final long RECEIVE_TIMEOUT = 5000;

   protected String subName;

   protected String clientID;

   public DrainJob()
   {
      super();
   }

   public DrainJob(Properties jndiProperties,
                   String destinationName,
                   String connectionFactoryJndiName,
                   String subName,
                   String clientID)
   {
      super(jndiProperties, destinationName, connectionFactoryJndiName);
      this.subName = subName;
      this.clientID = clientID;
   }

   public String getType()
   {
      return TYPE;
   }

   public ThroughputResult execute() throws PerfException
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
      catch (Exception e)
      {
         log.error("Failed to drain", e);
         throw new PerfException("Failed to drain", e);
      }
      finally
      {
         try
         {
            if (conn != null)
            {
               conn.close();
            }
         }
         catch (Exception e)
         {
            log.error("Failed to close", e);
            throw new PerfException("Failed to close", e);
         }
      }
   }

   public void setSubName(String subName)
   {
      this.subName = subName;
   }

   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

}
