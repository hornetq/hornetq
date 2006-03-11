
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.jboss.logging.Logger;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ReceiveJob extends ThroughputJobSupport
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 3633353742146810600L;

   private static final Logger log = Logger.getLogger(ReceiveJob.class);

   public static final String TYPE = "RECEIVE";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String subName;
   protected String selector;
   protected boolean noLocal;
   protected boolean asynch;
   protected String clientID;

   // Constructors --------------------------------------------------

   public ReceiveJob()
   {
      numConnections = 1;
      numSessions = 1;
   }

   // Public --------------------------------------------------------

   public String getType()
   {
      return TYPE;
   }

   public Servitor createServitor(long testTime)
   {
      return new Receiver(testTime);
   }

   public void setAsynch(boolean asynch)
   {
      this.asynch = asynch;
   }

   public void setNoLocal(boolean noLocal)
   {
      this.noLocal = noLocal;
   }

   public void setSelector(String selector)
   {
      this.selector = selector;
   }

   public void setSubName(String subName)
   {
      this.subName = subName;
   }

   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String fullDump()
   {
      StringBuffer sb = new StringBuffer();
      Properties p = getJNDIProperties();

      sb.append("receiver job\n");
      sb.append("    JNDI properties\n");
      sb.append("        java.naming.factory.initial: ").
         append(p.getProperty("java.naming.factory.initial")).append('\n');
      sb.append("        java.naming.provider.url:    ").
         append(p.getProperty("java.naming.provider.url")).append('\n');
      sb.append("        java.naming.factory.url.pkg: ").
         append(p.getProperty("java.naming.factory.url.pkg")).append('\n');
      sb.append("    destination name:                ").
         append(getDestinationName()).append('\n');
      sb.append("    connection factory name:         ").
         append(getConnectionFactoryName()).append('\n');
      sb.append("    transacted:                      ").
         append(isTransacted()).append('\n');
      sb.append("    acknowledgmentMode:              ").
         append(JobSupport.acknowledgmentModeToString(getAcknowledgmentMode())).append('\n');
      sb.append("    message size:                    ").
         append(getMessageSize()).append(" bytes").append('\n');
      sb.append("    message count:                   ").
         append(getMessageCount()).append('\n');
      sb.append("    duration:                        ").
         append(getDuration()).append('\n');

      return sb.toString();
   }

   // Inner classes -------------------------------------------------

   protected class Receiver extends AbstractServitor
   {
      Receiver(long testTime)
      {
         super(testTime);
      }

      private Session session;
      private MessageConsumer consumer;

      public void deInit()
      {
         try
         {
            if (subName != null)
            {
               session.unsubscribe(subName);
            }

            session.close();
         }
         catch (Throwable e)
         {
            log.error("deInit failed", e);
            failed = true;
         }
      }

      public void init()
      {
         try
         {
            Connection conn = getNextConnection();

            if (subName != null)
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

            session = conn.createSession(transacted, getAcknowledgmentMode());

            log.debug("session " + session + " created");

            if (subName == null)
            {
               consumer = session.createConsumer(destination, selector, noLocal);
            }
            else
            {
               consumer = session.createDurableSubscriber((Topic)destination, subName,
                                                          selector, noLocal);
            }
         }
         catch (Throwable e)
         {
            log.error("Init failed", e);
            failed = true;
         }
      }

      public void run()
      {
         try
         {
            log.debug("start receiving using " + fullDump());

            long start = System.currentTimeMillis();

            while (true)
            {
               long timeLeft = duration - System.currentTimeMillis() + start;

               if (timeLeft <= 0)
               {
                  log.debug("terminating receiving because time (" + duration + " ms) expired");
                  break;
               }

               Message m = consumer.receive(timeLeft);

               if (m == null)
               {
                  log.debug("terminating receiving because time (" + duration + " ms) expired");
                  break;
               }

               currentMessageCount++;

               if (transacted)
               {
                  if (currentMessageCount % transactionSize == 0)
                  {
                     session.commit();
                  }
               }

               if (currentMessageCount == messageCount)
               {
                  if (transacted)
                  {
                     session.commit();
                  }

                  log.debug("terminating receiving because message count (" + messageCount +
                     ") has been reached");
                  break;
               }
            }

            actualTime = System.currentTimeMillis() - start;
         }
         catch (Throwable e)
         {
            log.error("Receiver failed", e);
            failed = true;
         }
      }
   }
}