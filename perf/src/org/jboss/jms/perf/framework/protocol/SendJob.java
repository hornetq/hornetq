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
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.jms.perf.framework.factories.MessageFactory;
import org.jboss.logging.Logger;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SendJob extends ThroughputJobSupport
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -4031253412475892666L;

   private transient static final Logger log = Logger.getLogger(SendJob.class);

   private static final long SAMPLE_PERIOD = 50;

   public static final String TYPE = "SEND";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected boolean anon;
   double targetMsgsPerSamplePeriod;

   private MessageFactory messageFactory;
   
   // Constructors --------------------------------------------------

   public SendJob()
   {
      numConnections = 1;
      numSessions = 1;
      transacted = false;
      transactionSize = 0;
      duration = Long.MAX_VALUE;
      anon = false;
      messageSize = 0;
      rate = 0;
      targetMsgsPerSamplePeriod = (((double)rate) * SAMPLE_PERIOD) / 1000;
   }

   // Public --------------------------------------------------------

   public String getType()
   {
      return TYPE;
   }

   public Servitor createServitor(long duration)
   {
      return new Sender(duration);
   }

   public void setAnon(boolean anon)
   {
      this.anon = anon;
   }

   public String toString()
   {
      return "SEND JOB";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String fullDump()
   {
      StringBuffer sb = new StringBuffer();
      Properties p = getJNDIProperties();

      sb.append("sender job\n");
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
      sb.append("    message factory class:           ").
         append(getMessageFactoryClass()).append('\n');
      sb.append("    delivery mode:                   ").
         append(JobSupport.deliveryModeToString(getDeliveryMode())).append('\n');
      sb.append("    message size:                    ").
         append(getMessageSize()).append(" bytes").append('\n');
      sb.append("    message count:                   ").
         append(getMessageCount()).append('\n');
      sb.append("    duration:                        ").
         append(getDuration()).append('\n');
      sb.append("    rate:                            ").
         append(rate).append(" messages/second");

      return sb.toString();
   }

   // Inner classes -------------------------------------------------

   protected class Sender extends AbstractServitor
   {

      // Constants --------------------------------------------------

      // Static -----------------------------------------------------

      // Attributes -------------------------------------------------

      private Session sess;
      private MessageProducer prod;

      // Constructors -----------------------------------------------

      Sender(long duration)
      {
         super(duration);
      }

      // Servitor implementation ------------------------------------

      public void init()
      {
         log.debug(this + " initializing");
         try
         {
            messageFactory = (MessageFactory)Class.forName(getMessageFactoryClass()).newInstance();

            Connection conn = getNextConnection();
            sess = conn.createSession(transacted, getAcknowledgmentMode());
            prod = null;

            if (anon)
            {
               prod = sess.createProducer(null);
            }
            else
            {
               prod = sess.createProducer(destination);
            }

            prod.setDeliveryMode(deliveryMode);
         }
         catch (Throwable e)
         {
            log.error("Failed to init", e);
            failed = true;
         }
      }

      public void deInit()
      {
         log.debug(this + " de-initializing");

         try
         {
            sess.close();
         }
         catch (Throwable e)
         {
            log.error("Failed to deInit()", e);
            failed = true;
         }
      }

      // Runnable implementation ------------------------------------

      public void run()
      {

         // if both duration and message count are set, the job sends until the first condition
         // is met

         try
         {
            log.debug("start sending using " + fullDump());

            long start = System.currentTimeMillis();
            long timeLeft;

            while (true)
            {
               timeLeft = duration - System.currentTimeMillis() + start;

               if (timeLeft <= 0)
               {
                  log.debug("terminating sending because time (" + duration + " ms) expired");
                  break;
               }

               Message m = messageFactory.getMessage(sess, messageSize);

               if (anon)
               {
                  prod.send(destination, m);
               }
               else
               {
                  prod.send(m);
               }

               currentMessageCount++;

               if (log.isTraceEnabled()) { log.trace("sent message " + currentMessageCount); }

               if (transacted)
               {
                  if (currentMessageCount % transactionSize == 0)
                  {
                     sess.commit();
                     if (log.isTraceEnabled()) { log.trace("committed"); }
                  }
               }

               if (currentMessageCount == messageCount)
               {
                  // message count exit condition

                  if (transacted)
                  {
                     sess.commit();
                     if (log.isTraceEnabled()) { log.trace("committed"); }
                  }

                  log.debug("terminating sending because message count (" + messageCount +
                            ") has been reached");
                  break;
               }

               if (rate > 0)
               {
                  doThrottle(start);
               }
            }

            actualTime = System.currentTimeMillis() - start;

            log.info("sent " + currentMessageCount + " messages, actual duration is " +
                     actualTime + " ms, actual send rate is " +
                     (((double)Math.round(currentMessageCount * 100000 / actualTime)) / 100) +
                     " messages/sec");

         }
         catch (Throwable e)
         {
            log.error("Sender failed", e);
            failed = true;
         }
         finally
         {
            log.info("finished sending on " + destination);
         }
      }

      // Public -----------------------------------------------------

      // Package protected ------------------------------------------

      // Protected --------------------------------------------------

      // Private ----------------------------------------------------

      private void doThrottle(long start)
      {
         long plannedTime = Math.round((double)start + (double)currentMessageCount * 1000 / rate);
         long sleepTime = plannedTime - System.currentTimeMillis();

         if (sleepTime <= 0)
         {
            // we're falling behind schedule, but there's nothing we can do, planned send rate
            // is too high, we cannot send messages faster that that

            if (log.isTraceEnabled()) { log.trace("falling behind schedule, mustn't sleep"); }
            return;
         }

         try
         {
            if (log.isTraceEnabled()) { log.trace("throttling control says sleep for " + sleepTime); }
            Thread.sleep(sleepTime);
         }
         catch(InterruptedException e)
         {
            // that's OK, next time we'll sleep longer
         }
      }

      // Inner classes ----------------------------------------------

   }
}
