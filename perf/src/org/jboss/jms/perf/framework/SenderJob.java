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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.DeliveryMode;
import javax.jms.Queue;

import org.jboss.jms.perf.framework.factories.MessageFactory;
import org.jboss.jms.perf.framework.factories.MessageMessageFactory;
import org.jboss.logging.Logger;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SenderJob extends BaseThroughputJob
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -4031253412475892666L;

   private transient static final Logger log = Logger.getLogger(SenderJob.class);

   private static final long SAMPLE_PERIOD = 50;

   public static final String TYPE = "SEND";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected boolean anon;
   protected int deliveryMode;
   protected MessageFactory messageFactory;

   double targetMsgsPerSamplePeriod;
   
   // Constructors --------------------------------------------------

   public SenderJob()
   {
      this(null, null, null, 1, 1, false, 0, Long.MAX_VALUE, false, 0, new MessageMessageFactory(),
           DeliveryMode.NON_PERSISTENT, 0);
   }

   public SenderJob(Properties jndiProperties,
                    String destinationName,
                    String connectionFactoryJndiName,
                    int numConnections,
                    int numSessions,
                    boolean transacted,
                    int transactionSize,
                    long duration,
                    boolean anon,
                    int messageSize,
                    MessageFactory messageFactory,
                    int deliveryMode,
                    int rate)
   {
      super(jndiProperties,
            destinationName,
            connectionFactoryJndiName,
            numConnections,
            numSessions,
            transacted,
            transactionSize,
            duration,
            rate);

      this.anon = anon;
      this.messageSize = messageSize;
      this.messageFactory = messageFactory;
      this.deliveryMode = deliveryMode;

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

   public void setDeliveryMode(int deliveryMode)
   {
      this.deliveryMode = deliveryMode;
   }

   public void setMessageFactory(MessageFactory messageFactory)
   {
      this.messageFactory = messageFactory;
   }

   public MessageFactory getMessageFactory()
   {
      return messageFactory;
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
      sb.append("    message factory:                 ").
         append(getMessageFactory()).append('\n');
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
      Sender(long duration)
      {
         super(duration);
      }
      
      MessageProducer prod;
      
      Session sess;
      
      public void deInit()
      {
         log.info("De-initialising");
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
      
      public void init()
      {
         try
         {
            Connection conn = getNextConnection();
            
            sess = conn.createSession(transacted, Session.AUTO_ACKNOWLEDGE); //Ackmode doesn't matter            
            
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
      
      public void run()
      {

         // if both duration and message count are set, the job sends until the first condition
         // is met

         try
         {
            log.debug("start sending using " + fullDump());

            long start = System.currentTimeMillis();
            
            long now = 0;

            while (true)
            {
               now = System.currentTimeMillis();
               
               if (duration != Long.MAX_VALUE && now > start + duration)
               {                                    
                  log.debug("terminating sending because time expired");
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

                  log.debug("terminating sending because message count has been reached");
                  break;
               }
               
               doThrottle(now);
                             
            }
                                     
            actualTime = now - start;
            
            log.info("sent " + currentMessageCount + " messages, actual duration is " + actualTime + " ms");

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
      
      private long lastTime = 0;
      private int lastCount = 0;
                  
      protected void doThrottle(long now)
      {
         if (rate == 0)
         {
            return;
         }

         if (log.isTraceEnabled()) { log.trace("doThrottle(" + now + ")"); }

         if (lastTime != 0)
         {
            long elapsedTime = now - lastTime;
            
            if (elapsedTime >= SAMPLE_PERIOD)
            {
               long msgs = currentMessageCount - lastCount;
               
               //log.info("elapsedTime is " + elapsedTime);
               //log.info("msgs:" + msgs);
               
               double targetMsgs = rate * (double)elapsedTime / 1000;
               
               //log.info("Target msgs:" + targetMsgs);
               
               if (msgs > targetMsgs)
               {
                  //Need to throttle
                  //log.info("throttling");
                  
                  long throttleTime = (long)((1000 * (double)msgs / rate) - elapsedTime);
                                    
                  //log.info("Throttle time is:" + throttleTime);
                  try
                  {
                     Thread.sleep(throttleTime);
                  }
                  catch (InterruptedException e)
                  {
                     //Ignore
                  }
               }
               
               lastTime = now;
               
               lastCount = currentMessageCount;
            }
         }
         else
         {
            lastTime = now;
         }
      }
   }
}
