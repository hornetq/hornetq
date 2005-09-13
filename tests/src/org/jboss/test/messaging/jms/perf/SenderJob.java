/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.logging.Logger;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SenderJob extends AbstractJob
{
   private static final long serialVersionUID = -4031253412475892666L;

   private transient static final Logger log = Logger.getLogger(SenderJob.class);

   protected boolean anon;
   
   protected int msgSize;

   protected int deliveryMode;
   
   protected MessageFactory mf;
   
   public void getResults(ResultPersistor persistor)
   {
      super.getResults(persistor);
      
      persistor.addValue("anon", anon);
      persistor.addValue("msgSize", msgSize);
      persistor.addValue("deliveryMode", deliveryMode);
      persistor.addValue("messageFactory", mf.getClass().getName());
   }
   
   public Servitor createServitor()
   {
      return new Sender();
   }
   
   public String getName()
   {
      return "Sender";
   }
   
   public SenderJob(Element e) throws DeploymentException
   {
      importXML(e);
   }
   

   protected void logInfo()
   {
      super.logInfo();
      log.info("Use anonymous producer? " + anon);
      log.info("Message size: " + msgSize);
      log.info("Message type: " + mf.getClass().getName());
      log.info("Delivery Mode:" + (deliveryMode == DeliveryMode.PERSISTENT ? "Persistent" : "Non-persistent"));
   }
   
   public void importXML(Element element) throws DeploymentException
   {  
      super.importXML(element);
      
      this.anon = MetadataUtils.getOptionalChildBooleanContent(element, "anonymous-producer", false);
      this.msgSize = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "message-size"));
      String messageType = MetadataUtils.getUniqueChildContent(element, "message-type");
      
      if ("javax.jms.Message".equals(messageType))
      {
         mf = new MessageMessageFactory();
      }
      else if ("javax.jms.BytesMessage".equals(messageType))
      {
         mf = new BytesMessageMessageFactory();
      }
      else if ("javax.jms.MapMessage".equals(messageType))
      {
         mf = new MapMessageMessageFactory();
      }
      else if ("javax.jms.ObjectMessage".equals(messageType))
      {
         mf = new ObjectMessageMessageFactory();
      }
      else if ("javax.jms.StreamMessage".equals(messageType))
      {
         mf = new StreamMessageMessageFactory();
      }
      else if ("javax.jms.TextMessage".equals(messageType))
      {
         mf = new TextMessageMessageFactory();
      }
      else if ("foreign".equals(messageType))
      {
         mf = new ForeignMessageMessageFactory();
      }
      else
      {
         throw new DeploymentException("Invalid message type:" + messageType);
      }
      
      this.deliveryMode = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "delivery-mode"));
      
      if (deliveryMode != DeliveryMode.NON_PERSISTENT && deliveryMode != DeliveryMode.PERSISTENT)
      {
         throw new DeploymentException("Invalid delivery mode:" + deliveryMode);
      }
   }

   protected class Sender extends AbstractServitor
   {
      private boolean failed;
      
      private boolean stopping;
      
      public void stop()
      {
         stopping = true;
      }
      
      public void run()
      {
         try
         {
            Thread.sleep(rampDelay);
            
            Connection conn = getNextConnection();
            
            Session sess = conn.createSession(transacted, Session.AUTO_ACKNOWLEDGE); //Ackmode doesn't matter            
            
            MessageProducer prod = null;
            
            if (anon)
            {
               prod = sess.createProducer(null);
            }
            else
            {
               prod = sess.createProducer(dest);
            }
            
            prod.setDeliveryMode(deliveryMode);                       
            
            long count = 0;
            
            while (true)
            {
            
               Message m = mf.getMessage(sess, msgSize);
               
               if (anon)
               {
                  prod.send(dest, m);            
               }
               else
               {
                  prod.send(m);
               }
               
               count++;
               
               if (count != 0 && count % 50 == 0)
               {
                  updateTotalCount(50);
                  count = 0;
               }
               
               if (transacted)
               {
                  if (count % transactionSize == 0)
                  {
                     sess.commit();
                  }
               }                          
               
               if (stopping)
               {
                  break;
               }
            }
                        
            updateTotalCount(count);
            
         }
         catch (Exception e)
         {
            log.error("Sender failed", e);
            failed = true;
         }
      }
      
      public boolean isFailed()
      {
         return failed;
      }
   }
   
   
}
