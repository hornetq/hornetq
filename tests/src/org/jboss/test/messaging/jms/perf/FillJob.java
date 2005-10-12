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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.logging.Logger;
import org.w3c.dom.Element;

public class FillJob extends BaseJob
{
   private static final Logger log = Logger.getLogger(FillJob.class);
   
   protected int numMessages;
   
   protected int deliveryMode;
   
   protected int msgSize;
   
   protected MessageFactory mf;
   
   public String getName()
   {
      return "Fill Job";
   }
   
   public FillJob()
   {
      
   }
   
   public FillJob(Element e) throws ConfigurationException
   {
      super(e);
   }
   
   public void logInfo()
   {
      super.logInfo();
      log.info("numMessages: " + this.numMessages);
      log.info("Delivery mode: " + this.deliveryMode);
      log.info("Message size: " + this.msgSize);
      log.info("Message type: " + this.mf.getClass().getName());
   }

   public JobResult run()
   {
            
      Connection conn = null;
      
      try
      {
         super.setup();
         
         conn = cf.createConnection();
      
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(dest);
         prod.setDeliveryMode(deliveryMode);
         
         for (int i = 0; i < numMessages; i++)
         {
            Message m = mf.getMessage(sess, msgSize);
            prod.send(m);
         }
        
      }
      catch (Exception e)
      {
         log.error("Failed to fill destination", e);
         failed = true;
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception e)
            {
               log.error("Failed to close connection", e);
               failed = true;
            }
         }
      }
      
      return null;
   } 
   
   public void importXML(Element element) throws ConfigurationException
   {  
      super.importXML(element);
      
      this.numMessages = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "num-messages"));
      
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
         throw new ConfigurationException("Invalid message type:" + messageType);
      }
      
      this.deliveryMode = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "delivery-mode"));
      
      if (deliveryMode != DeliveryMode.NON_PERSISTENT && deliveryMode != DeliveryMode.PERSISTENT)
      {
         throw new ConfigurationException("Invalid delivery mode:" + deliveryMode);
      }
   }
   
   public void fillInResults(ResultPersistor persistor)
   {
      
   }

}