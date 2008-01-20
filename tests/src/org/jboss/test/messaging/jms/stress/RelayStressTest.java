/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.stress;

import org.jboss.messaging.util.Logger;
import org.jboss.test.messaging.JBMServerTestCase;

import javax.jms.*;
import javax.naming.InitialContext;

/**
 * Send messages to a topic with selector1, consumer them with multiple consumers and relay them
 * back to the topic with a different selector, then consume that with more consumers.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class RelayStressTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   private static Logger log = Logger.getLogger(RelayStressTest.class);


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;

   // Constructors --------------------------------------------------

   public RelayStressTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      //ServerManagement.start("all");
      ic = getInitialContext();
      createTopic("StressTestTopic");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      destroyTopic("StressTestTopic");
      ic.close();
      super.tearDown();
   }
   
   public void testRelay() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      Topic topic = (Topic)ic.lookup("/topic/StressTestTopic");
      
      final int numMessages = 20000;
      
      final int numRelayers = 5;
      
      final int numConsumers = 20;
      
      Connection conn = cf.createConnection();
      
      class Relayer implements MessageListener
      {
         boolean done;
         
         boolean failed;
         
         int count;
         
         MessageProducer prod;
         
         Relayer(MessageProducer prod)
         {
            this.prod = prod;
         }
         
         public void onMessage(Message m)
         {
            try
            {
               m.clearProperties();
               m.setStringProperty("name", "Tim");
               
               prod.send(m);
               
               count++;               
               
               if (count == numMessages)
               {
                  synchronized (this)
                  {                
                     done = true;
                     notify();
                  }
               }
            }
            catch (JMSException e)
            {
               e.printStackTrace();
               synchronized (this)
               {                  
                  done = true;
                  failed = true;
                  notify();
               }               
            }         
         }
      }
      
      class Consumer implements MessageListener
      {
         boolean failed;
         
         boolean done;
         
         int count;
         
         public void onMessage(Message m)
         {
            count++; 
             
            if (count == numMessages * numRelayers)
            {
               synchronized (this)
               {  
                  done = true;                  
                  notify();
               }
            }
         }
      }
      
      Relayer[] relayers = new Relayer[numRelayers];
      
      Consumer[] consumers = new Consumer[numConsumers];
      
            
      for (int i = 0; i < numRelayers; i++)
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sess.createConsumer(topic, "name = 'Watt'");
         //MessageConsumer cons = sess.createConsumer(topic);
         
         MessageProducer prod = sess.createProducer(topic);
         
         relayers[i] = new Relayer(prod);
         
         cons.setMessageListener(relayers[i]);
      }
      
      for (int i = 0; i < numConsumers; i++)
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sess.createConsumer(topic, "name = 'Tim'");
         
         consumers[i] = new Consumer();
         
         cons.setMessageListener(consumers[i]);
      }
      
      conn.start();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(topic);
      
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      for (int i = 0; i < numMessages; i++)
      {
         Message m = sess.createMessage();
         
         m.setStringProperty("name", "Watt");
         
         prod.send(m);
      }
      
      for (int i = 0; i < numRelayers; i++)
      {
         synchronized (relayers[i])
         {
            if (!relayers[i].done)
            {
               relayers[i].wait();
            }
         }
      }
      
      for (int i = 0; i < numConsumers; i++)
      {
         synchronized (consumers[i])
         {
            if (!consumers[i].done)
            {
               consumers[i].wait();
            }
         }
      }
      
      conn.close();
      
      for (int i = 0; i < numRelayers; i++)
      {
         assertFalse(relayers[i].failed);         
      }
      
      for (int i = 0; i < numConsumers; i++)
      {
         assertFalse(consumers[i].failed);         
      }
           
   }  
}

