/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.jms.p2p;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.p2p.P2PImplementation;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.test.jms.BaseJMSTest;

/**
 * A basic test
 * 
 * @author <a href="adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class BasicTestCase extends BaseJMSTest
   implements MessageListener
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ArrayList messages = new ArrayList();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BasicTestCase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSimpleSendReceive()
      throws Exception
   {
      Topic topic = new JBossTopic("testTopic");
      ConnectionFactory cf = new JBossConnectionFactory(new P2PImplementation());
      Connection c = cf.createConnection();
      try
      {
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(topic);
         Message m = s.createTextMessage("Hello");
         MessageConsumer r = s.createConsumer(topic);
         c.start();
         p.send(m);
         TextMessage tm = (TextMessage) r.receive(1000);
         assertTrue("Should have a message", tm != null);
         assertTrue("Message should say Hello", tm.getText().equals("Hello"));
      }
      finally
      {
         c.close();
      }
   }

   public void testMessageListener()
      throws Exception
   {
      messages.clear();
      Topic topic = new JBossTopic("testTopic");
      ConnectionFactory cf = new JBossConnectionFactory(new P2PImplementation());
      Connection c = cf.createConnection();
      try
      {
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(topic);
         Message m = s.createTextMessage("Listen to this");
         MessageConsumer r = s.createConsumer(topic);
         r.setMessageListener(this);
         c.start();
         p.send(m);
         Thread.sleep(1000);
         assertTrue("Should have one message", messages.size() == 1);
         TextMessage tm = (TextMessage) messages.remove(0);
         assertTrue("Message should say Listen to this", tm.getText().equals("Listen to this"));
      }
      finally
      {
         c.close();
      }
   }

   // MessageListener implementation----------------------------------
   
   public void onMessage(Message message)
   {
      messages.add(message);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
