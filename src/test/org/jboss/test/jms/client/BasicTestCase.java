/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.jms.client;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.jboss.jms.client.ImplementationDelegate;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.jvm.JVMImplementation;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.server.MessageBroker;
import org.jboss.jms.server.standard.StandardMessageBroker;
import org.jboss.test.jms.BaseJMSTest;

/**
 * A basic test
 * 
 * @author <a href="adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class BasicTestCase extends BaseJMSTest
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BasicTestCase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSomething()
      throws Exception
   {
      Queue queue = new JBossQueue("queue");
      MessageBroker broker = new StandardMessageBroker();
      ImplementationDelegate impl = new JVMImplementation(broker);
      ConnectionFactory cf = new JBossConnectionFactory(impl);
      Connection c = cf.createConnection();
      try
      {
         Session s = c.createSession(true, 0);
         MessageProducer p = s.createProducer(queue);
         Message m = s.createMessage();
         p.send(m);
         p.send(m);
         QueueBrowser b = s.createBrowser(queue);
         Enumeration e = b.getEnumeration();
         while (e.hasMoreElements())
            System.out.println(e.nextElement());
      }
      finally
      {
         //c.close();
      }
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
