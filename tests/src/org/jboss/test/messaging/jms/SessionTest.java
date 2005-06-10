/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.naming.InitialContext;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public class SessionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination topic;

   // Constructors --------------------------------------------------

   public SessionTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.startInVMServer();
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      cf =
            (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.deployTopic("Topic");
      topic = (Destination)initialContext.lookup("/topic/Topic");

      
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      //connection.stop();
      //connection = null;
      super.tearDown();
   }


   // Public --------------------------------------------------------

   public void testCreateProducer() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer mp = sess.createProducer(topic);
      conn.close();
   }
   
   public void testCreateConsumer() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer mc = sess.createConsumer(topic);
      conn.close();
   }
   
   public void testGetSession1() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         Session sess2 = ((XASession)sess).getSession();
         fail("Should throw IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {}
      conn.close();
   }
   
   public void testGetSession2() throws Exception
   {
      XAConnection conn = cf.createXAConnection();      
      XASession sess = conn.createXASession();
      
      Session sess2 = sess.getSession();      
      conn.close();
   }
   
	 // TODO: enable it after implementing JBossSession.getXAResource()
	/*
   public void testGetXAResource() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         XAResource xaResource = ((XASession)sess).getXAResource();
         fail("Should throw IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {}
      conn.close();
   }
   */


   // TODO: enable it after implementing JBossSession.getXAResource()
//   public void testGetXAResource2() throws Exception
//   {
//      XAConnection conn = cf.createXAConnection();
//      XASession sess = conn.createXASession();
//
//      XAResource xaRessource = sess.getXAResource();
//      conn.close();
//   }
   
   
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}

