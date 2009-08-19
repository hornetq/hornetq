/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.hornetq.tests.integration.jms.server;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.security.JBMSecurityManager;
import org.hornetq.core.security.impl.JBMSecurityManagerImpl;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.core.server.impl.MessagingServerImpl;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.jms.client.JBossConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A JMSServerStartStopTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class JMSServerStartStopTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(JMSServerStartStopTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private JMSServerManager liveJMSServer;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testStopStart1() throws Exception
   {
      final int numMessages = 5;
      
      for (int j = 0; j < numMessages; j++)
      {
         log.info("Iteration " + j);
         
         start();
         
         JBossConnectionFactory jbcf = new JBossConnectionFactory(new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName()));
                                                                  
         
         jbcf.setBlockOnPersistentSend(true);
         jbcf.setBlockOnNonPersistentSend(true);
         
         Connection conn = jbcf.createConnection();
   
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
               
         Queue queue = sess.createQueue("myJMSQueue");
   
         MessageProducer producer = sess.createProducer(queue);
    
         TextMessage tm = sess.createTextMessage("message" + j);
   
         producer.send(tm);
                                 
         conn.close();
         
         jbcf.close();
         
         stop();
      }
      
      start();
      
      JBossConnectionFactory jbcf = new JBossConnectionFactory(new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName()));
      
      jbcf.setBlockOnPersistentSend(true);
      jbcf.setBlockOnNonPersistentSend(true);
      
      Connection conn = jbcf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
      Queue queue = sess.createQueue("myJMSQueue");

      MessageConsumer consumer = sess.createConsumer(queue);

      conn.start();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = (TextMessage)consumer.receive(1000);

         assertNotNull(tm);

         assertEquals("message" + i, tm.getText());
      }
            
      conn.close();
      
      jbcf.close();
      
      stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   protected void tearDown() throws Exception
   {
      liveJMSServer = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------
   
   private void stop() throws Exception
   {
      liveJMSServer.stop();
   }
   
   private void start() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();
      
      fc.setConfigurationUrl("server-start-stop-config1.xml");
      
      fc.start();
      
      JBMSecurityManager sm = new JBMSecurityManagerImpl();
      
      MessagingServer liveServer = new MessagingServerImpl(fc, sm);     
      
      liveJMSServer = new JMSServerManagerImpl(liveServer, "server-start-stop-jms-config1.xml");
      
      liveJMSServer.setContext(null);
      
      liveJMSServer.start();
   }
   

   // Inner classes -------------------------------------------------

}
