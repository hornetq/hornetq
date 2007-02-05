/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.test.messaging.jms;

import java.net.SocketPermission;
import java.security.Permission;
import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A ClientInRestrictedSecurityEnvironmentTest
 * 
 * This test runs the JMS client in a restricted security environment to ensure it works.
 * 
 * Currently we just check that no socket connections are listened for or accepted on the client side
 * (which would be true for the socket transport)
 * 
 * Therefore this test will fail until the bisocket transport is integrated.
 * 
 * The test can be easily extended for other security requirements, e.g. getting system properties
 * might be prohibited.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClientInRestrictedSecurityEnvironmentTest extends MessagingTestCase
{   
   // MessagingTestCase overrides ------------------------------------------------------------------
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.start("all");                  
   }

   protected void tearDown() throws Exception
   {            
      try
      {
         super.tearDown();
      }
      catch (Exception e)
      {
         //Ignore - this will probably faail because the new security manager won't allow something
         //that is done in tearDown()
      }
   }
   
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------
   
   // Constructors ---------------------------------------------------------------------------------

   public ClientInRestrictedSecurityEnvironmentTest(String name)
   {
      super(name);
   }

   
   // Public ---------------------------------------------------------------------------------------
   
   public void testSendReceiveWithSecurityManager() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      
      ServerManagement.undeployQueue("TestQueue");
      
      ServerManagement.deployQueue("TestQueue");
      
      InitialContext ic = null;
      
      Connection conn = null;
      
      Hashtable env = ServerManagement.getJNDIEnvironment();
      
      ServerManagement.undeployQueue("TestQueue");
      
      ServerManagement.deployQueue("TestQueue");
      
      ic = new InitialContext(env);
      
      ConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
      
      Queue queue = (Queue)ic.lookup("/queue/TestQueue");
      
      SecurityManager oldSm = System.getSecurityManager();
                  
      SecurityManager sm = new MySecurityManager();
      
      System.setSecurityManager(sm);
      
      log.info("Security Manager is now " + System.getSecurityManager());
      
      
      try
      {
         conn = cf.createConnection();
 
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer p = s.createProducer(queue);
         
         MessageConsumer c = s.createConsumer(queue);
         
         conn.start();

         p.send(s.createTextMessage("payload"));
         
         TextMessage m = (TextMessage)c.receive();

         assertEquals("payload", m.getText());
         
         conn.close();
         conn = null;
         
         ic.close();
         ic = null;
      }
      catch (Exception e)
      {
         e.printStackTrace();
         
         throw e;
      }
      finally
      {
         System.setSecurityManager(oldSm);
         
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception ignore)
            {               
            }
         }
         if (ic != null)
         {
            try
            {
               ic.close();
            }
            catch (Exception ignore)
            {               
            }
         }
         
         try
         {
            ServerManagement.undeployQueue("TestQueue");
         }
         catch (Exception ignore)
         {            
         }
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
   
   protected class MySecurityManager extends SecurityManager
   {

      public void checkPermission(Permission perm, Object context)
      {         
         checkPermission(perm);
      }

      public void checkPermission(Permission perm)
      {
         if (perm instanceof SocketPermission)
         {
            if (perm.getActions().indexOf("listen") != -1 ||
                perm.getActions().indexOf("accept") != -1)
            {

               //We disallow listening or accepting sockets in the client
               //This should test whether the bisocket is working properly
               
               throw new SecurityException("Client shouldn't listen/accept");
            }
         }                                  
      }

   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}


  
