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
package org.jboss.test.messaging.jms.bridge;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.server.bridge.Bridge;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A BridgeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeTest extends MessagingTestCase
{

   public BridgeTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testBridge() throws Exception
   {
      ServerManagement.start(0, "all", null, true);
      
      ServerManagement.start(1, "all", null, false);
      
      Hashtable props0 = ServerManagement.getJNDIEnvironment(0);
      
      Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
      
      ServerManagement.deployQueue("sourceQueue", 0);
      
      ServerManagement.deployQueue("destQueue", 1);
      
      InitialContext ic0 = new InitialContext(props0);
      
      InitialContext ic1 = new InitialContext(props1);
      
      ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");
      
      ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
      
      Queue sourceQueue = (Queue)ic0.lookup("/queue/sourceQueue");
      
      Queue destQueue = (Queue)ic1.lookup("/queue/destQueue");
      
      
      Bridge bridge = new Bridge(props0, props1, "/ConnectionFactory", "/ConnectionFactory",
               "/queue/sourceQueue", "/queue/destQueue", null, null, null, null,
               null, 0, false,
               false, 10, -1,
               null, null,
               false, false);
      
      bridge.start();
      

      Connection connSource = cf0.createConnection();
      
      Connection connDest = cf1.createConnection();
      
      Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sessSend.createProducer(sourceQueue);
      
      final int NUM_MESSAGES = 10;
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         TextMessage tm = sessSend.createTextMessage("message" + i);
         
         prod.send(tm);
      }
      
      Session sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessRec.createConsumer(destQueue);
      
      connDest.start();
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         TextMessage tm = (TextMessage)cons.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("message" + i, tm.getText());
      }
      
      //Make sure no messages are left in the source dest
      
      MessageConsumer cons2 = sessSend.createConsumer(sourceQueue);
      
      connSource.start();
      
      Message m = cons2.receive(1000);
      
      assertNull(m);
      
      connSource.close();
      
      connDest.close();
      
      bridge.stop();
      
      ServerManagement.stop(0);
      
      ServerManagement.stop(1);
      
   }

}
