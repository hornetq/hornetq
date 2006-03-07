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
package org.jboss.test.messaging.core.local.base;

import org.jboss.test.messaging.core.base.ChannelTestBase;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;

import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public abstract class QueueTestBase extends ChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected JDBCPersistenceManager pm;

   // Constructors --------------------------------------------------
   
   public QueueTestBase(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      pm = new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());
      pm.start();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testUnreliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      channel.add(r1);
      channel.add(r2);

      Delivery d = channel.handle(observer, MessageFactory.createCoreMessage(0, false, "payload"), null);

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();
      if (l2.isEmpty())
      {
         assertEquals(1, l1.size());
         Message m = (Message)l1.get(0);
         assertEquals("payload", m.getPayload());
      }
      else
      {
         assertTrue(l1.isEmpty());
         assertEquals(1, l2.size());
         Message m = (Message)l2.get(0);
         assertEquals("payload", m.getPayload());
      }
   }


   public void testReliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      assertTrue(channel.add(r1));
      assertTrue(channel.add(r2));

      Delivery d = channel.handle(observer, MessageFactory.createCoreMessage(0, true, "payload"), null);

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();
      if (l2.isEmpty())
      {
         assertEquals(1, l1.size());
         Message m = (Message)l1.get(0);
         assertEquals("payload", m.getPayload());
      }
      else
      {
         assertTrue(l1.isEmpty());
         assertEquals(1, l2.size());
         Message m = (Message)l2.get(0);
         assertEquals("payload", m.getPayload());
      }
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
