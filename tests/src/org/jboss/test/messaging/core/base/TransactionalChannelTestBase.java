/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.base;

import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.message.Factory;
import org.jboss.messaging.core.message.TransactionalMessageStore;

import javax.naming.InitialContext;
import javax.transaction.TransactionManager;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class TransactionalChannelTestBase extends ChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static void assertEqualSets(Routable[] a, List messages)
   {
      assertEquals(a.length, messages.size());
      List l = new ArrayList(messages);

      for(int i = 0; i < a.length; i++)
      {
         for(Iterator j = l.iterator(); j.hasNext(); )
         {
            if (a[i].getMessageID().equals(((MessageReference)j.next()).getMessageID()))
            {
               j.remove();
               break;
            }
         }
      }

      if (!l.isEmpty())
      {
         fail("Messages " + l + " do not match!");
      }
   }

   // Attributes ----------------------------------------------------

   protected TransactionManager tm;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public TransactionalChannelTestBase(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides -------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      InitialContext ic = new InitialContext();
      tm = (TransactionManager)ic.lookup("java:/TransactionManager");
      ic.close();

      ms = new TransactionalMessageStore("message-store", tm);

   }

   public void tearDown() throws Exception
   {
      ms = null;
      tm = null;
      super.tearDown();
   }

   // Public --------------------------------------------------------

   public void testUnreliableMessageCommit() throws Throwable
   {
      if (!channel.isTransactional())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      channel.add(receiver);
      MessageStore ms = channel.getMessageStore();
      Routable[] m =
            {
               ms.reference(Factory.createMessage("m0", false, "payload0")),
               ms.reference(Factory.createMessage("m1", false, "payload1")),
               ms.reference(Factory.createMessage("m2", false, "payload2")),
            };


      tm.begin();

      channel.handle(observer, m[0]);
      channel.handle(observer, m[1]);
      channel.handle(observer, m[2]);

      assertTrue(receiver.getMessages().isEmpty());
      assertTrue(channel.browse().isEmpty());

      tm.commit();

      List messages = receiver.getMessages();
      assertEqualSets(m, messages);
   }

   public void testUnreliableMessageRollback() throws Throwable
   {
      if (!channel.isTransactional())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      channel.add(receiver);
      MessageStore ms = channel.getMessageStore();
      Routable[] m =
            {
               ms.reference(Factory.createMessage("m0", false, "payload0")),
               ms.reference(Factory.createMessage("m1", false, "payload1")),
               ms.reference(Factory.createMessage("m2", false, "payload2")),
            };

      tm.begin();

      channel.handle(observer, m[0]);
      channel.handle(observer, m[1]);
      channel.handle(observer, m[2]);

      assertTrue(receiver.getMessages().isEmpty());
      assertTrue(channel.browse().isEmpty());

      tm.rollback();

      assertTrue(receiver.getMessages().isEmpty());
   }


   public void testReliableMessageCommit() throws Throwable
   {
      if (!channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      channel.add(receiver);
      MessageStore ms = channel.getMessageStore();
      Routable[] m =
            {
               ms.reference(Factory.createMessage("m0", true, "payload0")),
               ms.reference(Factory.createMessage("m1", true, "payload1")),
               ms.reference(Factory.createMessage("m2", true, "payload2")),
            };


      tm.begin();

      channel.handle(observer, m[0]);
      channel.handle(observer, m[1]);
      channel.handle(observer, m[2]);

      assertTrue(receiver.getMessages().isEmpty());
      assertTrue(channel.browse().isEmpty());

      tm.commit();

      List messages = receiver.getMessages();
      assertEqualSets(m, messages);
   }

   public void testReliableMessageRollback() throws Throwable
   {
      if (!channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      channel.add(receiver);
      MessageStore ms = channel.getMessageStore();
      Routable[] m =
            {
               ms.reference(Factory.createMessage("m0", true, "payload0")),
               ms.reference(Factory.createMessage("m1", true, "payload1")),
               ms.reference(Factory.createMessage("m2", true, "payload2")),
            };



      tm.begin();

      channel.handle(observer, m[0]);
      channel.handle(observer, m[1]);
      channel.handle(observer, m[2]);

      assertTrue(receiver.getMessages().isEmpty());
      assertTrue(channel.browse().isEmpty());

      tm.rollback();

      assertTrue(receiver.getMessages().isEmpty());
   }



   public void testUnreliableMessageNACKingReceiverAcknowldgmentCommit() throws Throwable
   {
      if (!channel.isTransactional())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.NACKING);
      channel.add(receiver);
      MessageStore ms = channel.getMessageStore();
      Routable[] m =
            {
               ms.reference(Factory.createMessage("m0", false, "payload0")),
               ms.reference(Factory.createMessage("m1", false, "payload1")),
               ms.reference(Factory.createMessage("m2", false, "payload2")),
            };

      channel.handle(observer, m[0]);
      channel.handle(observer, m[1]);
      channel.handle(observer, m[2]);

      List l = channel.browse();
      assertEquals(3, l.size());

      tm.begin();

      receiver.acknowledge(m[0]);
      receiver.acknowledge(m[1]);
      receiver.acknowledge(m[2]);

      l = channel.browse();
      assertEquals(3, l.size());

      tm.commit();

      assertTrue(channel.browse().isEmpty());
   }

   public void testUnreliableMessageNACKingReceiverAcknowldgmentRollback() throws Throwable
   {
      if (!channel.isTransactional())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.NACKING);
      channel.add(receiver);
      MessageStore ms = channel.getMessageStore();
      Routable[] m =
            {
               ms.reference(Factory.createMessage("m0", false, "payload0")),
               ms.reference(Factory.createMessage("m1", false, "payload1")),
               ms.reference(Factory.createMessage("m2", false, "payload2")),
            };

      channel.handle(observer, m[0]);
      channel.handle(observer, m[1]);
      channel.handle(observer, m[2]);

      List l = channel.browse();
      assertEquals(3, l.size());

      tm.begin();

      receiver.acknowledge(m[0]);
      receiver.acknowledge(m[1]);
      receiver.acknowledge(m[2]);

      l = channel.browse();
      assertEquals(3, l.size());

      tm.rollback();


      l = channel.browse();
      assertEqualSets(m, l);
   }



   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
