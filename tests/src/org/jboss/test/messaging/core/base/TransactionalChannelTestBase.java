/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.base;

import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.message.Factory;
import org.jboss.messaging.core.message.TransactionalMessageStore;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;

import javax.naming.InitialContext;
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
            Object o = j.next();
            Message m = null;
            if (o instanceof MessageReference)
            {
               MessageReference ref = (MessageReference)o;
               m = ref.getMessage();
            }
            else
            {
               m = (Message)o;
            }
            
            if (a[i].getMessageID().equals(m.getMessageID()))
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

   protected TransactionRepository tm;
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
      tm = new TransactionRepository(null);
      ic.close();

      ms = new TransactionalMessageStore("message-store");

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
               Factory.createMessage("m0", false, "payload0"),
               Factory.createMessage("m1", false, "payload1"),
               Factory.createMessage("m2", false, "payload2"),
            };


      Transaction tx = tm.createTransaction();

      channel.handle(observer, m[0], tx);
      channel.handle(observer, m[1], tx);
      channel.handle(observer, m[2], tx);

      assertTrue(receiver.getMessages().isEmpty());
      
      List msgs = channel.browse();
      log.debug("I have " + msgs.size() + " messages in channel");
      assertTrue(msgs.isEmpty());

      tx.commit();

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
               Factory.createMessage("m0", false, "payload0"),
               Factory.createMessage("m1", false, "payload1"),
               Factory.createMessage("m2", false, "payload2"),
            };

      Transaction tx = tm.createTransaction();

      channel.handle(observer, m[0], tx);
      channel.handle(observer, m[1], tx);
      channel.handle(observer, m[2], tx);

      assertTrue(receiver.getMessages().isEmpty());
      assertTrue(channel.browse().isEmpty());

      tx.rollback();

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
               Factory.createMessage("m0", true, "payload0"),
               Factory.createMessage("m1", true, "payload1"),
               Factory.createMessage("m2", true, "payload2"),
            };


      Transaction tx = tm.createTransaction();

      channel.handle(observer, m[0], tx);
      channel.handle(observer, m[1], tx);
      channel.handle(observer, m[2], tx);

      assertTrue(receiver.getMessages().isEmpty());
      assertTrue(channel.browse().isEmpty());

      tx.commit();

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
               Factory.createMessage("m0", true, "payload0"),
               Factory.createMessage("m1", true, "payload1"),
               Factory.createMessage("m2", true, "payload2"),
            };



      Transaction tx = tm.createTransaction();

      channel.handle(observer, m[0], tx);
      channel.handle(observer, m[1], tx);
      channel.handle(observer, m[2], tx);

      assertTrue(receiver.getMessages().isEmpty());
      assertTrue(channel.browse().isEmpty());

      tx.rollback();

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
               Factory.createMessage("m0", false, "payload0"),
               Factory.createMessage("m1", false, "payload1"),
               Factory.createMessage("m2", false, "payload2"),
            };

      channel.handle(observer, m[0], null);
      channel.handle(observer, m[1], null);
      channel.handle(observer, m[2], null);

      List l = channel.browse();
      assertEquals(3, l.size());

      Transaction tx = tm.createTransaction();

      receiver.acknowledge(m[0], tx);
      receiver.acknowledge(m[1], tx);
      receiver.acknowledge(m[2], tx);

      l = channel.browse();
      assertEquals(3, l.size());

      tx.commit();

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
               Factory.createMessage("m0", false, "payload0"),
               Factory.createMessage("m1", false, "payload1"),
               Factory.createMessage("m2", false, "payload2"),
            };

      channel.handle(observer, m[0], null);
      channel.handle(observer, m[1], null);
      channel.handle(observer, m[2], null);

      List l = channel.browse();
      assertEquals(3, l.size());

      Transaction tx = tm.createTransaction();

      receiver.acknowledge(m[0], tx);
      receiver.acknowledge(m[1], tx);
      receiver.acknowledge(m[2], tx);

      l = channel.browse();
      assertEquals(3, l.size());

      tx.rollback();


      l = channel.browse();
      assertEqualSets(m, l);
   }



   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
