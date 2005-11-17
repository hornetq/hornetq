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
package org.jboss.test.messaging.core.persistence;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.message.PersistentMessageStore;
import org.jboss.messaging.core.persistence.JDBCPersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleChannel;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CoreMessageJDBCPersistenceManagerTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------
   
   protected MessageStore ms;
   
   protected Channel channel;
   
   protected JDBCPersistenceManager pm;

   // Constructors --------------------------------------------------

   public CoreMessageJDBCPersistenceManagerTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {

      super.setUp();
      ServerManagement.init("all");

      pm = new JDBCPersistenceManager();
      ms = new PersistentMessageStore("persistentMessageStore0", pm);
      channel = new SimpleChannel("channel0", ms);
      
   }

   public void tearDown() throws Exception
   {
      ServerManagement.deInit();
      super.tearDown();
   }
   
   public void testAddRemoveDeliveryNonTx() throws Exception
   {

      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
         
         Delivery del = new SimpleDelivery(channel, ref);
         
         pm.addDelivery(channel.getChannelID(), del);
         
         List dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         
         assertNotNull(dels);
         assertEquals(1, dels.size());
         String messageID = (String)dels.get(0);
         
         assertEquals(ref.getMessageID(), messageID);
         
         boolean removed = pm.removeDelivery(channel.getChannelID(), del, null);
         
         assertTrue(removed);
         
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertTrue(dels.isEmpty());
      }
           
   }
   
   public void testAddRemoveMessageRefNonTx() throws Exception
   {
      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];     
         MessageReference ref = ms.reference(m);
         
         pm.addReference(channel.getChannelID(), ref, null);
         
         List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         
         assertNotNull(refs);
         assertEquals(1, refs.size());
         String messageID = (String)refs.get(0);
         
         assertEquals(ref.getMessageID(), messageID);
         
         boolean removed = pm.removeReference(channel.getChannelID(), ref);
         
         assertTrue(removed);            
         
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertTrue(refs.isEmpty());
      }
      
   }
   
   public void testAddRetrieveRemoveMessage() throws Exception
   {
      Message[] messages = createMessages();

      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         String id = (String)m.getMessageID();

         pm.storeMessage(m);
         assertEquals(1, pm.getMessageReferenceCount(id));

         Message m2 = pm.retrieveMessage(id);
         assertNotNull(m2);
         checkEqual(m, m2);
         assertEquals(2, pm.getMessageReferenceCount(id));

         boolean removed = pm.removeMessage(id);
         assertTrue(removed);
         assertEquals(1, pm.getMessageReferenceCount(id));

         removed = pm.removeMessage(id);
         assertTrue(removed);
         assertEquals(0, pm.getMessageReferenceCount(id));

         Message m3 = pm.retrieveMessage(id);
         assertNull(m3);

         assertFalse(pm.removeMessage(id));
      }
   }

   public void testGetDeliveries() throws Exception
   {
      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
         
         Delivery del = new SimpleDelivery(channel, ref);
         
         pm.addDelivery(channel.getChannelID(), del);
      }
         
      List dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
      assertNotNull(dels);
      assertEquals(messages.length, dels.size());
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];         
         assertTrue(dels.contains(m.getMessageID()));         
      }
      
      pm.removeAllMessageData(channel.getChannelID());
      dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
      assertNotNull(dels);
      assertTrue(dels.isEmpty());
      
   }
   
   public void testGetMessageReferences() throws Exception
   {
      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
                           
         pm.addReference(channel.getChannelID(), ref, null);
      }
         
      List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(messages.length, refs.size());
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];         
         assertTrue(refs.contains(m.getMessageID()));         
      }
      
      pm.removeAllMessageData(channel.getChannelID());
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertTrue(refs.isEmpty());
 
   }
   
   
   public void testRemoveAllMessageData() throws Exception
   {
      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
         
         Delivery del = new SimpleDelivery(channel, ref);
         
         pm.addDelivery(channel.getChannelID(), del);
                           
         pm.addReference(channel.getChannelID(), ref, null);
      }
         
      List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(messages.length, refs.size());
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];         
         assertTrue(refs.contains(m.getMessageID()));         
      }
      
      List dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
      assertNotNull(dels);
      assertEquals(messages.length, dels.size());
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];         
         assertTrue(dels.contains(m.getMessageID()));         
      }
      
      pm.removeAllMessageData(channel.getChannelID());
            
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertTrue(refs.isEmpty());
      
      dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
      assertNotNull(dels);
      assertTrue(dels.isEmpty());
      
 
   }
   
   public void testTransactionCommit() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         
         Message[] messages = createMessages();     
         
         Message m1 = messages[0];
         Message m2 = messages[1];
         Message m3 = messages[2];      
         Message m4 = messages[3];
         Message m5 = messages[4];
         Message m6 = messages[5];
         Message m7 = messages[6];      
         Message m8 = messages[7];
         Message m9 = messages[8];
         Message m10 = messages[9];
         
         TransactionRepository txRep = new TransactionRepository(pm); 
         Transaction tx = txRep.createTransaction();
         
         MessageReference ref1 = ms.reference(m1);
         MessageReference ref2 = ms.reference(m2);  
         MessageReference ref3 = ms.reference(m3);       
         MessageReference ref4 = ms.reference(m4);
         MessageReference ref5 = ms.reference(m5);  
         MessageReference ref6 = ms.reference(m6);
         MessageReference ref7 = ms.reference(m7);       
         MessageReference ref8 = ms.reference(m8);
         MessageReference ref9 = ms.reference(m9);  
         MessageReference ref10 = ms.reference(m10);
         
         Delivery del6 = new SimpleDelivery(channel, ref6);
         Delivery del7 = new SimpleDelivery(channel, ref7);
         Delivery del8 = new SimpleDelivery(channel, ref8);
         Delivery del9 = new SimpleDelivery(channel, ref9);
         Delivery del10 = new SimpleDelivery(channel, ref10);
               
         //Add first two refs non transactionally
         pm.addReference(channel.getChannelID(), ref1, null);
         pm.addReference(channel.getChannelID(), ref2, null);
         
         //check they're there
         List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));      
         
         //Add the next 3 refs transactionally
         pm.addReference(channel.getChannelID(), ref3, tx);
         pm.addReference(channel.getChannelID(), ref4, tx);
         pm.addReference(channel.getChannelID(), ref5, tx);
              
         //Check they're not visible
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));  
         
         //Add deliveries non transactionally
         pm.addDelivery(channel.getChannelID(), del6);
         pm.addDelivery(channel.getChannelID(), del7);
         pm.addDelivery(channel.getChannelID(), del8);
         pm.addDelivery(channel.getChannelID(), del9);
         pm.addDelivery(channel.getChannelID(), del10);
         
         //Check they're there
         List dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //Remove first three transactionally
         pm.removeDelivery(channel.getChannelID(), del6, tx);
         pm.removeDelivery(channel.getChannelID(), del7, tx);
         pm.removeDelivery(channel.getChannelID(), del8, tx);
   
         //Check changes are not visible
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //commit transaction
         pm.commitTx(tx);
         
         //check we can see all 5 refs
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(5, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));    
         assertTrue(refs.contains(ref3.getMessageID()));
         assertTrue(refs.contains(ref4.getMessageID()));  
         assertTrue(refs.contains(ref5.getMessageID()));
         
         //Check we can see only 2 deliveries
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(2, dels.size());
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID())); 
         
         pm.removeAllMessageData(channel.getChannelID());
      }
      
   }
   
   
   public void testTransactionRollback() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         
         Message[] messages = createMessages();     
         
         Message m1 = messages[0];
         Message m2 = messages[1];
         Message m3 = messages[2];      
         Message m4 = messages[3];
         Message m5 = messages[4];
         Message m6 = messages[5];
         Message m7 = messages[6];      
         Message m8 = messages[7];
         Message m9 = messages[8];
         Message m10 = messages[9];
         
         TransactionRepository txRep = new TransactionRepository(pm); 
         Transaction tx = txRep.createTransaction();
         
         MessageReference ref1 = ms.reference(m1);
         MessageReference ref2 = ms.reference(m2);  
         MessageReference ref3 = ms.reference(m3);       
         MessageReference ref4 = ms.reference(m4);
         MessageReference ref5 = ms.reference(m5);  
         MessageReference ref6 = ms.reference(m6);
         MessageReference ref7 = ms.reference(m7);       
         MessageReference ref8 = ms.reference(m8);
         MessageReference ref9 = ms.reference(m9);  
         MessageReference ref10 = ms.reference(m10);
         
         Delivery del6 = new SimpleDelivery(channel, ref6);
         Delivery del7 = new SimpleDelivery(channel, ref7);
         Delivery del8 = new SimpleDelivery(channel, ref8);
         Delivery del9 = new SimpleDelivery(channel, ref9);
         Delivery del10 = new SimpleDelivery(channel, ref10);
               
         //Add first two refs non transactionally
         pm.addReference(channel.getChannelID(), ref1, null);
         pm.addReference(channel.getChannelID(), ref2, null);
         
         //check they're there
         List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));      
         
         //Add the next 3 refs transactionally
         pm.addReference(channel.getChannelID(), ref3, tx);
         pm.addReference(channel.getChannelID(), ref4, tx);
         pm.addReference(channel.getChannelID(), ref5, tx);
              
         //Check they're not visible
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));  
         
         //Add deliveries non transactionally
         pm.addDelivery(channel.getChannelID(), del6);
         pm.addDelivery(channel.getChannelID(), del7);
         pm.addDelivery(channel.getChannelID(), del8);
         pm.addDelivery(channel.getChannelID(), del9);
         pm.addDelivery(channel.getChannelID(), del10);
         
         //Check they're there
         List dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //Remove first three transactionally
         pm.removeDelivery(channel.getChannelID(), del6, tx);
         pm.removeDelivery(channel.getChannelID(), del7, tx);
         pm.removeDelivery(channel.getChannelID(), del8, tx);
   
         //Check changes are not visible
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //rollback transaction
         pm.rollbackTx(tx);
         
         //check we can see only two
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));
         
         //Check we can see all 5 deliveries
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         pm.removeAllMessageData(channel.getChannelID());
      }
      
   }
      
   
   public void testTransactionCommitXA() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         
         Message[] messages = createMessages();     
         
         Message m1 = messages[0];
         Message m2 = messages[1];
         Message m3 = messages[2];      
         Message m4 = messages[3];
         Message m5 = messages[4];
         Message m6 = messages[5];
         Message m7 = messages[6];      
         Message m8 = messages[7];
         Message m9 = messages[8];
         Message m10 = messages[9];
         
         TransactionRepository txRep = new TransactionRepository(pm); 
         
         Transaction tx = txRep.createTransaction(new MockXid());
         
         MessageReference ref1 = ms.reference(m1);
         MessageReference ref2 = ms.reference(m2);  
         MessageReference ref3 = ms.reference(m3);       
         MessageReference ref4 = ms.reference(m4);
         MessageReference ref5 = ms.reference(m5);  
         MessageReference ref6 = ms.reference(m6);
         MessageReference ref7 = ms.reference(m7);       
         MessageReference ref8 = ms.reference(m8);
         MessageReference ref9 = ms.reference(m9);  
         MessageReference ref10 = ms.reference(m10);
         
         Delivery del6 = new SimpleDelivery(channel, ref6);
         Delivery del7 = new SimpleDelivery(channel, ref7);
         Delivery del8 = new SimpleDelivery(channel, ref8);
         Delivery del9 = new SimpleDelivery(channel, ref9);
         Delivery del10 = new SimpleDelivery(channel, ref10);
               
         //Add first two refs non transactionally
         pm.addReference(channel.getChannelID(), ref1, null);
         pm.addReference(channel.getChannelID(), ref2, null);
         
         //check they're there
         List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));      
         
         //Add the next 3 refs transactionally
         pm.addReference(channel.getChannelID(), ref3, tx);
         pm.addReference(channel.getChannelID(), ref4, tx);
         pm.addReference(channel.getChannelID(), ref5, tx);
              
         //Check they're not visible
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));  
         
         //Add deliveries non transactionally
         pm.addDelivery(channel.getChannelID(), del6);
         pm.addDelivery(channel.getChannelID(), del7);
         pm.addDelivery(channel.getChannelID(), del8);
         pm.addDelivery(channel.getChannelID(), del9);
         pm.addDelivery(channel.getChannelID(), del10);
         
         //Check they're there
         List dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //Remove first three transactionally
         pm.removeDelivery(channel.getChannelID(), del6, tx);
         pm.removeDelivery(channel.getChannelID(), del7, tx);
         pm.removeDelivery(channel.getChannelID(), del8, tx);
   
         //Check changes are not visible
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //commit transaction
         pm.commitTx(tx);
         
         //check we can see all 5 refs
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(5, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));    
         assertTrue(refs.contains(ref3.getMessageID()));
         assertTrue(refs.contains(ref4.getMessageID()));  
         assertTrue(refs.contains(ref5.getMessageID()));
         
         //Check we can see only 2 deliveries
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(2, dels.size());
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID())); 
         
         pm.removeAllMessageData(channel.getChannelID());
      }
      
   }
   
   
   public void testTransactionRollbackXA() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         
         Message[] messages = createMessages();     
         
         Message m1 = messages[0];
         Message m2 = messages[1];
         Message m3 = messages[2];      
         Message m4 = messages[3];
         Message m5 = messages[4];
         Message m6 = messages[5];
         Message m7 = messages[6];      
         Message m8 = messages[7];
         Message m9 = messages[8];
         Message m10 = messages[9];
         
         TransactionRepository txRep = new TransactionRepository(pm); 
         Transaction tx = txRep.createTransaction(new MockXid());
         
         MessageReference ref1 = ms.reference(m1);
         MessageReference ref2 = ms.reference(m2);  
         MessageReference ref3 = ms.reference(m3);       
         MessageReference ref4 = ms.reference(m4);
         MessageReference ref5 = ms.reference(m5);  
         MessageReference ref6 = ms.reference(m6);
         MessageReference ref7 = ms.reference(m7);       
         MessageReference ref8 = ms.reference(m8);
         MessageReference ref9 = ms.reference(m9);  
         MessageReference ref10 = ms.reference(m10);
         
         Delivery del6 = new SimpleDelivery(channel, ref6);
         Delivery del7 = new SimpleDelivery(channel, ref7);
         Delivery del8 = new SimpleDelivery(channel, ref8);
         Delivery del9 = new SimpleDelivery(channel, ref9);
         Delivery del10 = new SimpleDelivery(channel, ref10);
               
         //Add first two refs non transactionally
         pm.addReference(channel.getChannelID(), ref1, null);
         pm.addReference(channel.getChannelID(), ref2, null);
         
         //check they're there
         List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));      
         
         //Add the next 3 refs transactionally
         pm.addReference(channel.getChannelID(), ref3, tx);
         pm.addReference(channel.getChannelID(), ref4, tx);
         pm.addReference(channel.getChannelID(), ref5, tx);
              
         //Check they're not visible
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));  
         
         //Add deliveries non transactionally
         pm.addDelivery(channel.getChannelID(), del6);
         pm.addDelivery(channel.getChannelID(), del7);
         pm.addDelivery(channel.getChannelID(), del8);
         pm.addDelivery(channel.getChannelID(), del9);
         pm.addDelivery(channel.getChannelID(), del10);
         
         //Check they're there
         List dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //Remove first three transactionally
         pm.removeDelivery(channel.getChannelID(), del6, tx);
         pm.removeDelivery(channel.getChannelID(), del7, tx);
         pm.removeDelivery(channel.getChannelID(), del8, tx);
   
         //Check changes are not visible
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         //rollback transaction
         pm.rollbackTx(tx);
         
         //check we can see only two
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(ref1.getMessageID()));
         assertTrue(refs.contains(ref2.getMessageID()));
         
         //Check we can see all 5 deliveries
         dels = pm.deliveries(ms.getStoreID(), channel.getChannelID());
         assertNotNull(dels);
         assertEquals(5, dels.size());
         assertTrue(dels.contains(ref6.getMessageID()));
         assertTrue(dels.contains(ref7.getMessageID())); 
         assertTrue(dels.contains(ref8.getMessageID()));
         assertTrue(dels.contains(ref9.getMessageID()));
         assertTrue(dels.contains(ref10.getMessageID()));
         
         pm.removeAllMessageData(channel.getChannelID());
      }
      
   }
   
   public void retrievePreparedTransactions() throws Exception
   {
      TransactionRepository txRep = new TransactionRepository(pm);
            
      Message[] messages = createMessages();  
      
      Xid[] xids = new Xid[messages.length];
      
      for (int i = 0; i < messages.length; i++)
      {         
         Transaction tx = txRep.createTransaction();
         MessageReference ref = ms.reference(messages[i]);
         pm.addReference(channel.getChannelID(), ref, tx);         
      }
      
      
      List txs = pm.retrievePreparedTransactions();
      assertNotNull(txs);
      assertEquals(messages.length, txs.size());
      
      for (int i = 0; i < xids.length; i++)
      {
         Xid xid = xids[i];
         assertTrue(txs.contains(xid));
      }
      
      pm.removeAllMessageData(channel.getChannelID());
      
   }
   
   protected Message[] createMessages() throws Exception
   {
      //Generate some messages with a good range of attribute values
      Message[] messages = new Message[10];
      for (int i = 0; i < 10; i++)
      {
         Map headers = generateHeadersFilledWithCrap(true);
                  
         messages[i] =
            new CoreMessage(new GUID().toString(),
               true,
               System.currentTimeMillis() + 1000 * 60 * 60,
               System.currentTimeMillis(),
               i,
               headers,            
               new WibblishObject());
      }
      return messages;
   }
   
   protected void checkEqual(Message m1, Message m2) throws Exception
   {
      if (m1 == m2)
      {
         fail();
      }
      
      if (m1 == null || m2 == null)
      {
         fail();
      }
      
      if (!m1.getClass().equals(m2.getClass()))
      {
         fail();
      }
      
      //Attributes from org.jboss.messaging.core.Message
      assertEquals(m1.getMessageID(), m2.getMessageID());
      assertEquals(m1.isReference(), m2.isReference());
      assertEquals(m1.isReliable(), m2.isReliable());
      assertEquals(m1.getExpiration(), m2.getExpiration());
      assertEquals(m1.isExpired(), m2.isExpired());
      assertEquals(m1.getTimestamp(), m2.getTimestamp());
      assertEquals(m1.getPriority(), m2.getPriority());
      assertEquals(m1.isRedelivered(), m2.isRedelivered());
      Map m1Headers = m1.getHeaders();
      Map m2Headers = m2.getHeaders();
      checkHeadersEquals(m1Headers, m2Headers);
      checkHeadersEquals(m2Headers, m1Headers);
      assertEquals(m1.getPayload(), m2.getPayload());
      
   }
   
   protected void checkHeadersEquals(Map headers1, Map headers2)
   {
      Iterator iter = headers1.entrySet().iterator();
      while (iter.hasNext())
      {
         Map.Entry entry1 = (Map.Entry)iter.next();
         Object value2 = headers2.get(entry1.getKey());
         assertNotNull(value2);
         if (value2 instanceof byte[])
         {
            checkByteArraysEqual((byte[])entry1.getValue(), (byte[])value2);
         }
         else
         {
            assertEquals(entry1.getValue(), value2);
         }
      }
   }
   
   public static class WibblishObject implements Serializable
   {
      private static final long serialVersionUID = -822739710811857027L;
      public String wibble;
      public WibblishObject()
      {
         this.wibble = new GUID().toString();
      }
      public boolean equals(Object other)
      {
         if (!(other instanceof WibblishObject))
         {
            return false;
         }
         WibblishObject oo = (WibblishObject)other;
         return oo.wibble.equals(this.wibble);
      }
   }
   
   protected Map generateHeadersFilledWithCrap(boolean useObject)
   {
      Map headers = new HashMap();
      for (int j = 0; j < 27; j++)
      {
         //put some crap in the map
         int k;
         if (useObject)
         {
            k = j % 11;
         }
         else
         {
            k = j % 10;
         }
         
         switch (k)
         {
            case 0:
               headers.put(new GUID().toString(), new GUID().toString());
            case 1:
               headers.put(new GUID().toString(), randByte());
            case 2:
               headers.put(new GUID().toString(), randShort());
            case 3:
               headers.put(new GUID().toString(), randInt());
            case 4:
               headers.put(new GUID().toString(), randLong());
            case 5:
               headers.put(new GUID().toString(), randBool());
            case 6:
               headers.put(new GUID().toString(), randFloat());
            case 7:
               headers.put(new GUID().toString(), randDouble());
            case 8:
               headers.put(new GUID().toString(), randLong());
            case 9:
               headers.put(new GUID().toString(), randByteArray());
            case 10:
               headers.put(new GUID().toString(), new WibblishObject());               
         }
      }
      return headers;
   }
   
   protected Byte randByte()
   {
      return new Byte((byte)(Math.random() * (2^8 - 1) - (2^7)));
   }
   
   protected Short randShort()
   {
      return new Short((short)(Math.random() * (2^16 - 1) - (2^15)));
   }
   
   protected Integer randInt()
   {
      return new Integer((int)(Math.random() * (2^32 - 1) - (2^31)));
   }
   
   protected Long randLong()
   {
      return new Long((long)(Math.random() * (2^64 - 1) - (2^64)));
   }
   
   protected Boolean randBool()
   {
      return new Boolean(Math.random() > 0.5 ? true : false);
   }
   
   protected Float randFloat()
   {
      return new Float((float)(Math.random() * 1000000));
   }
   
   protected Double randDouble()
   {
      return new Double(Math.random() * 1000000);
   }
   
   protected byte[] randByteArray()
   {
      String s = new GUID().toString();
      return s.getBytes();
   }
   
   protected void checkByteArraysEqual(byte[] b1, byte[] b2)
   {
      if (b1 == null || b2 == null)
      {
         fail();
      }
      if (b1.length != b2.length)
      {
         fail();
      }
      
      for (int i = 0; i < b1.length; i++)
      {
         assertEquals(b1[i], b2[i]);
      }
      
   }
   
   protected class MockXid implements Xid
   {
      byte[] branchQual;
      int formatID;
      byte[] globalTxId;
      
      protected MockXid()
      {
         branchQual = new GUID().toString().getBytes();
         formatID = randInt().intValue();
         globalTxId = new GUID().toString().getBytes();
      }

      public byte[] getBranchQualifier()
      {
         return branchQual;
      }

      public int getFormatId()
      {
         return formatID;
      }

      public byte[] getGlobalTransactionId()
      {
         return globalTxId;
      }
      
   }
}



