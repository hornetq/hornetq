/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.base;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.ChannelState;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.State;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.PagingMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.tm.TransactionManagerService;
import org.jboss.util.id.GUID;

/**
 * 
 * A PagingStateTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * PagingStateTest.java,v 1.1 2006/03/05 16:17:24 timfox Exp
 */
public class PagingStateTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 10;

   // Static --------------------------------------------------------
   
   private static final int FULL_SIZE1 = 100;
   private static final int PAGE_SIZE1 = 20;
   
   private static final int FULL_SIZE2 = 50;
   private static final int PAGE_SIZE2 = 20;
   
   private static final int DOWNCACHE_SIZE = 10;
   
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;
   protected PersistenceManager pm;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public PagingStateTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();

      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());

      ((JDBCPersistenceManager)pm).start();

      ms = new PagingMessageStore("store1");
      
      if (FULL_SIZE2 >= FULL_SIZE1)
      {
         throw new IllegalStateException("Invalid size");
      }
     
   }
   
   
   public void tearDown() throws Exception
   {
      log.info("pm is:" + pm);
      ((JDBCPersistenceManager)pm).stop();
      pm = null;
      sc.stop();
      sc = null;
      ms = null;
      
      super.tearDown();
   }
   
   
   //First set of tests have downCache = 0
   
   public void test1_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      //Send FULL_SIZE non persistent refs to state
      //Verify none are paged to storage
      //Consumer them all
      //Verify none are left in storage
      
      MessageReference[] refs = new MessageReference[FULL_SIZE1];
      
      for (int i = 0; i < FULL_SIZE1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         refs[i] = ref;
      }
      
      //Check no refs are in storage
      List refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      List msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());      
      
      //Check msgs are in the store
      assertEquals(FULL_SIZE1, ms.size());
      
      //Now consume them
      for (int i = 0; i < FULL_SIZE1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check no refs are in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty()); 
      
      //Check no msgs are in the store
      assertEquals(0, ms.size());
      
   }
   
   public void test2_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      
      //Send FULL_SIZE + 1 non persistent refs to state
      //Verify last one is paged to storage
      //Consumer them all
      //Verify none are left in storage
          
      MessageReference[] refs = new MessageReference[FULL_SIZE1 + 1];
      
      for (int i = 0; i < FULL_SIZE1 + 1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         refs[i] = ref;
      }
      
      //Check one ref
      List refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(1, refIds.size());
      assertTrue(refIds.contains(refs[FULL_SIZE1].getMessageID()));
            
      //Check one message is in storage
      List msgIds = getMessageIds();
      assertEquals(1, refIds.size());
      assertTrue(refIds.contains(refs[FULL_SIZE1].getMessageID()));     
      
      //Check all but one msgs are in the store
      assertEquals(FULL_SIZE1, ms.size());
            
      //Now consume them
      for (int i = 0; i < FULL_SIZE1 + 1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check no refs are in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
      
      //Check no msgs are in the store
      assertEquals(0, ms.size());
      
   }
   
   public void test3_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      
      //Send FULL_SIZE + PAGE_SIZE non persistent refs to state
      //Verify PAGE_SIZE refs are paged to storage
      //Consumer them all
      //Verify none are left in storage
          
      MessageReference[] refs = new MessageReference[FULL_SIZE1 + PAGE_SIZE1];
      
      for (int i = 0; i < FULL_SIZE1 + PAGE_SIZE1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         refs[i] = ref;
      }
      
      
      //Check PAGE_SIZE refs in storage
      List refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(PAGE_SIZE1, refIds.size());
      
      Iterator iter = refIds.iterator();
      int count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String refId = (String)iter.next();
         assertEquals(refId, refs[count].getMessageID());
         count++;
      }
                  
      //Check PAGE_SIZE messages is in storage
      List msgIds = getMessageIds();
      assertEquals(PAGE_SIZE1, msgIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
            
      assertEquals(FULL_SIZE1, ms.size());
      
      
      //Now consume them
      for (int i = 0; i < FULL_SIZE1 + PAGE_SIZE1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check no refs are in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
      
      //Check no msgs are in the store
      assertEquals(0, ms.size());
      
   }
   
   public void test4_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      
      //Send FULL_SIZE + 2 * PAGE_SIZE  + 1 non persistent refs to state
      //Verify 2 * PAGE_SIZE  + 1 refs are paged to storage
      //Consumer them all
      //Verify none are left in storage
          
      MessageReference[] refs = new MessageReference[FULL_SIZE1 + 2 * PAGE_SIZE1 + 1];
      
      for (int i = 0; i < FULL_SIZE1 + 2 * PAGE_SIZE1 + 1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         refs[i] = ref;
      }
      
      
      //Check 2 * PAGE_SIZE + 1 refs in storage
      List refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(2 * PAGE_SIZE1 + 1, refIds.size());
      
      Iterator iter = refIds.iterator();
      int count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String refId = (String)iter.next();
         assertEquals(refId, refs[count].getMessageID());
         count++;
      }
                  
      //Check PAGE_SIZE messages is in storage
      List msgIds = getMessageIds();
      assertEquals(2 * PAGE_SIZE1 + 1, msgIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
      
      assertEquals(FULL_SIZE1, ms.size());
      
      
      //Now consume them
      for (int i = 0; i < FULL_SIZE1 + 2 * PAGE_SIZE1 + 1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check no refs are in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
      
      //Check no msgs are in the store
      assertEquals(0, ms.size());      
   }
   
   public void test5_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      
      //Send FULL_SIZE + PAGE_SIZE / 2  non persistent refs to state
      //Verify FULL_SIZE + PAGE_SIZE / 2 refs are paged to storage
      //Consume PAGE_SIZE / 2  - 1 messages
      //Verify refs are still in storage
      //Consume one more
      //Verify PAGE_SIZE /2 messages rare loaded from storage
      //Consumer them all
      //Verify none are left in storage
          
      MessageReference[] refs = new MessageReference[FULL_SIZE1 + PAGE_SIZE1 / 2];
      
      for (int i = 0; i < FULL_SIZE1 + PAGE_SIZE1 / 2; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
          
         state1.addReference(ref);
         
         refs[i] = ref;
      }
      
      
      //Check refs in storage
      List refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(PAGE_SIZE1 / 2, refIds.size());
      
      Iterator iter = refIds.iterator();
      int count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String refId = (String)iter.next();
         assertEquals(refId, refs[count].getMessageID());
         count++;
      }
                  
      //Check messages is in storage
      List msgIds = getMessageIds();
      assertEquals(PAGE_SIZE1 / 2, msgIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
      
      //Consume PAGE_SIZE / 2  - 1 refs
            
      for (int i = 0; i < PAGE_SIZE1 / 2 - 1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check refs and messages still there
      
      //Check refs in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(PAGE_SIZE1 / 2, refIds.size());
      
      iter = refIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String refId = (String)iter.next();
         assertEquals(refId, refs[count].getMessageID());
         count++;
      }
                  
      //Check messages is in storage
      msgIds = getMessageIds();
      assertEquals(PAGE_SIZE1 / 2, refIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
         
      //Consumer one more ref
      MessageReference ref = state1.removeFirstInMemory();
      assertNotNull(ref);
      assertEquals(refs[PAGE_SIZE1 / 2 - 1].getMessageID(), ref.getMessageID());
      Delivery del = new SimpleDelivery(channel1, ref, false);
      state1.addDelivery(del);
      state1.acknowledge(del);
      
      //Check nothing in storage
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
      
      //Consume the rest
      
      for (int i = PAGE_SIZE1 / 2; i < FULL_SIZE1 + PAGE_SIZE1 / 2; i++)
      {
         ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check nothing in storage
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
           
      //Check no msgs are in the store
      assertEquals(0, ms.size());  
      
   }
   
   public void test6_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      
      //Send FULL_SIZE + PAGE_SIZE + 1  non persistent refs to state
      //Verify FULL_SIZE + PAGE_SIZE  + 1 refs are paged to storage
      //Consume PAGE_SIZE - 1 refs
      //Verify refs are still in storage
      //Consume one more
      //Verify PAGE_SIZE messages rare loaded from storage
      //Consumer them all
      //Verify none are left in storage
          
      MessageReference[] refs = new MessageReference[FULL_SIZE1 + PAGE_SIZE1 + 1];
      
      for (int i = 0; i < FULL_SIZE1 + PAGE_SIZE1 + 1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         refs[i] = ref;
      }
      
      
      //Check refs in storage
      List refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(PAGE_SIZE1 + 1, refIds.size());
      
      Iterator iter = refIds.iterator();
      int count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String refId = (String)iter.next();
         assertEquals(refId, refs[count].getMessageID());
         count++;
      }
                  
      //Check messages is in storage
      List msgIds = getMessageIds();
      assertEquals(PAGE_SIZE1 + 1, msgIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
      
      //Consume PAGE_SIZE - 1 refs
            
      for (int i = 0; i < PAGE_SIZE1 - 1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check refs and messages still there
      
      //Check refs in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(PAGE_SIZE1 + 1, refIds.size());
      
      iter = refIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String refId = (String)iter.next();
         assertEquals(refId, refs[count].getMessageID());
         count++;
      }
                  
      //Check messages is in storage
      msgIds = getMessageIds();
      assertEquals(PAGE_SIZE1 + 1, msgIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
      
      //Consumer one more ref
      MessageReference ref = state1.removeFirstInMemory();
      assertNotNull(ref);
      assertEquals(refs[PAGE_SIZE1 - 1].getMessageID(), ref.getMessageID());
      Delivery del = new SimpleDelivery(channel1, ref, false);
      state1.addDelivery(del);
      state1.acknowledge(del);
      
      //There should be one ref in storage
      
      refIds = getReferenceIds(channel1.getChannelID());
      
      assertEquals(1, refIds.size());
      
      assertEquals(refs[FULL_SIZE1 + PAGE_SIZE1].getMessageID(), (String)refIds.get(0));
      
      //Check one message is in storage
      msgIds = getMessageIds();
      assertEquals(1, msgIds.size());   
      assertEquals(refs[FULL_SIZE1 + PAGE_SIZE1].getMessageID(), (String)msgIds.get(0));
      
      //Consumer one more
      ref = state1.removeFirstInMemory();
      assertNotNull(ref);
      assertEquals(refs[PAGE_SIZE1].getMessageID(), ref.getMessageID());
      del = new SimpleDelivery(channel1, ref, false);
      state1.addDelivery(del);
      state1.acknowledge(del);
      
      //Should be no messages in storage now
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
      
      //Add a couple more messages - verify they don't go to storage
      Message mNew1 = MessageFactory.createCoreMessage(new GUID().toString(), false, null);              
      MessageReference refNew1 = ms.reference(mNew1);      
      state1.addReference(refNew1);
      Message mNew2 = MessageFactory.createCoreMessage(new GUID().toString(), false, null);              
      MessageReference refNew2 = ms.reference(mNew2);      
      state1.addReference(refNew2);
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());
     
                       
      //Consume the rest
      
      for (int i = PAGE_SIZE1 + 1; i < FULL_SIZE1 + PAGE_SIZE1 + 1; i++)
      {
         ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //And the last 2
      ref = state1.removeFirstInMemory();
      assertNotNull(ref);
      assertEquals(refNew1.getMessageID(), ref.getMessageID());
      del = new SimpleDelivery(channel1, ref, false);
      state1.addDelivery(del);
      state1.acknowledge(del);
      
      ref = state1.removeFirstInMemory();
      assertNotNull(ref);
      assertEquals(refNew2.getMessageID(), ref.getMessageID());
      del = new SimpleDelivery(channel1, ref, false);
      state1.addDelivery(del);
      state1.acknowledge(del);
      
      //Check nothing in storage
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
           
      //Check no msgs are in the store
      assertEquals(0, ms.size());        
   }
   
   
   public void test7_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      Channel channel2 = new Queue(2, ms, pm, true, FULL_SIZE2, PAGE_SIZE2, 0);
      
      State state2 = new ChannelState(channel2, pm, true, true, FULL_SIZE2, PAGE_SIZE2, 0);
 
  
      
      //Test with same message in 2 channels
      
      //Send message in two channels
      //Spillover one of them
      //Verify that message ref and message is spilled over
      //Verify that message still in store
      //Spill over second one
      //Verify that message is spilled over too and no message in store
      //Consume all messages
      
      Message[] msgs = new Message[FULL_SIZE1 + 1];
      
      for (int i = 0; i < FULL_SIZE2; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref1 = ms.reference(m);
         
         state1.addReference(ref1);
         
         MessageReference ref2 = ms.reference(m);
         
         state2.addReference(ref2);
         
         msgs[i] = m;
      }
      
      //Check no refs are in storage
      List refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      refIds = getReferenceIds(channel2.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      List msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());   

      //Check msgs are in the store
      assertEquals(FULL_SIZE2, ms.size());
      
      //Send another message
      
      Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
      
      msgs[FULL_SIZE2] = m;
      
      MessageReference ref1 = ms.reference(m);
      
      state1.addReference(ref1);
      
      MessageReference ref2 = ms.reference(m);
      
      state2.addReference(ref2);
      
      // Check one ref is in storage
      refIds = getReferenceIds(channel1.getChannelID());
          
      assertTrue(refIds.isEmpty());
            
      refIds = getReferenceIds(channel2.getChannelID());
      assertEquals(1, refIds.size());
      assertEquals(ref2.getMessageID(), (String)refIds.get(0));
            
      //Check message in storage
      msgIds = getMessageIds();
      assertEquals(1, msgIds.size());
      assertEquals(ref2.getMessageID(), (String)refIds.get(0));
      
      //Check messages still in memory store      
      assertEquals(FULL_SIZE2 + 1, ms.size());
      
      //Spillover the first channel
      
      for (int i = FULL_SIZE2 + 1; i < FULL_SIZE1 + 1; i++)
      {
         m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         ref1 = ms.reference(m);
         
         state1.addReference(ref1);
         
         ref2 = ms.reference(m);
         
         state2.addReference(ref2);
         
         msgs[i] = m;
      }
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(1, refIds.size());
            
      refIds = getReferenceIds(channel2.getChannelID());
      assertEquals(FULL_SIZE1 + 1 - FULL_SIZE2, refIds.size());
      
      //Consume all the messages
      
      for (int i = 0; i < FULL_SIZE1 + 1; i++)
      { 
         MessageReference ref = state1.removeFirstInMemory();   
         assertNotNull(ref);
         assertEquals(msgs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
        
   }
   
   
   public void test8_NDC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
  
      
      //Add some persistent messages, restart server and make sure state loads up ok
      
      Message[] msgs = new Message[100];
      
      for (int i = 0; i < 100; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), true, null);        
         
         MessageReference ref1 = ms.reference(m);
         
         state1.addReference(ref1);
         
         msgs[i] = m;         
      }
      
      //Shutdown and restart      
      
      ((JDBCPersistenceManager)pm).stop();
      
      ms.stop();
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());

      ((JDBCPersistenceManager)pm).start();
      
      ms = new PagingMessageStore("store1");
            
      channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, 0);

      state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, 0);
     
      state1.load();
              
      //Consume all the messages
      
      for (int i = 0; i < 100; i++)
      { 
         MessageReference ref = state1.removeFirstInMemory();   
         assertNotNull(ref);
         assertEquals(msgs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
        
   }
   
   //Second set of tests have active down cache
   
   public void test1_DC() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, DOWNCACHE_SIZE);
  
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, DOWNCACHE_SIZE);
  
      //Send FULL_SIZE non persistent refs to state
      //Verify none are paged to storage
      //Consumer them all
      //Verify none are left in storage
      
      MessageReference[] refs = new MessageReference[FULL_SIZE1];
      
      for (int i = 0; i < FULL_SIZE1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         refs[i] = ref;
      }
      
      //Check no refs are in storage
      List refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      List msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());      
      
      //Check msgs are in the store
      assertEquals(FULL_SIZE1, ms.size());
      
      //Now consume them
      for (int i = 0; i < FULL_SIZE1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check no refs are in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty()); 
      
      //Check no msgs are in the store
      assertEquals(0, ms.size());
      
   }
   
   public void testDownCache() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, DOWNCACHE_SIZE);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, DOWNCACHE_SIZE);
        
      //Send FULL_SIZE + 1 non persistent refs to state
      //Verify none page to storage
      //Add DOWNCACHE_SIZE - 1 more refs
      //Verify DOWNCACHE_SIZE in storage
      //Consumer them all
      //Verify none are left in storage
          
      MessageReference[] refs = new MessageReference[FULL_SIZE1 + DOWNCACHE_SIZE];
      
      for (int i = 0; i < FULL_SIZE1 + 1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
               
         refs[i] = ref;
      }
      
      List refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      List msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());
      
      //Add more refs
      for (int i = 0; i < DOWNCACHE_SIZE - 1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
           
         refs[FULL_SIZE1 + 1 + i] = ref;
      }
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(DOWNCACHE_SIZE, refIds.size());
      Iterator iter = refIds.iterator();
      int count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String id = (String)iter.next();
         assertEquals(refs[count].getMessageID(), id);
         count++;
      }
      msgIds = getMessageIds();
      assertEquals(DOWNCACHE_SIZE, msgIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
          
      //Now consume them
      for (int i = 0; i < FULL_SIZE1 + DOWNCACHE_SIZE; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check no refs are in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());     
      
      //Check no msgs are in the store
      assertEquals(0, ms.size());
      
   }
      
   public void testDownCacheMixed() throws Throwable
   {
      Channel channel1 = new Queue(1, ms, pm, true, FULL_SIZE1, PAGE_SIZE1, DOWNCACHE_SIZE);
      
      State state1 = new ChannelState(channel1, pm, true, true, FULL_SIZE1, PAGE_SIZE1, DOWNCACHE_SIZE);
        
      //Send FULL_SIZE non persistent refs to state
      //Verify none page to storage
      //Add DOWNCACHE_SIZE - 1 more non persistent refs
      //Add DOWNCACHE_SIZE more persistent refs
      //Verify DOWNCACHE_SIZE all persistent refs in storage
      //Consume them all
      //Verify none are left in storage
      
      //This checks the downcache is flushed properly before a load
          
      MessageReference[] refs = new MessageReference[FULL_SIZE1 + 2 * DOWNCACHE_SIZE - 1];
      
      for (int i = 0; i < FULL_SIZE1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
               
         refs[i] = ref;
      }
      
      List refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      List msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());
      
      log.info("Adding np refs:");
      
      //Add more np refs
      for (int i = 0; i < DOWNCACHE_SIZE - 1; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), false, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         log.info("Added ref:" + ref.getMessageID());
           
         refs[FULL_SIZE1 + i] = ref;
      }
      
      //Add some persistent refs
      
      log.info("Adding p refs");
      
      for (int i = 0; i < DOWNCACHE_SIZE; i++)
      {
         Message m = MessageFactory.createCoreMessage(new GUID().toString(), true, null);        
         
         MessageReference ref = ms.reference(m);
         
         state1.addReference(ref);
         
         log.info("Added p ref:" + ref.getMessageID());
           
         refs[FULL_SIZE1 + DOWNCACHE_SIZE - 1 + i] = ref;
      }
      
      //Only the persistent refs should be in the db
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertEquals(DOWNCACHE_SIZE, refIds.size());
      Iterator iter = refIds.iterator();
      int count = FULL_SIZE1 + DOWNCACHE_SIZE - 1;
      while (iter.hasNext())
      {
         String id = (String)iter.next();
         assertEquals(refs[count].getMessageID(), id);         
         count++;
      }
      msgIds = getMessageIds();
      assertEquals(DOWNCACHE_SIZE, msgIds.size());
      iter = msgIds.iterator();
      count = FULL_SIZE1 + DOWNCACHE_SIZE - 1;
      while (iter.hasNext())
      {
         String msgId = (String)iter.next();
         assertTrue(refIds.contains(msgId));
      }
          
      //Now consume them
      for (int i = 0; i < FULL_SIZE1 + 2 * DOWNCACHE_SIZE - 1; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel1, ref, false);
         state1.addDelivery(del);
         state1.acknowledge(del);
      }
      
      //Check no refs are in storage
      refIds = getReferenceIds(channel1.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Check no messages are in storage
      msgIds = getMessageIds();
      log.info("msgs ids size:" + msgIds.size());
      //log.info("msgids:" + msgIds);
      
      log.info("Remaining message ids:");
      iter = msgIds.iterator();
      while (iter.hasNext())
      {
         log.info(iter.next());
      }
      
      assertTrue(msgIds.isEmpty());     
      
      //Check no msgs are in the store
      assertEquals(0, ms.size());
      
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
 
   protected List getReferenceIds(long channelId) throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID, ORD FROM MESSAGE_REFERENCE WHERE CHANNELID=? ORDER BY ORD";
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setLong(1, channelId);
   
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         String msgId = rs.getString(1);
         msgIds.add(msgId);
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      
      return msgIds;
   }
   
   protected List getMessageIds() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID FROM MESSAGE ORDER BY MESSAGEID";
      PreparedStatement ps = conn.prepareStatement(sql);
      
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         String msgId = rs.getString(1);
         msgIds.add(msgId);
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      
      return msgIds;
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
