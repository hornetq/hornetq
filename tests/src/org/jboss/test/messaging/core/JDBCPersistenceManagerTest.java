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
package org.jboss.test.messaging.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.contract.Channel;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.JDBCPersistenceManager;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * JDBCPersistenceManagerTest.java,v 1.1 2006/02/22 17:33:44 timfox Exp
 */
public class JDBCPersistenceManagerTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;
   
   protected JDBCPersistenceManager pm;
   
   protected MessageStore ms;
   
   
   // Constructors --------------------------------------------------

   public JDBCPersistenceManagerTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("This test is not supposed to run remotely!");
      }

      super.setUp();
      
      ServerManagement.stop();

      sc = new ServiceContainer("all");
      sc.start();                      
   }
   
   protected void doSetup(boolean batch, boolean useBinaryStream,
                          boolean trailingByte, int maxParams) throws Throwable
   {
      pm = createPM(batch, useBinaryStream, trailingByte, maxParams);         
      ms = new SimpleMessageStore();      
   }
   
   protected JDBCPersistenceManager createPM(boolean batch, boolean useBinaryStream,
                                             boolean trailingByte, int maxParams) throws Throwable
   {      
      JDBCPersistenceManager p =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, batch, useBinaryStream, trailingByte, maxParams, 5000, !sc.getDatabaseName().equals("oracle"));
      ((JDBCPersistenceManager)p).injectNodeID(1);
      p.start();
      return p;
   }

   public void tearDown() throws Exception
   {
      sc.stop();
      sc = null;
      
      pm.stop();
      ms.stop();
      super.tearDown();
   }
      
   public void testAddRemoveGetReferences_Batch() throws Throwable
   {
      addRemoveGetReferences(true);
   }
   
   public void testAddRemoveGetReferences_NoBatch() throws Throwable
   {
      addRemoveGetReferences(false);
   }
   
   public void testAddRemoveReference() throws Throwable
   {
      doSetup(false, false, false, 100);
      
      Channel channel1 = new SimpleChannel(0, ms);
      Channel channel2 = new SimpleChannel(1, ms);

      Message[] messages = createMessages(10);     
      
      for (int i = 0; i < 5; i++)
      {
         Message m1 = messages[i * 2];
         Message m2 = messages[i * 2 + 1];
         
         MessageReference ref1_1 = ms.reference(m1);
         MessageReference ref1_2 = ms.reference(m1);
                
         MessageReference ref2_1 = ms.reference(m2);
         MessageReference ref2_2 = ms.reference(m2);
                  
         pm.addReference(channel1.getChannelID(), ref1_1, null);
         pm.addReference(channel1.getChannelID(), ref2_1, null);         
         
         pm.addReference(channel2.getChannelID(), ref1_2, null);
         pm.addReference(channel2.getChannelID(), ref2_2, null);
      
         List refs = getReferenceIds(channel1.getChannelID());
         
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(new Long(m1.getMessageID())));
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         refs = getReferenceIds(channel2.getChannelID());
         
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(new Long(m1.getMessageID())));
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         pm.reapUnreferencedMessages();
         List msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         assertTrue(msgs.contains(new Long(m1.getMessageID())));
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
                  
         pm.removeReference(channel1.getChannelID(), ref1_1, null);
                  
         refs = getReferenceIds(channel1.getChannelID());
         assertNotNull(refs);
         assertEquals(1, refs.size());
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         refs = getReferenceIds(channel2.getChannelID());
         assertNotNull(refs);
         assertEquals(2, refs.size());
         assertTrue(refs.contains(new Long(m1.getMessageID())));
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         pm.reapUnreferencedMessages();
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         assertTrue(msgs.contains(new Long(m1.getMessageID())));
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
         
         pm.removeReference(channel2.getChannelID(), ref1_2, null);
         
         refs = getReferenceIds(channel1.getChannelID());
         assertNotNull(refs);
         assertEquals(1, refs.size());
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         refs = getReferenceIds(channel2.getChannelID());
         assertNotNull(refs);
         assertEquals(1, refs.size());         
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         pm.reapUnreferencedMessages();
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(1, msgs.size()); 
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
         
         pm.removeReference(channel1.getChannelID(), ref2_1, null);
         
         refs = getReferenceIds(channel1.getChannelID());
         assertNotNull(refs);
         assertTrue(refs.isEmpty());
         
         refs = getReferenceIds(channel2.getChannelID());
         assertNotNull(refs);
         assertEquals(1, refs.size());         
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         pm.reapUnreferencedMessages();
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
         
         pm.removeReference(channel2.getChannelID(), ref2_2, null);
         
         refs = getReferenceIds(channel1.getChannelID());
         assertNotNull(refs);
         assertTrue(refs.isEmpty());
         
         refs = getReferenceIds(channel2.getChannelID());
         assertNotNull(refs);
         assertTrue(refs.isEmpty());
         
         pm.reapUnreferencedMessages();
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
            
      }
   }
   
   // Trailing zero
   // -----------------
   
   // Binary Stream
   
   //non batch
   
   public void testCommit_NotXA_Long_NB_BS_TZ() throws Throwable
   {
      doTransactionCommit(false, false, true, true);
   }
   
   public void testCommit_XA_Long_NB_BS_TZ() throws Throwable
   {
      doTransactionCommit(true, false, true, true);
   }

   public void testRollback_NotXA_Long_NB_BS_TZ() throws Throwable
   {
      doTransactionRollback(false, false, true, true);
   }
       
   public void testRollback_XA_Long_NB_BS_TZ() throws Throwable
   {
      doTransactionRollback(true, false, true, true);
   }
   

   //batch
   
   public void testCommit_NotXA_Long_B_BS_TZ() throws Throwable
   {
      doTransactionCommit(false, true, true, true);
   }
     
   public void testCommit_XA_Long_B_BS_TZ() throws Throwable
   {
      doTransactionCommit(true, true, true, true);
   }

   public void testRollback_NotXA_Long_B_BS_TZ() throws Throwable
   {
      doTransactionRollback(false, true, true, true);
   }
        
   public void testRollback_XA_Long_B_BS_TZ() throws Throwable
   {
      doTransactionRollback(true, true, true, true);
   }
   
   // No binary stream
   
   //non batch
   
   public void testCommit_NotXA_Long_NB_BNS_TZ() throws Throwable
   {
      doTransactionCommit(false, false, false, true);
   }
   
   public void testCommit_XA_Long_NB_NBS_TZ() throws Throwable
   {
      doTransactionCommit(true, false, false, true);
   }

   public void testRollback_NotXA_Long_NB_NBS_TZ() throws Throwable
   {
      doTransactionRollback(false, false, false, true);
   }
       
   public void testRollback_XA_Long_NB_NBS_TZ() throws Throwable
   {
      doTransactionRollback(true, false, false, true);
   }
   

   //batch
   
   public void testCommit_NotXA_Long_B_NBS_TZ() throws Throwable
   {
      doTransactionCommit(false, true, false, true);
   }
     
   public void testCommit_XA_Long_B_NBS_TZ() throws Throwable
   {
      doTransactionCommit(true, true, false, true);
   }

   public void testRollback_NotXA_Long_B_NBS_TZ() throws Throwable
   {
      doTransactionRollback(false, true, false, true);
   }
        
   public void testRollback_XA_Long_B_NBS_TZ() throws Throwable
   {
      doTransactionRollback(true, true, false, true);
   }
   
       
   // Non trailing zero
   // -----------------
   
   // Binary Stream
   
   //non batch
   
   public void testCommit_NotXA_Long_NB_BS_NTZ() throws Throwable
   {
      doTransactionCommit(false, false, true, false);
   }
   
   public void testCommit_XA_Long_NB_BS_NTZ() throws Throwable
   {
      doTransactionCommit(true, false, true, false);
   }

   public void testRollback_NotXA_Long_NB_BS_NTZ() throws Throwable
   {
      doTransactionRollback(false, false, true, false);
   }
       
   public void testRollback_XA_Long_NB_BS_NTZ() throws Throwable
   {
      doTransactionRollback(true, false, true, false);
   }
   

   //batch
   
   public void testCommit_NotXA_Long_B_BS_NTZ() throws Throwable
   {
      doTransactionCommit(false, true, true, false);
   }
     
   public void testCommit_XA_Long_B_BS_NTZ() throws Throwable
   {
      doTransactionCommit(true, true, true, false);
   }

   public void testRollback_NotXA_Long_B_BS_NTZ() throws Throwable
   {
      doTransactionRollback(false, true, true, false);
   }
        
   public void testRollback_XA_Long_B_BS_NTZ() throws Throwable
   {
      doTransactionRollback(true, true, true, false);
   }
   
   // No binary stream
   
   //non batch
   
   public void testCommit_NotXA_Long_NB_BNS_NTZ() throws Throwable
   {
      doTransactionCommit(false, false, false, false);
   }
   
   public void testCommit_XA_Long_NB_NBS_NTZ() throws Throwable
   {
      doTransactionCommit(true, false, false, false);
   }

   public void testRollback_NotXA_Long_NB_NBS_NTZ() throws Throwable
   {
      doTransactionRollback(false, false, false, false);
   }
       
   public void testRollback_XA_Long_NB_NBS_NTZ() throws Throwable
   {
      doTransactionRollback(true, false, false, false);
   }
   

   //batch
   
   public void testCommit_NotXA_Long_B_NBS_NTZ() throws Throwable
   {
      doTransactionCommit(false, true, false, false);
   }
     
   public void testCommit_XA_Long_B_NBS_NTZ() throws Throwable
   {
      doTransactionCommit(true, true, false, false);
   }

   public void testRollback_NotXA_Long_B_NBS_NTZ() throws Throwable
   {
      doTransactionRollback(false, true, false, false);
   }
        
   public void testRollback_XA_Long_B_NBS_NTZ() throws Throwable
   {
      doTransactionRollback(true, true, false, false);
   }
   
   
   
   
   protected void addRemoveGetReferences(boolean batch) throws Throwable
   {
      doSetup(false, false, false, 100);
      
      Channel channel1 = new SimpleChannel(0, ms);
      
      Channel channel2 = new SimpleChannel(1, ms);
      
      Message[] m = createMessages(10);
      
      MessageReference ref1 = ms.reference(m[0]);
      MessageReference ref2 = ms.reference(m[1]);
      MessageReference ref3 = ms.reference(m[2]);
      MessageReference ref4 = ms.reference(m[3]);
      MessageReference ref5 = ms.reference(m[4]);
      MessageReference ref6 = ms.reference(m[5]);
      MessageReference ref7 = ms.reference(m[6]);
      MessageReference ref8 = ms.reference(m[7]);
      MessageReference ref9 = ms.reference(m[8]);
      MessageReference ref10 = ms.reference(m[9]);
      
      MessageReference ref11 = ms.reference(m[0]);
      MessageReference ref12 = ms.reference(m[1]);
      MessageReference ref13 = ms.reference(m[2]);
      MessageReference ref14 = ms.reference(m[3]);
      MessageReference ref15 = ms.reference(m[4]);
      
      List refs = new ArrayList();
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      refs.add(ref4);
      refs.add(ref5);
      refs.add(ref6);
      refs.add(ref7);
      refs.add(ref8);
      refs.add(ref9);
      refs.add(ref10);
      
      pm.pageReferences(channel1.getChannelID(), refs, false);
      
      refs = new ArrayList();
      refs.add(ref11);
      refs.add(ref12);
      refs.add(ref13);
      refs.add(ref14);
      refs.add(ref15);
    
      pm.pageReferences(channel2.getChannelID(), refs, false);
                  
      List refIds = getReferenceIds(channel1.getChannelID());
      assertNotNull(refIds);
      assertEquals(10, refIds.size());
      assertTrue(refIds.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessage().getMessageID())));
      
      refIds = getReferenceIds(channel2.getChannelID());
      assertNotNull(refIds);
      assertEquals(5, refIds.size());
      assertTrue(refIds.contains(new Long(ref11.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref12.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref13.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref14.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref15.getMessage().getMessageID())));
     
      
      pm.reapUnreferencedMessages();
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessage().getMessageID())));
      
      List msgIds = new ArrayList();
      msgIds.add(new Long(ref3.getMessage().getMessageID()));
      msgIds.add(new Long(ref4.getMessage().getMessageID()));
      msgIds.add(new Long(ref7.getMessage().getMessageID()));
      msgIds.add(new Long(ref9.getMessage().getMessageID()));
      msgIds.add(new Long(ref1.getMessage().getMessageID()));
      
      List ms = pm.getMessages(msgIds);
      assertNotNull(ms);
      assertEquals(5, ms.size());
      assertTrue(containsMessage(ms, ref3.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref4.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref7.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref9.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref1.getMessage().getMessageID()));
      
      refs = new ArrayList();
      refs.add(ref12);
      refs.add(ref13);
      refs.add(ref14);
      refs.add(ref15);
      pm.removeDepagedReferences(channel2.getChannelID(), refs);
      
      refIds = getReferenceIds(channel2.getChannelID());
      assertNotNull(refIds);
      assertEquals(1, refIds.size());
      assertTrue(refIds.contains(new Long(ref11.getMessage().getMessageID())));
      
      pm.reapUnreferencedMessages();
      ms = getMessageIds();

      assertNotNull(ms);
      assertEquals(10, ms.size());
      
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessage().getMessageID())));
     
      
      refs = new ArrayList();
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      pm.removeDepagedReferences(channel1.getChannelID(), refs);
      
      refIds = getReferenceIds(channel1.getChannelID());
      assertNotNull(refIds);
      assertEquals(7, refIds.size());
      assertTrue(refIds.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessage().getMessageID())));
     
      pm.reapUnreferencedMessages();
      ms = getMessageIds();
        
      assertNotNull(ms);
      assertEquals(8, ms.size());
      
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessage().getMessageID())));
      
      refs = new ArrayList();
      refs.add(ref11);
      pm.removeDepagedReferences(channel2.getChannelID(), refs);
      
      refs = new ArrayList();
      refs.add(ref4);
      refs.add(ref5);
      refs.add(ref6);
      refs.add(ref7);
      refs.add(ref8);
      refs.add(ref9);
      refs.add(ref10);
      pm.removeDepagedReferences(channel1.getChannelID(), refs);
      
      pm.reapUnreferencedMessages();
      ms = getMessageIds();
      assertNotNull(ms);
      assertEquals(0, ms.size());
   }
   
   public void testPageOrders() throws Throwable
   {
      doSetup(false, false, false, 100);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message[] m = createMessages(10);
      
      List refs = new ArrayList();
      
      MessageReference ref1 = ms.reference(m[0]);
      MessageReference ref2 = ms.reference(m[1]);
      MessageReference ref3 = ms.reference(m[2]);
      MessageReference ref4 = ms.reference(m[3]);
      MessageReference ref5 = ms.reference(m[4]);
      MessageReference ref6 = ms.reference(m[5]);
      MessageReference ref7 = ms.reference(m[6]);
      MessageReference ref8 = ms.reference(m[7]);
      MessageReference ref9 = ms.reference(m[8]);
      MessageReference ref10 = ms.reference(m[9]);
      
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      refs.add(ref4);
      refs.add(ref5);
      refs.add(ref6);
      refs.add(ref7);
      refs.add(ref8);
      refs.add(ref9);
      refs.add(ref10);
      
      pm.pageReferences(channel.getChannelID(), refs, false); 
      
      ref1.setPagingOrder(0);
      ref2.setPagingOrder(1);
      ref3.setPagingOrder(2);
      ref4.setPagingOrder(3);
      ref5.setPagingOrder(4);
      ref6.setPagingOrder(5);
      ref7.setPagingOrder(6);
      ref8.setPagingOrder(7);
      ref9.setPagingOrder(8);
      ref10.setPagingOrder(9);
      
      pm.updatePageOrder(channel.getChannelID(), refs);
      
      List refInfos = pm.getPagedReferenceInfos(channel.getChannelID(), 0, 10);
      
      assertNotNull(refInfos);
      
      assertEquals(10, refInfos.size());
      
      assertEquals(ref1.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(0)).getMessageId());
      assertEquals(ref2.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(1)).getMessageId());
      assertEquals(ref3.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(2)).getMessageId());
      assertEquals(ref4.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(3)).getMessageId());
      assertEquals(ref5.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(4)).getMessageId());
      assertEquals(ref6.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(5)).getMessageId());
      assertEquals(ref7.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(6)).getMessageId());
      assertEquals(ref8.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(7)).getMessageId());
      assertEquals(ref9.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(8)).getMessageId());
      assertEquals(ref10.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(9)).getMessageId());
      
      refInfos = pm.getPagedReferenceInfos(channel.getChannelID(), 3, 5);
      
      assertNotNull(refInfos);
      
      assertEquals(5, refInfos.size());
      
      assertEquals(ref4.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(0)).getMessageId());
      assertEquals(ref5.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(1)).getMessageId());
      assertEquals(ref6.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(2)).getMessageId());
      assertEquals(ref7.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(3)).getMessageId());
      assertEquals(ref8.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(4)).getMessageId());
    
      pm.updateReferencesNotPagedInRange(channel.getChannelID(), 0, 3, 4);
      
      refInfos = pm.getPagedReferenceInfos(channel.getChannelID(), 5, 5);
      
      assertNotNull(refInfos);
      
      assertEquals(5, refInfos.size());
          
      assertEquals(ref6.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(0)).getMessageId());
      assertEquals(ref7.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(1)).getMessageId());
      assertEquals(ref8.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(2)).getMessageId());
      assertEquals(ref9.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(3)).getMessageId());
      assertEquals(ref10.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(4)).getMessageId());                
   }
     
   public void testGetMessages() throws Throwable
   {
      doSetup(false, false, false, 100);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message[] m = createMessages(10);
      
      MessageReference ref1 = ms.reference(m[0]);
      MessageReference ref2 = ms.reference(m[1]);
      MessageReference ref3 = ms.reference(m[2]);
      MessageReference ref4 = ms.reference(m[3]);
      MessageReference ref5 = ms.reference(m[4]);
      MessageReference ref6 = ms.reference(m[5]);
      MessageReference ref7 = ms.reference(m[6]);
      MessageReference ref8 = ms.reference(m[7]);
      MessageReference ref9 = ms.reference(m[8]);
      MessageReference ref10 = ms.reference(m[9]);
      
      pm.addReference(channel.getChannelID(), ref1, null);
      pm.addReference(channel.getChannelID(), ref2, null);
      pm.addReference(channel.getChannelID(), ref3, null);
      pm.addReference(channel.getChannelID(), ref4, null);
      pm.addReference(channel.getChannelID(), ref5, null);
      pm.addReference(channel.getChannelID(), ref6, null);
      pm.addReference(channel.getChannelID(), ref7, null);
      pm.addReference(channel.getChannelID(), ref8, null);
      pm.addReference(channel.getChannelID(), ref9, null);
      pm.addReference(channel.getChannelID(), ref10, null);
      
      List refIds = getReferenceIds(channel.getChannelID());
      assertNotNull(refIds);
      assertEquals(10, refIds.size());
      assertTrue(refIds.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessage().getMessageID())));
      
      pm.reapUnreferencedMessages();
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessage().getMessageID())));
      
      List msgIds = new ArrayList();
      msgIds.add(new Long(ref3.getMessage().getMessageID()));
      msgIds.add(new Long(ref4.getMessage().getMessageID()));
      msgIds.add(new Long(ref7.getMessage().getMessageID()));
      msgIds.add(new Long(ref9.getMessage().getMessageID()));
      msgIds.add(new Long(ref1.getMessage().getMessageID()));
      
      List ms = pm.getMessages(msgIds);
      assertNotNull(ms);
      assertEquals(5, ms.size());
        
      assertTrue(containsMessage(ms, ref3.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref4.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref7.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref9.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref1.getMessage().getMessageID()));
        
   }
   
   public void testGetInitialRefInfos() throws Throwable
   {
      doSetup(false, false, false, 100);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message[] m = createMessages(10);
      
      List refs = new ArrayList();
      
      MessageReference ref1 = ms.reference(m[0]);
      MessageReference ref2 = ms.reference(m[1]);
      MessageReference ref3 = ms.reference(m[2]);
      MessageReference ref4 = ms.reference(m[3]);
      MessageReference ref5 = ms.reference(m[4]);
      MessageReference ref6 = ms.reference(m[5]);
      MessageReference ref7 = ms.reference(m[6]);
      MessageReference ref8 = ms.reference(m[7]);
      MessageReference ref9 = ms.reference(m[8]);
      MessageReference ref10 = ms.reference(m[9]);
      
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      refs.add(ref4);
      refs.add(ref5);
      refs.add(ref6);
      refs.add(ref7);
      refs.add(ref8);
      refs.add(ref9);
      refs.add(ref10);
      
      pm.pageReferences(channel.getChannelID(), refs, false); 
      
      //First load exactly 10
      PersistenceManager.InitialLoadInfo info = pm.loadFromStart(channel.getChannelID(), 10);
      
      assertNull(info.getMinPageOrdering());
      assertNull(info.getMaxPageOrdering());
      
      List refInfos = info.getRefInfos();
      
      assertNotNull(refInfos);
      assertEquals(10, refInfos.size());
      
      assertEquals(ref1.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(0)).getMessageId());
      assertEquals(ref2.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(1)).getMessageId());
      assertEquals(ref3.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(2)).getMessageId());
      assertEquals(ref4.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(3)).getMessageId());
      assertEquals(ref5.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(4)).getMessageId());
      assertEquals(ref6.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(5)).getMessageId());
      assertEquals(ref7.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(6)).getMessageId());
      assertEquals(ref8.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(7)).getMessageId());
      assertEquals(ref9.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(8)).getMessageId());
      assertEquals(ref10.getMessage().getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(9)).getMessageId());          
   }
      
   
   protected boolean containsMessage(List msgs, long msgId)
   {
      Iterator iter = msgs.iterator();
      while (iter.hasNext())
      {
         Message m = (Message)iter.next();
         if (m.getMessageID() == msgId)
         {
            return true;
         }           
      }
      return false;
   }
   
   public void testGetMessagesMaxParams() throws Throwable
   {
      doSetup(false, false, false, 5);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message[] m = createMessages(10);
      
      MessageReference ref1 = ms.reference(m[0]);
      MessageReference ref2 = ms.reference(m[1]);
      MessageReference ref3 = ms.reference(m[2]);
      MessageReference ref4 = ms.reference(m[3]);
      MessageReference ref5 = ms.reference(m[4]);
      MessageReference ref6 = ms.reference(m[5]);
      MessageReference ref7 = ms.reference(m[6]);
      MessageReference ref8 = ms.reference(m[7]);
      MessageReference ref9 = ms.reference(m[8]);
      MessageReference ref10 = ms.reference(m[9]);
      
      pm.addReference(channel.getChannelID(), ref1, null);
      pm.addReference(channel.getChannelID(), ref2, null);
      pm.addReference(channel.getChannelID(), ref3, null);
      pm.addReference(channel.getChannelID(), ref4, null);
      pm.addReference(channel.getChannelID(), ref5, null);
      pm.addReference(channel.getChannelID(), ref6, null);
      pm.addReference(channel.getChannelID(), ref7, null);
      pm.addReference(channel.getChannelID(), ref8, null);
      pm.addReference(channel.getChannelID(), ref9, null);
      pm.addReference(channel.getChannelID(), ref10, null);
      
      List refIds = getReferenceIds(channel.getChannelID());
      assertNotNull(refIds);
      assertEquals(10, refIds.size());
      assertTrue(refIds.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessage().getMessageID())));
      
      pm.reapUnreferencedMessages();
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessage().getMessageID())));
      
      List msgIds = new ArrayList();
      msgIds.add(new Long(ref3.getMessage().getMessageID()));
      msgIds.add(new Long(ref4.getMessage().getMessageID()));
      msgIds.add(new Long(ref7.getMessage().getMessageID()));
      msgIds.add(new Long(ref9.getMessage().getMessageID()));
      msgIds.add(new Long(ref1.getMessage().getMessageID()));
      
      List ms = pm.getMessages(msgIds);
      assertNotNull(ms);
      assertEquals(5, ms.size());
      assertTrue(containsMessage(ms, ref3.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref4.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref7.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref9.getMessage().getMessageID()));
      assertTrue(containsMessage(ms, ref1.getMessage().getMessageID()));   
   }
   
   
   protected Message createMessage(byte i, boolean reliable) throws Throwable
   {
      Map headers = generateFilledMap(true);

      return CoreMessageFactory.
         createCoreMessage(i,
                           reliable,
                           System.currentTimeMillis() + 1000 * 60 * 60,
                           System.currentTimeMillis(),
                           (byte)(i % 10),
                           headers,
                           i % 2 == 0 ? new WibblishObject() : null);
   }
   
   protected Message[] createMessages(int num) throws Throwable
   {
      //Generate some messages with a good range of attribute values
      Message[] messages = new Message[num];
      for (int i = 0; i < num; i++)
      {            
         messages[i] = createMessage((byte)i, true);
      }
      return messages;
   }
   
   protected void checkEquivalent(Message m1, Message m2) throws Throwable
   {
      if (m1 == m2)
      {
         fail();
      }
      
      if (m1 == null || m2 == null)
      {
         fail();
      }
      
      //Attributes from org.jboss.messaging.core.Message
      assertEquals(m1.getMessageID(), m2.getMessageID());
      assertEquals(m1.isReliable(), m2.isReliable());
      assertEquals(m1.getExpiration(), m2.getExpiration());
      assertEquals(m1.isExpired(), m2.isExpired());
      assertEquals(m1.getTimestamp(), m2.getTimestamp());
      assertEquals(m1.getPriority(), m2.getPriority());
      Map m1Headers = m1.getHeaders();
      Map m2Headers = m2.getHeaders();
      checkMapsEquivalent(m1Headers, m2Headers);
      checkMapsEquivalent(m2Headers, m1Headers);
      
      if (m1.getPayload() instanceof byte[] && m2.getPayload() instanceof byte[])
      {
         this.checkByteArraysEqual((byte[])m1.getPayload(), (byte[])m2.getPayload());
      }
      else if (m1.getPayload() instanceof Map && m2.getPayload() instanceof Map)
      {
         this.checkMapsEquivalent((Map)m1.getPayload(), (Map)m2.getPayload());
      }
      else if (m1.getPayload() instanceof List && m2.getPayload() instanceof List)
      {
         this.checkListsEquivalent((List)m1.getPayload(), (List)m2.getPayload());
      }
      else
      {      
         assertEquals(m1.getPayload(), m2.getPayload());
      }
      
   }
   
   protected void checkMapsEquivalent(Map headers1, Map headers2)
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
   
   protected void checkListsEquivalent(List l1, List l2)
   {      
      Iterator iter1 = l1.iterator();
      Iterator iter2 = l2.iterator();
      while (iter1.hasNext())
      {
         Object o1 = iter1.next();
         Object o2 = iter2.next();
         
         if (o1 instanceof byte[])
         {
            checkByteArraysEqual((byte[])o1, (byte[])o2);
         }
         else
         {
            assertEquals(o1, o2);
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
   
   protected HashMap generateFilledMap(boolean useObject)
   {
      HashMap headers = new HashMap();
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
               headers.put(new GUID().toString(), randString(1000));
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
               headers.put(new GUID().toString(), randByteArray(500));
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
      return new Boolean(Math.random() > 0.5);
   }
   
   protected Float randFloat()
   {
      return new Float((float)(Math.random() * 1000000));
   }
   
   protected Double randDouble()
   {
      return new Double(Math.random() * 1000000);
   }
   
   protected String randString(int length)
   {
      StringBuffer buf = new StringBuffer(length);
      for (int i = 0; i < length; i++)
      {
         buf.append(randChar().charValue());
      }
      return buf.toString();
   }
   
   protected byte[] randByteArray(int size)
   {
      String s = randString(size / 2);
      return s.getBytes();
   }
   
   protected Character randChar()
   {
      return new Character((char)randShort().shortValue());
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
      
      public boolean equals(Object other)
      {
         if (!(other instanceof Xid))
         {
            return false;
         }
         Xid xother = (Xid)other;
         if (xother.getFormatId() != this.formatID)
         {
            return false;
         }
         if (xother.getBranchQualifier().length != this.branchQual.length)
         {
            return false;
         }
         if (xother.getGlobalTransactionId().length != this.globalTxId.length)
         {
            return false;
         }
         for (int i = 0; i < this.branchQual.length; i++)
         {
            byte[] otherBQ = xother.getBranchQualifier();
            if (this.branchQual[i] != otherBQ[i])
            {
               return false;
            }
         }
         for (int i = 0; i < this.globalTxId.length; i++)
         {
            byte[] otherGtx = xother.getGlobalTransactionId();
            if (this.globalTxId[i] != otherGtx[i])
            {
               return false;
            }
         }
         return true;
      }
      
   }
   
   protected void doTransactionCommit(boolean xa, boolean batch, boolean useBinaryStream, boolean trailingByte) throws Throwable
   {
      doSetup(batch, useBinaryStream, trailingByte, 100);

      Channel channel = new SimpleChannel(0, ms);
      
      IDManager idm = new IDManager("TRANSACTION_ID", 10, pm);
      idm.start();
      
      TransactionRepository txRep = new TransactionRepository(pm, ms, idm);
      txRep.start();

      log.debug("transaction log started");

      Message[] messages = createMessages(10);
      
      Message m1 = messages[0];
      Message m2 = messages[1];
      Message m3 = messages[2];      
      Message m4 = messages[3];
      Message m5 = messages[4];

      Transaction tx = null;
      if (xa)
      {         
         tx = txRep.createTransaction(new MockXid());
      }
      else
      {
         tx = txRep.createTransaction();
      }
      
      if (xa)
      {
    	  assertEquals(1,txRep.getNumberOfRegisteredTransactions());
      }
      else
      {
    	  assertEquals(0,txRep.getNumberOfRegisteredTransactions());
      }
      
      MessageReference ref1 = ms.reference(m1);
      MessageReference ref2 = ms.reference(m2);  
      MessageReference ref3 = ms.reference(m3);       
      MessageReference ref4 = ms.reference(m4);
      MessageReference ref5 = ms.reference(m5);

      log.debug("adding references non-transactionally");

      // Add first two refs non transactionally
      pm.addReference(channel.getChannelID(), ref1, null);
      pm.addReference(channel.getChannelID(), ref2, null);
      
      //check they're there
      List refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessage().getMessageID())));
      
      pm.reapUnreferencedMessages();
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));

      log.debug("ref1 and ref2 are there");

      //Add the next 3 refs transactionally
      pm.addReference(channel.getChannelID(), ref3, tx);
      pm.addReference(channel.getChannelID(), ref4, tx);
      pm.addReference(channel.getChannelID(), ref5, tx);
      
      //Remove the other 2 transactionally
      pm.removeReference(channel.getChannelID(), ref1, tx);
      pm.removeReference(channel.getChannelID(), ref2, tx);
      
      //Check the changes aren't visible
      refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessage().getMessageID())));  
      
      pm.reapUnreferencedMessages();
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));
      
      //commit transaction
      tx.commit();

      assertEquals("numberOfRegisteredTransactions",0,txRep.getNumberOfRegisteredTransactions());
      
      //check we can see only the last 3 refs
      refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(3, refs.size()); 
      assertTrue(refs.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(refs.contains(new Long(ref4.getMessage().getMessageID())));  
      assertTrue(refs.contains(new Long(ref5.getMessage().getMessageID())));
      
      pm.reapUnreferencedMessages();
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(3, msgs.size());
      assertTrue(msgs.contains(new Long(ref3.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessage().getMessageID())));
   }
         
   protected void doTransactionRollback(boolean xa, boolean batch, boolean useBinaryStream, boolean trailingByte) throws Throwable
   {
      doSetup(batch, useBinaryStream, trailingByte, 100);

      Channel channel = new SimpleChannel(0, ms);
      
      IDManager idm = new IDManager("TRANSACTION_ID", 10, pm);
      idm.start();
      
      TransactionRepository txRep = new TransactionRepository(pm, ms, idm);
      txRep.start();
 
      Message[] messages = createMessages(10);     
      
      Message m1 = messages[0];
      Message m2 = messages[1];
      Message m3 = messages[2];      
      Message m4 = messages[3];
      Message m5 = messages[4];

      
      Transaction tx = null;
      if (xa)
      {
         tx = txRep.createTransaction(new MockXid());
      }
      else
      {
         tx = txRep.createTransaction();
      }
      
      MessageReference ref1 = ms.reference(m1);
      MessageReference ref2 = ms.reference(m2);  
      MessageReference ref3 = ms.reference(m3);       
      MessageReference ref4 = ms.reference(m4);
      MessageReference ref5 = ms.reference(m5);  

      //Add first two refs non transactionally
      pm.addReference(channel.getChannelID(), ref1, null);
      pm.addReference(channel.getChannelID(), ref2, null);
      
      //check they're there
      List refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessage().getMessageID())));      
      
      pm.reapUnreferencedMessages();
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));
      
      
      
      //Add the next 3 refs transactionally
      pm.addReference(channel.getChannelID(), ref3, tx);
      pm.addReference(channel.getChannelID(), ref4, tx);
      pm.addReference(channel.getChannelID(), ref5, tx);
      
      //Remove the other 2 transactionally
      pm.removeReference(channel.getChannelID(), ref1, tx);
      pm.removeReference(channel.getChannelID(), ref2, tx);
      
      //Check the changes aren't visible
      refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessage().getMessageID())));  
      
      pm.reapUnreferencedMessages();
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));
      
      //rollback transaction
      tx.rollback();
      
      refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessage().getMessageID())));  
      
      pm.reapUnreferencedMessages();
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessage().getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessage().getMessageID())));          
   }
   
   
  
}



