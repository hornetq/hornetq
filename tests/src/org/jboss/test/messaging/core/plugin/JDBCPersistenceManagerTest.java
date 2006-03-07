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
package org.jboss.test.messaging.core.plugin;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.PagingMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleChannel;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.tm.TransactionManagerService;
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

      sc = new ServiceContainer("all");
      sc.start();                
      
   }
   
   protected void doSetup(boolean batch) throws Exception
   {
      pm = createPM();      
      pm.setUsingBatchUpdates(batch);      
      ms = new PagingMessageStore("s0");    
      pm.start();
   }
   
   protected JDBCPersistenceManager createPM() throws Exception
   {
      return new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());
   }

   public void tearDown() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         sc.stop();
         sc = null;
      }
      pm.stop();
      super.tearDown();
   }
   
   public void testGetMaxOrdering() throws Exception
   {
      doSetup(false);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message[] m = createMessages(10);
      
      MessageReference ref1 = ms.reference(m[0]);
      ref1.setOrdering(1);
      MessageReference ref2 = ms.reference(m[1]);
      ref1.setOrdering(3);
      MessageReference ref3 = ms.reference(m[2]);
      ref1.setOrdering(6);
      MessageReference ref4 = ms.reference(m[3]);
      ref1.setOrdering(13);
      MessageReference ref5 = ms.reference(m[4]);
      ref1.setOrdering(15);
      MessageReference ref6 = ms.reference(m[5]);
      ref1.setOrdering(8);
      MessageReference ref7 = ms.reference(m[6]);
      ref1.setOrdering(23);
      MessageReference ref8 = ms.reference(m[7]);
      ref1.setOrdering(45);
      MessageReference ref9 = ms.reference(m[8]);
      ref1.setOrdering(10);
      MessageReference ref10 = ms.reference(m[9]);
      ref1.setOrdering(111);
      
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
      assertTrue(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessageID())));
      
      long maxOrdering = pm.getMaxOrdering(channel.getChannelID());
      
      assertEquals(111, maxOrdering);
      
      pm.removeAllChannelData(channel.getChannelID());
      
   }
   
   public void testGetNumberOfReferences() throws Exception
   {
      doSetup(false);
      
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
      assertTrue(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessageID())));
      
      int numberOfReferences = pm.getNumberOfReferences(channel.getChannelID());
      
      assertEquals(10, numberOfReferences);
      
      pm.removeReference(channel.getChannelID(), ref1, null);
      
      numberOfReferences = pm.getNumberOfReferences(channel.getChannelID());
      
      assertEquals(9, numberOfReferences);
       
      pm.removeAllChannelData(channel.getChannelID());
     
   }
   
   public void testGetReferenceInfos() throws Exception
   {
      doSetup(false);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message[] m = createMessages(10);
      
      MessageReference ref1 = ms.reference(m[0]);
      ref1.setOrdering(0);
      MessageReference ref2 = ms.reference(m[1]);
      ref2.setOrdering(1);
      MessageReference ref3 = ms.reference(m[2]);
      ref3.setOrdering(2);
      MessageReference ref4 = ms.reference(m[3]);
      ref4.setOrdering(11);
      MessageReference ref5 = ms.reference(m[4]);
      ref5.setOrdering(22);
      MessageReference ref6 = ms.reference(m[5]);
      ref6.setOrdering(100);
      MessageReference ref7 = ms.reference(m[6]);
      ref7.setOrdering(303);
      MessageReference ref8 = ms.reference(m[7]);
      ref8.setOrdering(1000);
      MessageReference ref9 = ms.reference(m[8]);
      ref9.setOrdering(1001);
      MessageReference ref10 = ms.reference(m[9]);
      ref10.setOrdering(1002);
      
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
      assertTrue(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessageID())));
      
      List refInfos = pm.getReferenceInfos(channel.getChannelID(), 0);
      
      assertNotNull(refInfos);
      assertEquals(0, refInfos.size());
      
      refInfos = pm.getReferenceInfos(channel.getChannelID(), 3);
      assertNotNull(refInfos);
      assertEquals(3, refInfos.size());
      assertEquals(ref1.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(0)).getMessageId());
      assertEquals(ref2.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(1)).getMessageId());
      assertEquals(ref3.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(2)).getMessageId());
      
      refInfos = pm.getReferenceInfos(channel.getChannelID(), 10);
      assertNotNull(refInfos);
      assertEquals(10, refInfos.size());
      assertEquals(ref1.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(0)).getMessageId());
      assertEquals(ref2.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(1)).getMessageId());
      assertEquals(ref3.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(2)).getMessageId());
      assertEquals(ref4.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(3)).getMessageId());
      assertEquals(ref5.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(4)).getMessageId());
      assertEquals(ref6.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(5)).getMessageId());
      assertEquals(ref7.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(6)).getMessageId());
      assertEquals(ref8.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(7)).getMessageId());
      assertEquals(ref9.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(8)).getMessageId());
      assertEquals(ref10.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(9)).getMessageId());
      
      refInfos = pm.getReferenceInfos(channel.getChannelID(), 19);
      assertNotNull(refInfos);
      assertEquals(10, refInfos.size());
      assertEquals(ref1.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(0)).getMessageId());
      assertEquals(ref2.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(1)).getMessageId());
      assertEquals(ref3.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(2)).getMessageId());
      assertEquals(ref4.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(3)).getMessageId());
      assertEquals(ref5.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(4)).getMessageId());
      assertEquals(ref6.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(5)).getMessageId());
      assertEquals(ref7.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(6)).getMessageId());
      assertEquals(ref8.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(7)).getMessageId());
      assertEquals(ref9.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(8)).getMessageId());
      assertEquals(ref10.getMessageID(), ((PersistenceManager.ReferenceInfo)refInfos.get(9)).getMessageId());
       
      pm.removeAllChannelData(channel.getChannelID());
     
   }
   
   
   public void testGetMessages() throws Exception
   {
      doSetup(false);
      
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
      assertTrue(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessageID())));
      
      List msgIds = new ArrayList();
      msgIds.add(new Long(ref3.getMessageID()));
      msgIds.add(new Long(ref4.getMessageID()));
      msgIds.add(new Long(ref7.getMessageID()));
      msgIds.add(new Long(ref9.getMessageID()));
      msgIds.add(new Long(ref1.getMessageID()));
      
      List ms = pm.getMessages(msgIds);
      assertNotNull(ms);
      assertEquals(5, ms.size());
        
      assertTrue(containsMessage(ms, ref3.getMessageID()));
      assertTrue(containsMessage(ms, ref4.getMessageID()));
      assertTrue(containsMessage(ms, ref7.getMessageID()));
      assertTrue(containsMessage(ms, ref9.getMessageID()));
      assertTrue(containsMessage(ms, ref1.getMessageID()));
      
      pm.removeAllChannelData(channel.getChannelID());    
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
   
   public void testGetMessagesMaxParams() throws Exception
   {
      doSetup(false);
      
      pm.setMaxParams(5);
      
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
      assertTrue(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessageID())));
      
      List msgIds = new ArrayList();
      msgIds.add(new Long(ref3.getMessageID()));
      msgIds.add(new Long(ref4.getMessageID()));
      msgIds.add(new Long(ref7.getMessageID()));
      msgIds.add(new Long(ref9.getMessageID()));
      msgIds.add(new Long(ref1.getMessageID()));
      
      List ms = pm.getMessages(msgIds);
      assertNotNull(ms);
      assertEquals(5, ms.size());
      assertTrue(containsMessage(ms, ref3.getMessageID()));
      assertTrue(containsMessage(ms, ref4.getMessageID()));
      assertTrue(containsMessage(ms, ref7.getMessageID()));
      assertTrue(containsMessage(ms, ref9.getMessageID()));
      assertTrue(containsMessage(ms, ref1.getMessageID()));
      
      pm.removeAllChannelData(channel.getChannelID());    
   }
   
   public void testAddReferences() throws Exception
   {
      doSetup(false);
      
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
      
      pm.addReferences(channel.getChannelID(), refs);
      
      List refIds = getReferenceIds(channel.getChannelID());
      assertNotNull(refIds);
      assertEquals(10, refIds.size());
      assertTrue(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(10, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessageID())));
      
      List msgIds = new ArrayList();
      msgIds.add(new Long(ref3.getMessageID()));
      msgIds.add(new Long(ref4.getMessageID()));
      msgIds.add(new Long(ref7.getMessageID()));
      msgIds.add(new Long(ref9.getMessageID()));
      msgIds.add(new Long(ref1.getMessageID()));
      
      List ms = pm.getMessages(msgIds);
      assertNotNull(ms);
      assertEquals(5, ms.size());
      assertTrue(containsMessage(ms, ref3.getMessageID()));
      assertTrue(containsMessage(ms, ref4.getMessageID()));
      assertTrue(containsMessage(ms, ref7.getMessageID()));
      assertTrue(containsMessage(ms, ref9.getMessageID()));
      assertTrue(containsMessage(ms, ref1.getMessageID()));
      
      pm.removeAllChannelData(channel.getChannelID());    
   }
   
   
   public void testRemoveNonPersistentMessages() throws Exception
   {
      doSetup(false);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message m1 = createMessage((byte)0, false);    
      Message m2 = createMessage((byte)1, true);
      Message m3 = createMessage((byte)2, true);
      Message m4 = createMessage((byte)3, false);
      Message m5 = createMessage((byte)4, false);
      Message m6 = createMessage((byte)5, false);
      Message m7 = createMessage((byte)6, true);
      Message m8 = createMessage((byte)7, true);
      Message m9 = createMessage((byte)8, false);
      Message m10 = createMessage((byte)9, true);
      Message m11 = createMessage((byte)10, false);
      Message m12 = createMessage((byte)11, false);
      Message m13 = createMessage((byte)12, false);
      Message m14 = createMessage((byte)13, false);
      Message m15 = createMessage((byte)14, true);
      Message m16 = createMessage((byte)15, true);
      Message m17 = createMessage((byte)16, false);
      Message m18 = createMessage((byte)17, false);
      Message m19 = createMessage((byte)18, false);
      Message m20 = createMessage((byte)19, true);
      
                  
      MessageReference ref1 = ms.reference(m1);
      ref1.setOrdering(1);
      MessageReference ref2 = ms.reference(m2);
      ref2.setOrdering(2);
      MessageReference ref3 = ms.reference(m3);
      ref3.setOrdering(3);
      MessageReference ref4 = ms.reference(m4);
      ref4.setOrdering(4);
      MessageReference ref5 = ms.reference(m5);
      ref5.setOrdering(5);
      MessageReference ref6 = ms.reference(m6);
      ref6.setOrdering(6);
      MessageReference ref7 = ms.reference(m7);
      ref7.setOrdering(7);
      MessageReference ref8 = ms.reference(m8);
      ref8.setOrdering(8);
      MessageReference ref9 = ms.reference(m9);
      ref9.setOrdering(9);
      MessageReference ref10 = ms.reference(m10);
      ref10.setOrdering(10);
      MessageReference ref11 = ms.reference(m11);
      ref11.setOrdering(11);
      MessageReference ref12 = ms.reference(m12);
      ref12.setOrdering(12);
      MessageReference ref13 = ms.reference(m13);
      ref13.setOrdering(13);
      MessageReference ref14 = ms.reference(m14);
      ref14.setOrdering(14);
      MessageReference ref15 = ms.reference(m15);
      ref15.setOrdering(15);
      MessageReference ref16 = ms.reference(m16);
      ref16.setOrdering(16);
      MessageReference ref17 = ms.reference(m17);
      ref17.setOrdering(17);
      MessageReference ref18 = ms.reference(m18);
      ref18.setOrdering(18);
      MessageReference ref19 = ms.reference(m19);
      ref19.setOrdering(19);
      MessageReference ref20 = ms.reference(m20);
      ref20.setOrdering(20);
      
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
      pm.addReference(channel.getChannelID(), ref11, null);
      pm.addReference(channel.getChannelID(), ref12, null);
      pm.addReference(channel.getChannelID(), ref13, null);
      pm.addReference(channel.getChannelID(), ref14, null);
      pm.addReference(channel.getChannelID(), ref15, null);
      pm.addReference(channel.getChannelID(), ref16, null);
      pm.addReference(channel.getChannelID(), ref17, null);
      pm.addReference(channel.getChannelID(), ref18, null);
      pm.addReference(channel.getChannelID(), ref19, null);
      pm.addReference(channel.getChannelID(), ref20, null);
      
      List refIds = getReferenceIds(channel.getChannelID());
      assertNotNull(refIds);
      assertEquals(20, refIds.size());
      assertTrue(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertTrue(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      assertTrue(refIds.contains(new Long(ref11.getMessageID())));
      assertTrue(refIds.contains(new Long(ref12.getMessageID())));
      assertTrue(refIds.contains(new Long(ref13.getMessageID())));
      assertTrue(refIds.contains(new Long(ref14.getMessageID())));
      assertTrue(refIds.contains(new Long(ref15.getMessageID())));
      assertTrue(refIds.contains(new Long(ref16.getMessageID())));
      assertTrue(refIds.contains(new Long(ref17.getMessageID())));
      assertTrue(refIds.contains(new Long(ref18.getMessageID())));
      assertTrue(refIds.contains(new Long(ref19.getMessageID())));
      assertTrue(refIds.contains(new Long(ref20.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(20, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      assertTrue(msgs.contains(new Long(ref6.getMessageID())));
      assertTrue(msgs.contains(new Long(ref7.getMessageID())));
      assertTrue(msgs.contains(new Long(ref8.getMessageID())));
      assertTrue(msgs.contains(new Long(ref9.getMessageID())));
      assertTrue(msgs.contains(new Long(ref10.getMessageID())));
      assertTrue(msgs.contains(new Long(ref11.getMessageID())));
      assertTrue(msgs.contains(new Long(ref12.getMessageID())));
      assertTrue(msgs.contains(new Long(ref13.getMessageID())));
      assertTrue(msgs.contains(new Long(ref14.getMessageID())));
      assertTrue(msgs.contains(new Long(ref15.getMessageID())));
      assertTrue(msgs.contains(new Long(ref16.getMessageID())));
      assertTrue(msgs.contains(new Long(ref17.getMessageID())));
      assertTrue(msgs.contains(new Long(ref18.getMessageID())));
      assertTrue(msgs.contains(new Long(ref19.getMessageID())));
      assertTrue(msgs.contains(new Long(ref20.getMessageID())));
      
      pm.removeNonPersistentMessageReferences(channel.getChannelID(), 1, 4);
      
      refIds = getReferenceIds(channel.getChannelID());
      assertNotNull(refIds);
      assertEquals(18, refIds.size());
      
      assertFalse(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertFalse(refIds.contains(new Long(ref4.getMessageID())));
      assertTrue(refIds.contains(new Long(ref5.getMessageID())));
      assertTrue(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertTrue(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      assertTrue(refIds.contains(new Long(ref11.getMessageID())));
      assertTrue(refIds.contains(new Long(ref12.getMessageID())));
      assertTrue(refIds.contains(new Long(ref13.getMessageID())));
      assertTrue(refIds.contains(new Long(ref14.getMessageID())));
      assertTrue(refIds.contains(new Long(ref15.getMessageID())));
      assertTrue(refIds.contains(new Long(ref16.getMessageID())));
      assertTrue(refIds.contains(new Long(ref17.getMessageID())));
      assertTrue(refIds.contains(new Long(ref18.getMessageID())));
      assertTrue(refIds.contains(new Long(ref19.getMessageID())));
      assertTrue(refIds.contains(new Long(ref20.getMessageID())));
      
      pm.removeNonPersistentMessageReferences(channel.getChannelID(), 4, 14);
      
      refIds = getReferenceIds(channel.getChannelID());
      assertNotNull(refIds);
      assertEquals(11, refIds.size());
      
      assertFalse(refIds.contains(new Long(ref1.getMessageID())));
      assertTrue(refIds.contains(new Long(ref2.getMessageID())));
      assertTrue(refIds.contains(new Long(ref3.getMessageID())));
      assertFalse(refIds.contains(new Long(ref4.getMessageID())));
      assertFalse(refIds.contains(new Long(ref5.getMessageID())));
      assertFalse(refIds.contains(new Long(ref6.getMessageID())));
      assertTrue(refIds.contains(new Long(ref7.getMessageID())));
      assertTrue(refIds.contains(new Long(ref8.getMessageID())));
      assertFalse(refIds.contains(new Long(ref9.getMessageID())));
      assertTrue(refIds.contains(new Long(ref10.getMessageID())));
      assertFalse(refIds.contains(new Long(ref11.getMessageID())));
      assertFalse(refIds.contains(new Long(ref12.getMessageID())));
      assertFalse(refIds.contains(new Long(ref13.getMessageID())));
      assertFalse(refIds.contains(new Long(ref14.getMessageID())));
      assertTrue(refIds.contains(new Long(ref15.getMessageID())));
      assertTrue(refIds.contains(new Long(ref16.getMessageID())));
      assertTrue(refIds.contains(new Long(ref17.getMessageID())));
      assertTrue(refIds.contains(new Long(ref18.getMessageID())));
      assertTrue(refIds.contains(new Long(ref19.getMessageID())));
      assertTrue(refIds.contains(new Long(ref20.getMessageID())));
      
      pm.removeAllChannelData(channel.getChannelID());    
   }
   
   public void testAddRemoveReference() throws Exception
   {
      doSetup(false);
      
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
                  
         ref1_1.incrementChannelCount();
         ref1_2.incrementChannelCount();
         ref2_1.incrementChannelCount();
         ref2_2.incrementChannelCount();
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
         
         List msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         assertTrue(msgs.contains(new Long(m1.getMessageID())));
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
                  
         ref1_1.decrementChannelCount();
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
         
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         assertTrue(msgs.contains(new Long(m1.getMessageID())));
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
         
         ref1_2.decrementChannelCount();
         pm.removeReference(channel2.getChannelID(), ref1_2, null);
         
         refs = getReferenceIds(channel1.getChannelID());
         assertNotNull(refs);
         assertEquals(1, refs.size());
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         refs = getReferenceIds(channel2.getChannelID());
         assertNotNull(refs);
         assertEquals(1, refs.size());         
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
         
         ref2_1.decrementChannelCount();
         pm.removeReference(channel1.getChannelID(), ref2_1, null);
         
         refs = getReferenceIds(channel1.getChannelID());
         assertNotNull(refs);
         assertTrue(refs.isEmpty());
         
         refs = getReferenceIds(channel2.getChannelID());
         assertNotNull(refs);
         assertEquals(1, refs.size());         
         assertTrue(refs.contains(new Long(m2.getMessageID())));
         
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         assertTrue(msgs.contains(new Long(m2.getMessageID())));
         
         ref2_2.decrementChannelCount();
         pm.removeReference(channel2.getChannelID(), ref2_2, null);
         
         refs = getReferenceIds(channel1.getChannelID());
         assertNotNull(refs);
         assertTrue(refs.isEmpty());
         
         refs = getReferenceIds(channel2.getChannelID());
         assertNotNull(refs);
         assertTrue(refs.isEmpty());
         
         msgs = getMessageIds();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
            
      }
   }
       
   public void testRemoveAllChannelData() throws Exception
   {
      doSetup(false);
      
      Channel channel = new SimpleChannel(0, ms);
      
      Message[] messages = createMessages(10);     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
                           
         pm.addReference(channel.getChannelID(), ref, null);
      }
         
      List refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(messages.length, refs.size());
      

      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(messages.length, msgs.size());
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];         
         assertTrue(refs.contains(new Long(m.getMessageID())));         
      }
      
      pm.removeAllChannelData(channel.getChannelID());
            
      refs = getReferenceIds(channel.getChannelID());
      assertTrue(refs.isEmpty());      
      
   }
   
   //non batch
   
   public void testCommit_NotXA_Long_NB() throws Exception
   {
      doTransactionCommit(false, false);
   }
   
   public void testCommit_XA_Long_NB() throws Exception
   {
      doTransactionCommit(true, false);
   }

   public void testRollback_NotXA_Long_NB() throws Exception
   {
      doTransactionRollback(false, false);
   }
       
   public void testRollback_XA_Long_NB() throws Exception
   {
      doTransactionRollback(true, false);
   }
   

   //batch
   
   public void testCommit_NotXA_Long_B() throws Exception
   {
      doTransactionCommit(false, true);
   }
     
   public void testCommit_XA_Long_B() throws Exception
   {
      doTransactionCommit(true, true);
   }

   public void testRollback_NotXA_Long_B() throws Exception
   {
      doTransactionRollback(false, true);
   }
        
   public void testRollback_XA_Long_B() throws Exception
   {
      doTransactionRollback(true, true);
   }
   

   public void testRetrievePreparedTransactions() throws Exception
   {
      doSetup(false);
      
      Channel channel = new SimpleChannel(0, ms);
      
      TransactionRepository txRep = new TransactionRepository();
      txRep.start(pm);

      Message[] messages = createMessages(10);
      
      Xid[] xids = new Xid[messages.length];
      Transaction[] txs = new Transaction[messages.length];
      
      for (int i = 0; i < messages.length; i++)
      {         
         xids[i] = new MockXid();
         txs[i] = txRep.createTransaction(xids[i]);
         MessageReference ref = ms.reference(messages[i]);
         pm.addReference(channel.getChannelID(), ref, txs[i]);
         txs[i].prepare();
      }
      
      List txList = pm.retrievePreparedTransactions();
      assertNotNull(txList);
      assertEquals(messages.length, txList.size());
      
      for (int i = 0; i < xids.length; i++)
      {
         Xid xid = xids[i];
         assertTrue(txList.contains(xid));
      }
      
      //rollback the txs
      for (int i = 0; i < txs.length; i++)
      {
         txs[i].rollback();
      }
      
      
      pm.removeAllChannelData(channel.getChannelID());
      
   }
   
   protected Message createMessage(byte i, boolean reliable) throws Exception
   {
      Map headers = generateFilledMap(true);
      
      Message m = MessageFactory.createCoreMessage(i,
            reliable,
            System.currentTimeMillis() + 1000 * 60 * 60,
            System.currentTimeMillis(),
            (byte)(i % 10),
            headers,            
            i % 2 == 0 ? new WibblishObject() : null);
      
      return m;
   }
   
   protected Message[] createMessages(int num) throws Exception
   {
      //Generate some messages with a good range of attribute values
      Message[] messages = new Message[num];
      for (int i = 0; i < num; i++)
      {            
         messages[i] = createMessage((byte)i, true);
      }
      return messages;
   }
   
   protected void checkEquivalent(Message m1, Message m2) throws Exception
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
      assertEquals(m1.isReference(), m2.isReference());
      assertEquals(m1.isReliable(), m2.isReliable());
      assertEquals(m1.getExpiration(), m2.getExpiration());
      assertEquals(m1.isExpired(), m2.isExpired());
      assertEquals(m1.getTimestamp(), m2.getTimestamp());
      assertEquals(m1.getPriority(), m2.getPriority());
      assertEquals(m1.isRedelivered(), m2.isRedelivered());
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
   
   protected void doTransactionCommit(boolean xa, boolean batch) throws Exception
   {
      doSetup(batch);

      Channel channel = new SimpleChannel(0, ms);
      TransactionRepository txRep = new TransactionRepository();
      txRep.start(pm);

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
      assertTrue(refs.contains(new Long(ref1.getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessageID())));
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));

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
      assertTrue(refs.contains(new Long(ref1.getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessageID())));  
      
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      
      //commit transaction
      tx.commit();
      
      //check we can see only the last 3 refs
      refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(3, refs.size()); 
      assertTrue(refs.contains(new Long(ref3.getMessageID())));
      assertTrue(refs.contains(new Long(ref4.getMessageID())));  
      assertTrue(refs.contains(new Long(ref5.getMessageID())));
      
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(3, msgs.size());
      assertTrue(msgs.contains(new Long(ref3.getMessageID())));
      assertTrue(msgs.contains(new Long(ref4.getMessageID())));
      assertTrue(msgs.contains(new Long(ref5.getMessageID())));
      
      pm.removeAllChannelData(channel.getChannelID());
      
   }
         
   protected void doTransactionRollback(boolean xa, boolean batch) throws Exception
   {
      doSetup(batch);

      Channel channel = new SimpleChannel(0, ms);
      TransactionRepository txRep = new TransactionRepository();
      txRep.start(pm);
 
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
      assertTrue(refs.contains(new Long(ref1.getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessageID())));      
      
      List msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      
      
      
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
      assertTrue(refs.contains(new Long(ref1.getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessageID())));  
      
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      
      //rollback transaction
      tx.rollback();
      
      refs = getReferenceIds(channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(new Long(ref1.getMessageID())));
      assertTrue(refs.contains(new Long(ref2.getMessageID())));  
      
      msgs = getMessageIds();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      assertTrue(msgs.contains(new Long(ref1.getMessageID())));
      assertTrue(msgs.contains(new Long(ref2.getMessageID())));
      
      pm.removeAllChannelData(channel.getChannelID());
            
   }
   
   protected List getReferenceIds(long channelId) throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID FROM JMS_MESSAGE_REFERENCE WHERE CHANNELID=? ORDER BY ORD";
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setLong(1, channelId);
   
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
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
      String sql = "SELECT MESSAGEID FROM JMS_MESSAGE ORDER BY MESSAGEID";
      PreparedStatement ps = conn.prepareStatement(sql);
      
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
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
   
}



