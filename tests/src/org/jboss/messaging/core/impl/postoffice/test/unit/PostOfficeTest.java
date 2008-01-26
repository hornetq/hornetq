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
package org.jboss.messaging.core.impl.postoffice.test.unit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.easymock.EasyMock;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.QueueFactory;
import org.jboss.messaging.core.Transaction;
import org.jboss.messaging.core.impl.BindingImpl;
import org.jboss.messaging.core.impl.ConditionImpl;
import org.jboss.messaging.core.impl.QueueFactoryImpl;
import org.jboss.messaging.core.impl.QueueImpl;
import org.jboss.messaging.core.impl.TransactionImpl;
import org.jboss.messaging.core.impl.filter.FilterImpl;
import org.jboss.messaging.core.impl.postoffice.PostOfficeImpl;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A PostOfficeTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class PostOfficeTest extends UnitTestCase
{
   //TODO  test all methods!!!
   
   @Override
   protected void setUp() throws Exception
   {
   }

   @Override
   protected void tearDown() throws Exception
   {
   }
   
   public void testAddQueue() throws Exception
   {
      PersistenceManager pm = EasyMock.createStrictMock(PersistenceManager.class);
      
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      
      final int nodeID = 21;
      
      PostOffice po = new PostOfficeImpl(nodeID, pm, qf);
      
      final long id = 324;
      final String name = "wibb22";
      final Filter filter = new FilterImpl("eek");
      final boolean clustered = true;
      final boolean durable = true;
      final boolean temporary = true;
      
      Queue queue = new QueueImpl(id, name, filter, clustered, durable, temporary, -1);
      
      EasyMock.expect(qf.createQueue(-1, name, filter, durable, temporary)).andReturn(queue);
            
      final Condition condition = new ConditionImpl(DestinationType.QUEUE, "wibble");
      final boolean allNodes = false;
      
      Binding expected = new BindingImpl(nodeID, condition, queue, allNodes);
      
      pm.addBinding(EasyMock.eq(expected));
      
      EasyMock.replay(qf);
      
      EasyMock.replay(pm);
      
      po.addQueue(condition, name, filter, durable, temporary, allNodes);
      
      EasyMock.verify(qf);
      
      EasyMock.verify(pm);
      
      EasyMock.reset(qf);
      
      EasyMock.reset(pm);
      
      final boolean durable2 = false;
      
      queue = new QueueImpl(id, name, filter, clustered, durable2, temporary, -1);
      
      EasyMock.expect(qf.createQueue(-1, name, filter, durable2, temporary)).andReturn(queue);
      
      EasyMock.replay(qf);
      
      EasyMock.replay(pm);
      
      po.addQueue(condition, name, filter, durable2, temporary, allNodes);
      
      EasyMock.verify(qf);
      
      EasyMock.verify(pm);
   }   
   
   public void testRemoveQueue() throws Exception
   {
      PersistenceManager pm = EasyMock.createStrictMock(PersistenceManager.class);
      
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      
      final int nodeID = 21;
      
      PostOffice po = new PostOfficeImpl(nodeID, pm, qf);
      
      final long id = 324;
      final String name = "wibb22";
      final Filter filter = new FilterImpl("eek");
      final boolean clustered = true;
      final boolean durable = true;
      final boolean temporary = true;
      
      Queue queue = new QueueImpl(id, name, filter, clustered, durable, temporary, -1);
      
      EasyMock.expect(qf.createQueue(-1, name, filter, durable, temporary)).andReturn(queue);
            
      final Condition condition = new ConditionImpl(DestinationType.QUEUE, "wibble");
      final boolean allNodes = false;
      
      Binding expected = new BindingImpl(nodeID, condition, queue, allNodes);
      
      pm.addBinding(EasyMock.eq(expected));
      
      pm.deleteBinding(EasyMock.eq(expected));
      
      EasyMock.replay(qf);
      
      EasyMock.replay(pm);
      
      po.addQueue(condition, name, filter, durable, temporary, allNodes);
      
      po.removeQueue(condition, name, allNodes);
      
      EasyMock.verify(qf);
      
      EasyMock.verify(pm);
      
      EasyMock.reset(qf);
      
      EasyMock.reset(pm);
      
      final boolean durable2 = false;
      
      queue = new QueueImpl(id, name, filter, clustered, durable2, temporary, -1);
      
      EasyMock.expect(qf.createQueue(-1, name, filter, durable2, temporary)).andReturn(queue);
      
      EasyMock.replay(qf);
      
      EasyMock.replay(pm);
      
      po.addQueue(condition, name, filter, durable2, temporary, allNodes);
      
      po.removeQueue(condition, name, allNodes);
      
      EasyMock.verify(qf);
      
      EasyMock.verify(pm);
   }   
   
   public void testAddRemoveMultipleWithDifferentConditions() throws Exception
   {
      PersistenceManager pm = EasyMock.createStrictMock(PersistenceManager.class);
      
      QueueFactory qf = new QueueFactoryImpl();
      
      final int nodeID = 21;
      
      PostOffice po = new PostOfficeImpl(nodeID, pm, qf);
      
      final Condition condition1 = new ConditionImpl(DestinationType.QUEUE, "wibble");      
                
      po.addQueue(condition1, "queue1", null, false, false, false);      
      Map<Condition, List<Binding>> mappings = po.getMappings();      
      assertEquals(1, mappings.size());
      
      po.addQueue(condition1, "queue2", null, false, false, false);     
      mappings = po.getMappings();      
      assertEquals(1, mappings.size());
      
      po.addQueue(condition1, "queue3", null, false, false, false); 
      mappings = po.getMappings();      
      assertEquals(1, mappings.size());
      
      List<Binding> bindings = mappings.get(condition1);
      assertNotNull(bindings);
      assertEquals(3, bindings.size());
      
      Binding binding1 = bindings.get(0);
      Queue queue1 = binding1.getQueue();
      assertEquals("queue1", queue1.getName());
            
      Binding binding2 = bindings.get(1);
      Queue queue2 = binding2.getQueue();
      assertEquals("queue2", queue2.getName());
      
      Binding binding3 = bindings.get(2);
      Queue queue3 = binding3.getQueue();
      assertEquals("queue3", queue3.getName());
      
      final Condition condition2 = new ConditionImpl(DestinationType.QUEUE, "wibble2"); 
      
      po.addQueue(condition2, "queue4", null, false, false, false);       
      mappings = po.getMappings();      
      assertEquals(2, mappings.size());
      
      po.addQueue(condition2, "queue5", null, false, false, false); 
      mappings = po.getMappings();      
      assertEquals(2, mappings.size());
      
      final Condition condition3 = new ConditionImpl(DestinationType.TOPIC, "wibblexyz"); 
      
      po.addQueue(condition3, "queue6", null, false, false, false);       
      mappings = po.getMappings();      
      assertEquals(3, mappings.size());
      
      po.removeQueue(condition3, "queue6", false);
      mappings = po.getMappings();      
      assertEquals(2, mappings.size());
      
      po.removeQueue(condition2, "queue4", false);
      mappings = po.getMappings();      
      assertEquals(2, mappings.size());
      
      po.removeQueue(condition2, "queue5", false);
      mappings = po.getMappings();      
      assertEquals(1, mappings.size());
      
      po.removeQueue(condition1, "queue1", false);
      mappings = po.getMappings();      
      assertEquals(1, mappings.size());
      
      po.removeQueue(condition1, "queue2", false);
      mappings = po.getMappings();      
      assertEquals(1, mappings.size());
      
      po.removeQueue(condition1, "queue3", false);
      mappings = po.getMappings();      
      assertEquals(0, mappings.size());      
   }
   
   public void testRouteNoTransaction() throws Exception
   {
      PersistenceManager pm = EasyMock.createStrictMock(PersistenceManager.class);
      
      QueueFactory qf = new QueueFactoryImpl();
      
      PostOffice po = new PostOfficeImpl(1, pm, qf);
      
      Message message = this.generateMessage(1);
      
      try
      {
         po.route(new ConditionImpl(DestinationType.QUEUE, "eek"), message);
         fail("Should throw exception");
      }
      catch (NullPointerException e)
      {
         //OK
      }
   }
   
   public void testRouteNPMessage_NDQueue() throws Exception
   {
      PersistenceManager pm = EasyMock.createStrictMock(PersistenceManager.class);
      
      QueueFactory qf = new QueueFactoryImpl();
      
      PostOffice po = new PostOfficeImpl(1, pm, qf);
      
      Condition condition = new ConditionImpl(DestinationType.QUEUE, "queue1");
      
      po.addQueue(condition, "queue1", null, false, false, false);
      
      Message message = this.generateMessage(1);
      
      message.setDurable(false);
      
      List<Message> msgs = new ArrayList<Message>();
      
      msgs.add(message);
      
      po.route(new ConditionImpl(DestinationType.QUEUE, "queue1"), message);
      
      if (message.getNumDurableReferences() != 0)
      {
         //Need to route in a transaction
         Transaction tx = new TransactionImpl();
      }
      
      
      
      Map<Condition, List<Binding>> mappings = po.getMappings();
      
      Binding binding = mappings.get(condition).get(0);
      
      Queue queue = binding.getQueue();
      
      assertEquals(1, queue.getMessageCount());
      
      assertTrue(message == queue.list(null).get(0).getMessage());

   }      
}
