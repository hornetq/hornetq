package org.jboss.messaging.tests.performance.persistence.fakes;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeQueueFactory;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.SimpleString;



/** Maybe this Fake should be moved to postoffice.fakes, but since this 
 *  Fake only has the basic needed for StorageManagerTest, I have left it here for now */
public class FakePostOffice implements PostOffice
{

   ConcurrentHashMap<SimpleString, Binding> bindings = new ConcurrentHashMap<SimpleString, Binding>();
   
   QueueFactory queueFactory = new FakeQueueFactory();
   
   ConcurrentHashSet<SimpleString> addresses = new ConcurrentHashSet<SimpleString>();
   
   public Binding addBinding(SimpleString address, SimpleString queueName,
         Filter filter, boolean durable, boolean temporary) throws Exception
   {
      Queue queue = queueFactory.createQueue(-1, queueName, filter, durable, temporary); 
      Binding binding = new FakeBinding(address, queue);
      bindings.put(address, binding);
      return binding;
   }

   public boolean addDestination(SimpleString address, boolean temporary)
         throws Exception
   {
      return addresses.addIfAbsent(address);
   }

   public boolean containsDestination(SimpleString address)
   {
      return addresses.contains(address);
   }

   public Binding getBinding(SimpleString queueName) throws Exception
   {
      return bindings.get(queueName);
   }

   public List<Binding> getBindingsForAddress(SimpleString address)
         throws Exception
   {
      return null;
   }

   public FlowController getFlowController(SimpleString address)
   {
      return null;
   }

   public Map<SimpleString, List<Binding>> getMappings()
   {
      return null;
   }

   public Set<SimpleString> listAllDestinations()
   {
      return null;
   }

   public Binding removeBinding(SimpleString queueName) throws Exception
   {
      return null;
   }

   public boolean removeDestination(SimpleString address, boolean temporary)
         throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   public void start() throws Exception
   {
      // TODO Auto-generated method stub
      
   }

   public void stop() throws Exception
   {
      // TODO Auto-generated method stub
      
   }

   public List<org.jboss.messaging.core.server.MessageReference> route(
         ServerMessage message) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }
   
}
