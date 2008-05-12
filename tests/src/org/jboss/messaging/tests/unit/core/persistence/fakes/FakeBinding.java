package org.jboss.messaging.tests.unit.core.persistence.fakes;

import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.util.SimpleString;

public class FakeBinding implements Binding
{
   SimpleString address;
   Queue queue;
   
   public FakeBinding(SimpleString address, Queue queue)
   {
      this.address = address;
      this.queue = queue;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public Queue getQueue()
   {
      return queue;
   }
   
}
