/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.cluster.restart;

import org.hornetq.utils.SimpleString;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.message.impl.MessageImpl;

import java.util.Collection;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Sep 29, 2009
 */
public class ClusterRestartTest extends ClusterTestBase
{
   public void testRestartWithDurableQueues() throws Exception
   {
      /*setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());



         createQueue(0, "queues.testaddress", "queue0", null, true);
         createQueue(1, "queues.testaddress", "queue0", null, true);
         createQueue(2, "queues.testaddress", "queue0", null, true);

         addConsumer(1, 1, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 0, true);

         waitForBindings(0, "queues.testaddress", 2, 1, false);
         waitForBindings(1, "queues.testaddress", 2, 0, false);
         waitForBindings(2, "queues.testaddress", 2, 1, false);

         printBindings();

         sendInRange(1, "queues.testaddress", 0, 10, false, null);


         sendInRange(2, "queues.testaddress", 10, 20, false, null);


         sendInRange(0, "queues.testaddress", 20, 30, false, null);

         System.out.println("stopping******************************************************");
         stopServers(1);
         System.out.println("stopped******************************************************");
         startServers(1);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         waitForBindings(1, "queues.testaddress", 1, 0, true);
         waitForBindings(2, "queues.testaddress", 1, 0, true);

         addConsumer(4, 1, "queue0", null);
         waitForBindings(0, "queues.testaddress", 2, 1, false);
         waitForBindings(1, "queues.testaddress", 2, 0, false);
         waitForBindings(2, "queues.testaddress", 2, 1, false);
         printBindings();
         sendInRange(2, "queues.testaddress", 30, 40, false, null);

         sendInRange(0, "queues.testaddress", 40, 50, false, null);

         verifyReceiveAllInRange(0, 50, 1);
         System.out.println("*****************************************************************************");
      }
      finally
      {
         //closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }*/
   }

   private void printBindings()
         throws Exception
   {
      Collection<Binding> bindings0 = getServer(0).getPostOffice().getBindingsForAddress(new SimpleString("queues.testaddress")).getBindings();
      Collection<Binding> bindings1 = getServer(1).getPostOffice().getBindingsForAddress(new SimpleString("queues.testaddress")).getBindings();
      Collection<Binding> bindings2 = getServer(2).getPostOffice().getBindingsForAddress(new SimpleString("queues.testaddress")).getBindings();
      for (Binding binding : bindings0)
      {
         System.out.println(binding + " on node 0 at " + binding.getID());
      }

      for (Binding binding : bindings1)
      {
         System.out.println(binding + " on node 1 at " + binding.getID());
      }

      for (Binding binding : bindings2)
      {
         System.out.println(binding + " on node 2 at " + binding.getID());
      }
   }

   public boolean isNetty()
   {
      return true;
   }

   public boolean isFileStorage()
   {
      return true;
   }
}
