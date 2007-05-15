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

package org.jboss.test.messaging.jms.clustering;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossMessageProducer;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.profiler.jvmti.InventoryDataPoint;
import org.jboss.profiler.jvmti.JVMTIInterface;
import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * This test is executed with target "leak-tests" on the testsuite/build.xml
 * It requires jbossAgent.dll or libJBossAgent.so on the LD_LIBRARY_PATH, so you need to set it
 * before calling the target
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class ClusterLeakTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClusterLeakTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testValidateFailover() throws Exception
   {
      Connection conn = null;

      JVMTIInterface jvmti = new JVMTIInterface();

      try
      {
         conn = cf.createConnection();
         conn.close();
         conn = cf.createConnection();
         conn.start();

         // make sure we're connecting to node 1

         int nodeID = ((ConnectionState)((DelegateSupport)((JBossConnection)conn).
            getDelegate()).getState()).getServerID();

         assertEquals(1, nodeID);

         Session s1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c1 = s1.createConsumer(queue[1]);
         JBossMessageProducer p1 = (JBossMessageProducer)s1.createProducer(queue[1]);
         p1.setDeliveryMode(DeliveryMode.PERSISTENT);

         // send a message

         p1.send(s1.createTextMessage("blip"));
         assertNotNull(c1.receive());
         p1.send(s1.createTextMessage("blip again"));

         log.info("Producing inventory");

         Map inventory1 = jvmti.produceInventory();

         // kill node 1

         ServerManagement.killAndWait(1);
         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         try
         {
            ic[1].lookup("queue"); // looking up anything
            fail("The server still alive, kill didn't work yet");
         }
         catch (Exception e)
         {
         }

         // we must receive the message

         TextMessage tm = (TextMessage)c1.receive(1000);
         assertEquals("blip again", tm.getText());

         log.info("Forcing release on SoftReferences");
         jvmti.forceReleaseOnSoftReferences();
         jvmti.forceGC();

         Map inventory2 = jvmti.produceInventory();

         InventoryDataPoint dataPoint = (InventoryDataPoint) inventory2.get(ProducerState.class);
         if (dataPoint.getInstances() > 1)
         {
            // We should only have this producerState... we will look for where are the
            // other references
            ProducerState originalState = (ProducerState )
                ((DelegateSupport)p1.getDelegate()).getState();
            Object obj[] = jvmti.getAllObjects(ProducerState.class);

            for (int i = 0; i < obj.length; i++)
            {
               if (obj[i] != originalState)
               {
                  log.info("Exploring references on " + obj[i]);
                  Object[] holders = jvmti.getReferenceHolders(new Object[]{obj[i]});

                  for (int j = 0; j < holders.length; j++)
                  {
                     log.info("Holder[" + j + "] = " + holders[j]);
                  }

               }
            }

         }

         assertTrue("Test produced unexpected objects", jvmti.compareInventories(System.out,
            inventory1, inventory2, null, null,
            new InventoryDataPoint[]{new InventoryDataPoint(Object.class, 10)}));

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }


   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 2;

      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
