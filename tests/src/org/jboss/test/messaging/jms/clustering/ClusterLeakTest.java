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
import javax.jms.MessageProducer;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossMessageProducer;
import org.jboss.jms.client.FailoverValve2;
import org.jboss.jms.client.container.ClientConsumer;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.profiler.jvmti.InventoryDataPoint;
import org.jboss.profiler.jvmti.JVMTIInterface;
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

   public void testValidateOpening() throws Exception
   {
      JVMTIInterface jvmti = new JVMTIInterface();


      Map inventory1 = null;
      Connection conn = null;
      for (int i=0;i<100;i++)
      {
         log.info("Creating connection " + i);
         conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue[0]);
         MessageConsumer consumer = session.createConsumer(queue[0]);
         conn.start();
         Thread.sleep(100);
         conn.close();

         if (i==10)
         {
            inventory1 = jvmti.produceInventory();
         }
      }

      conn = null;


      jvmti.forceReleaseOnSoftReferences();
      jvmti.forceGC();

      Map inventory2 = jvmti.produceInventory();

      validateInstances(jvmti, FailoverValve2.class, inventory2, 1);
      validateInstances(jvmti, ClientConsumer.class, inventory2, 1);

      assertTrue("Test produced unexpected objects", jvmti.compareInventories(System.out,
          inventory1, inventory2, null, null,
          new InventoryDataPoint[]{new InventoryDataPoint(java.lang.ref.SoftReference.class, 10),
                                   new InventoryDataPoint(jvmti.getClassByName("java.io.ObjectStreamClass$WeakClassKey"), 100),
             new InventoryDataPoint(jvmti.getClassByName("java.util.concurrent.ConcurrentHashMap$HashEntry"), 100),
             new InventoryDataPoint(jvmti.getClassByName("java.util.WeakHashMap$Entry"), 500)

                                   }));

   }

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

         validateInstances(jvmti, ClientConnectionDelegate.class, inventory2, 1);
         validateInstances(jvmti, ConnectionState.class, inventory2, 1);
         validateInstances(jvmti, MessageConsumer.class, inventory2, 1);
         validateInstances(jvmti, ProducerState.class, inventory2, 1);
         validateInstances(jvmti, FailoverValve2.class, inventory2, 1);
         validateInstances(jvmti, ClientConsumer.class, inventory2, 1);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /** Look on the HEAP information if a given class produced more objects than expected.
    *  If it happened, a report will be generated on the System.out */
   protected void validateInstances(JVMTIInterface jvmti, Class clazz, Map inventory, int maxExpectedResult)
   {
      InventoryDataPoint dataPoint = (InventoryDataPoint) inventory.get(clazz);
      if (dataPoint != null && dataPoint.getInstances() > 1)
      {
         log.info(clazz.getName() + " report -> " + jvmti.exploreObjectReferences(clazz.getName(),5,true));
         fail("Produced unexpected objects (" + dataPoint.getInstances() + " objects, while it was expecting " + maxExpectedResult + ") on " + clazz.getName());
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
