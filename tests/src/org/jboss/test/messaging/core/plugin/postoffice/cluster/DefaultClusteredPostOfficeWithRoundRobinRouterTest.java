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
package org.jboss.test.messaging.core.plugin.postoffice.cluster;

import java.util.List;

import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.test.messaging.core.SimpleCondition;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.base.PostOfficeTestBase;

/**
 * 
 * A DefaultClusteredPostOfficeWithRoundRobinRouterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultClusteredPostOfficeWithRoundRobinRouterTest extends PostOfficeTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
    
   // Constructors ---------------------------------------------------------------------------------

   public DefaultClusteredPostOfficeWithRoundRobinRouterTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {      
      super.tearDown();
   }
   
   public void testNotLocalPersistent() throws Throwable
   {
      notLocal(true);
   }
   
   public void testNotLocalNonPersistent() throws Throwable
   {
      notLocal(false);
   }
   
   
   protected void notLocal(boolean persistent) throws Throwable
   {
      log.info("not local:" + persistent);
      
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      ClusteredPostOffice office4 = null;
      
      ClusteredPostOffice office5 = null;
      
      ClusteredPostOffice office6 = null;
          
      try
      {   
         office1 = createClusteredPostOfficeWithRRR(1, "testgroup", sc, ms, pm, tr);
         
         office2 = createClusteredPostOfficeWithRRR(2, "testgroup", sc, ms, pm, tr);
         
         office3 = createClusteredPostOfficeWithRRR(3, "testgroup", sc, ms, pm, tr);
         
         office4 = createClusteredPostOfficeWithRRR(4, "testgroup", sc, ms, pm, tr);
         
         office5 = createClusteredPostOfficeWithRRR(5, "testgroup", sc, ms, pm, tr);
         
         office6 = createClusteredPostOfficeWithRRR(6, "testgroup", sc, ms, pm, tr);
         
         LocalClusteredQueue queue1 =
            new LocalClusteredQueue(office1, 1, "queue1", channelIDManager.getID(), ms, pm,
                                    true, false, -1, null, tr);
         office1.bindClusteredQueue(new SimpleCondition("topic"), queue1);
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         
         LocalClusteredQueue queue2 =
            new LocalClusteredQueue(office2, 2, "queue1", channelIDManager.getID(), ms, pm,
                                    true, false, -1, null, tr);
         office2.bindClusteredQueue(new SimpleCondition("topic"), queue2);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         
         LocalClusteredQueue queue3 =
            new LocalClusteredQueue(office3, 3, "queue1", channelIDManager.getID(), ms, pm,
                                    true, false, -1, null, tr);
         office3.bindClusteredQueue(new SimpleCondition("topic"), queue3);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         
         LocalClusteredQueue queue4 =
            new LocalClusteredQueue(office4, 4, "queue1", channelIDManager.getID(), ms, pm,
                                    true, false, -1, null, tr);
         office4.bindClusteredQueue(new SimpleCondition("topic"), queue4);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue4.add(receiver4);
         
         LocalClusteredQueue queue5 =
            new LocalClusteredQueue(office5, 5, "queue1", channelIDManager.getID(), ms, pm,
                                    true, false, -1, null, tr);
         office5.bindClusteredQueue(new SimpleCondition("topic"), queue5);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue5.add(receiver5);
         
         LocalClusteredQueue queue6 =
            new LocalClusteredQueue(office6, 6, "queue1", channelIDManager.getID(), ms, pm,
                                    true, false, -1, null, tr);
         office6.bindClusteredQueue(new SimpleCondition("topic"), queue6);
         SimpleReceiver receiver6 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue6.add(receiver6);
         
         log.info("setup");
               
         List msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         checkEmpty(receiver6);
         
         log.info("1");
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         checkEmpty(receiver6);
         
         log.info("2");
         
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkContainsAndAcknowledge(msgs, receiver3, queue1);                           
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         checkEmpty(receiver6);
         
         log.info("3");
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkContainsAndAcknowledge(msgs, receiver4, queue1);                                    
         checkEmpty(receiver5);
         checkEmpty(receiver6);
         
         
         
         log.info("4");
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkContainsAndAcknowledge(msgs, receiver5, queue1); 
         checkEmpty(receiver6);
         
         
         log.info("5");
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);       
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         checkContainsAndAcknowledge(msgs, receiver6, queue1); 
         
         log.info("6");
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);      
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         checkEmpty(receiver6);
         log.info("7");
                     
      }
      finally
      {
         if (office1 != null)
         {            
            try
            {
               office1.stop();
            }
            catch (Exception ignore)
            {
               
            }
         }
         
         if (office2 != null)
         {
            try
            {
               office2.stop();
            }
            catch (Exception ignore)
            {
               
            }
         }
         
         if (office3 != null)
         {            
            try
            {
               office3.stop();
            }
            catch (Exception ignore)
            {
               
            }
         }
         
         if (office4 != null)
         {
            try
            {
               office4.stop();
            }
            catch (Exception ignore)
            {
               
            }
         }
         
         if (office5 != null)
         {            
            try
            {
               office5.stop();
            }
            catch (Exception ignore)
            {
               
            }
         }
         
         if (office6 != null)
         {
            try
            {
               office6.stop();
            }
            catch (Exception ignore)
            {
               
            }
         }
      }
   }
   
   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}



