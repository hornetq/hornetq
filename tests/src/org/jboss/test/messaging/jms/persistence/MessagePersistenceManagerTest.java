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
package org.jboss.test.messaging.jms.persistence;

import java.util.HashMap;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.jms.server.plugin.JDBCChannelMapper;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.test.messaging.core.plugin.JDBCPersistenceManagerTest;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * MessagePersistenceManagerTest.java,v 1.1 2006/02/22 17:33:44 timfox Exp
 */
public class MessagePersistenceManagerTest extends JDBCPersistenceManagerTest
{
   // Attributes ----------------------------------------------------
   
   protected ChannelMapper cm;
      
   // Constructors --------------------------------------------------

   public MessagePersistenceManagerTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("This test must not be ran in remote mode!");
      }
      
      super.setUp();              
   }
   
   protected void doSetup(boolean batch) throws Exception
   {
      super.doSetup(batch);
      
      cm.deployCoreDestination(true, "testDestination", ms, pm, null, 100, 20, 10);
      cm.deployCoreDestination(true, "testReplyTo", ms, pm, null, 100, 20, 10);
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      
      cm.undeployCoreDestination(true, "testDestination");
      cm.undeployCoreDestination(true, "testReplyTo");
      
      cm.stop();
   }
   
   protected JDBCPersistenceManager createPM() throws Exception
   {
      try
      {
         cm = new JDBCChannelMapper(sc.getDataSource(), sc.getTransactionManager());
                  
         JDBCPersistenceManager pm = new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), cm);
         
         pm.start();
         
         cm.setPersistenceManager(pm);
         
         cm.setQueuedExecutorPool(new QueuedExecutorPool(100));
         
         cm.start();
                      
         return pm;
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return null;
      }
   }
  
   protected void checkEquivalent(Message m1, Message m2) throws Exception
   {
      super.checkEquivalent(m1, m2);
     
      if (!(m1 instanceof JBossMessage) && !(m2 instanceof JBossMessage))
      {
         fail();
      }
      
      JBossMessage jm1 = (JBossMessage)m1;
      JBossMessage jm2 = (JBossMessage)m2;
      
      assertEquals(jm1.isCorrelationIDBytes(), jm2.isCorrelationIDBytes());
      if (jm1.isCorrelationIDBytes())
      {
         checkByteArraysEqual(jm1.getJMSCorrelationIDAsBytes(), jm2.getJMSCorrelationIDAsBytes());
      }
      else
      {
         assertEquals(jm1.getJMSCorrelationID(), jm2.getJMSCorrelationID());
      }
            
      assertEquals(jm1.getJMSMessageID(), jm2.getJMSMessageID());
      assertEquals(jm1.getJMSRedelivered(), jm2.getJMSRedelivered());
      assertEquals(jm1.getJMSType(), jm2.getJMSType());
      assertEquals(jm1.getJMSDeliveryMode(), jm2.getJMSDeliveryMode());
      assertEquals(jm1.getJMSDestination(), jm2.getJMSDestination());
      assertEquals(jm1.getJMSExpiration(), jm2.getJMSExpiration());
      assertEquals(jm1.getJMSPriority(), jm2.getJMSPriority());
      assertEquals(jm1.getJMSReplyTo(), jm2.getJMSReplyTo());
      assertEquals(jm1.getJMSTimestamp(), jm2.getJMSTimestamp());

      checkMapsEquivalent(jm1.getJMSProperties(), jm2.getJMSProperties());
      

   }
   
   protected Message createMessage(byte i, boolean reliable) throws Exception
   {
      HashMap coreHeaders = generateFilledMap(true);         
      
      HashMap jmsProperties = generateFilledMap(false);
              
      JBossMessage jbm = 
         new JBossMessage(i,
            reliable,
            System.currentTimeMillis() + 1000 * 60 * 60,
            System.currentTimeMillis(),
            i,
            coreHeaders,
            null,
            0,
            i % 2 == 0 ? new GUID().toString() : null,
            genCorrelationID(i),
            i % 3 == 2 ? randByteArray(50) : null,
            new JBossQueue("testDestination"),
            new JBossQueue("testReplyTo"),            
            jmsProperties);    
      
      jbm.setPayload(new WibblishObject());
      
      return jbm;
   }
   
   protected String genCorrelationID(int i)
   {
      if (i % 3 == 0)
      {
         return null;
      }
      else if (i % 3 == 1)
      {
         return new GUID().toString();
      }     
      return null;
   }
   
 
}



