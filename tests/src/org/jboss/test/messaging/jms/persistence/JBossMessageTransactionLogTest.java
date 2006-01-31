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

import java.util.Map;

import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.Message;
import org.jboss.test.messaging.core.plugin.JDBCTransactionLogTest;
import org.jboss.test.messaging.core.plugin.JDBCTransactionLogTest;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossMessageTransactionLogTest extends JDBCTransactionLogTest
{
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public JBossMessageTransactionLogTest(String name)
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

   public void tearDown() throws Exception
   {
      super.tearDown();
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
   
   protected Message createMessage(int i) throws Exception
   {
      Map coreHeaders = generateFilledMap(true);         
      
      Map jmsProperties = generateFilledMap(false);
               
      JBossMessage m = 
         new JBossMessage(new GUID().toString(),
            true,
            System.currentTimeMillis() + 1000 * 60 * 60,
            System.currentTimeMillis(),
            i,
            i + 2,
            coreHeaders,
            new WibblishObject(),
            i % 2 == 0 ? new GUID().toString() : null,
            genCorrelationID(i),
            i % 2 == 0,
            new GUID().toString(),
            i % 2 == 1,
            new GUID().toString(),
            new GUID().toString(),
            jmsProperties);            

      return m;
   }
   
   protected Object genCorrelationID(int i)
   {
      if (i % 3 == 0)
      {
         return null;
      }
      else if (i % 3 == 1)
      {
         return new GUID().toString();
      }
      else if (i % 3 == 2)
      {
         return randByteArray();
      }
      return null;
   }
   
 
}



