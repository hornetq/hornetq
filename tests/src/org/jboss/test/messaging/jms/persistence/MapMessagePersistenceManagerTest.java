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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.jboss.jms.message.JBossMapMessage;
import org.jboss.messaging.core.Message;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * MapMessagePersistenceManagerTest.java,v 1.1 2006/02/22 17:33:44 timfox Exp
 */
public class MapMessagePersistenceManagerTest extends MessagePersistenceManagerTest
{
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public MapMessagePersistenceManagerTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   protected Message createMessage(byte i) throws Exception
   {
      HashMap coreHeaders = generateFilledMap(true);         
      
      HashMap jmsProperties = generateFilledMap(false);
               
      JBossMapMessage m = 
         new JBossMapMessage(new GUID().toString(),
               true,
               System.currentTimeMillis() + 1000 * 60 * 60,
               System.currentTimeMillis(),
               i,
               coreHeaders,
               null,
               i % 2 == 0 ? new GUID().toString() : null,
               genCorrelationID(i),
               i % 3 == 2 ? randByteArray(50) : null,
               i % 2 == 0,
               new GUID().toString(),
               i % 2 == 1,
               new GUID().toString(),            
               jmsProperties);      
      
      Map map = generateFilledMap(true);
      m.setPayload((Serializable)map);
      return m;      
   }
   
}



