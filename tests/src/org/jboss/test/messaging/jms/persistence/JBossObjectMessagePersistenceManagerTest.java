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

import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.messaging.core.Message;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 */
public class JBossObjectMessagePersistenceManagerTest extends JBossMessagePersistenceManagerTest
{
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public JBossObjectMessagePersistenceManagerTest(String name)
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
  
   
   protected Message createMessage(int i) throws Exception
   {
      Map coreHeaders = generateFilledMap(true);         
      
      Map jmsProperties = generateFilledMap(false);
               
      JBossObjectMessage m = 
         new JBossObjectMessage(new GUID().toString(),
            true,
            System.currentTimeMillis() + 1000 * 60 * 60,
            System.currentTimeMillis(),
            i,
            i + 2,
            coreHeaders,
            null,
            i % 2 == 0 ? new GUID().toString() : null,
            genCorrelationID(i),
            i % 2 == 0,
            new GUID().toString(),
            i % 2 == 1,
            new GUID().toString(),
            new GUID().toString(),
            jmsProperties);     
      m.setPayload(new WibblishObject());
      return m;      
   }
   
}



