/*
 * JBoss, Home of Professional Open Source Copyright 2008, Red Hat Middleware
 * LLC, and individual contributors by the @authors tag. See the copyright.txt
 * in the distribution for a full listing of individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.unit.core.client.management.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;

import java.util.HashMap;
import java.util.Map;

import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ManagementHelperTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testHasOperationSucceeded() throws Exception
   {
      boolean result = randomBoolean();

      Message msg = createMock(Message.class);
      expect(msg.containsProperty(ManagementHelper.HDR_JMX_OPERATION_SUCCEEDED)).andStubReturn(true);
      expect(msg.getProperty(ManagementHelper.HDR_JMX_OPERATION_SUCCEEDED)).andReturn(result);
      replay(msg);

      assertEquals(result, ManagementHelper.hasOperationSucceeded(msg));

      verify(msg);
   }

   public void testHasOperationSucceededWithPropertyAbsent() throws Exception
   {
      Message msg = createMock(Message.class);
      expect(msg.containsProperty(ManagementHelper.HDR_JMX_OPERATION_SUCCEEDED)).andReturn(false);

      replay(msg);

      assertFalse(ManagementHelper.hasOperationSucceeded(msg));

      verify(msg);
   }

   public void testGetOperationExceptionMessage() throws Exception
   {
      SimpleString message = RandomUtil.randomSimpleString();

      Message msg = createMock(Message.class);
      expect(msg.containsProperty(ManagementHelper.HDR_JMX_OPERATION_EXCEPTION)).andReturn(true);
      expect(msg.getProperty(ManagementHelper.HDR_JMX_OPERATION_EXCEPTION)).andReturn(message);

      replay(msg);

      assertEquals(message.toString(), ManagementHelper.getOperationExceptionMessage(msg));

      verify(msg);
   }

   public void testGetOperationExceptionMessageWithNoMessage() throws Exception
   {
      Message msg = createMock(Message.class);
      expect(msg.containsProperty(ManagementHelper.HDR_JMX_OPERATION_EXCEPTION)).andReturn(false);

      replay(msg);

      assertNull(ManagementHelper.getOperationExceptionMessage(msg));

      verify(msg);
   }

   public void testStoreInvalidPropertyType() throws Exception
   {
      SimpleString key = randomSimpleString();
      Map invalidType = new HashMap();
      
      Message msg = new ClientMessageImpl();
      try
      {
         ManagementHelper.storeTypedProperty(msg, key, invalidType);
         fail();
      }
      catch (Exception e)
      {
      }

      assertFalse(msg.containsProperty(key));
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
