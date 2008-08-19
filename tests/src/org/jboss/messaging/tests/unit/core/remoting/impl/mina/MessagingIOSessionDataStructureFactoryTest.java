/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.remoting.impl.mina;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import junit.framework.TestCase;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.session.IoSessionAttributeMap;
import org.jboss.messaging.core.remoting.impl.mina.MessagingIOSessionDataStructureFactory;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MessagingIOSessionDataStructureFactoryTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAttribute() throws Exception
   {
      IoSession session = createStrictMock(IoSession.class);

      replay(session);

      MessagingIOSessionDataStructureFactory factory = new MessagingIOSessionDataStructureFactory();
      IoSessionAttributeMap map = factory.getAttributeMap(null);

      try
      {
         map.getAttribute(session, null, null);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }

      String key = randomString();
      Object defaultValue = randomString();
      Object attribute = map.getAttribute(session, key, defaultValue);
      assertEquals(defaultValue, attribute);

      Object value = randomString();
      map.setAttribute(session, key, value);
      attribute = map.getAttribute(session, key, defaultValue);
      assertEquals(value, attribute);

      verify(session);
   }

   public void testSetAttribute() throws Exception
   {
      IoSession session = createStrictMock(IoSession.class);

      replay(session);

      MessagingIOSessionDataStructureFactory factory = new MessagingIOSessionDataStructureFactory();
      IoSessionAttributeMap map = factory.getAttributeMap(null);

      try
      {
         map.setAttribute(session, null, randomString());
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }

      String key = randomString();
      Object defaultValue = randomString();
      Object value = randomString();
      map.setAttribute(session, key, value);
      Object attribute = map.getAttribute(session, key, defaultValue);
      assertEquals(value, attribute);

      map.setAttribute(session, key, null);
      attribute = map.getAttribute(session, key, defaultValue);
      assertEquals(defaultValue, attribute);

      verify(session);
   }

   public void testSetAttributeIfAbsent() throws Exception
   {
      IoSession session = createStrictMock(IoSession.class);

      replay(session);

      MessagingIOSessionDataStructureFactory factory = new MessagingIOSessionDataStructureFactory();
      IoSessionAttributeMap map = factory.getAttributeMap(null);

      try
      {
         map.setAttributeIfAbsent(session, null, randomString());
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }

      String key = randomString();
      Object defaultValue = randomString();
      Object value = randomString();

      assertNull(map.setAttributeIfAbsent(session, key, null));
      Object attribute = map.setAttributeIfAbsent(session, key, value);
      assertNull(attribute);
      assertEquals(value, map.getAttribute(session, key, defaultValue));

      verify(session);
   }

   public void testRemoveAttribute() throws Exception
   {
      IoSession session = createStrictMock(IoSession.class);

      replay(session);

      MessagingIOSessionDataStructureFactory factory = new MessagingIOSessionDataStructureFactory();
      IoSessionAttributeMap map = factory.getAttributeMap(null);

      try
      {
         map.removeAttribute(session, null);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }

      String key = randomString();
      Object value = randomString();

      assertNull(map.removeAttribute(session, key));

      map.setAttribute(session, key, value);
      assertEquals(value, map.removeAttribute(session, key));
      assertNull(map.removeAttribute(session, key));

      verify(session);
   }

   public void testRemoveAttributeWithValue() throws Exception
   {
      IoSession session = createStrictMock(IoSession.class);

      replay(session);

      MessagingIOSessionDataStructureFactory factory = new MessagingIOSessionDataStructureFactory();
      IoSessionAttributeMap map = factory.getAttributeMap(null);

      try
      {
         map.removeAttribute(session, null, randomString());
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }

      assertFalse(map.removeAttribute(session, randomString(), null));

      String key = randomString();
      Object value = randomString();
      Object otherValue = randomString();

      assertFalse(map.removeAttribute(session, key, value));

      map.setAttribute(session, key, value);
      assertFalse(map.removeAttribute(session, key, otherValue));
      assertTrue(map.removeAttribute(session, key, value));

      verify(session);
   }

   public void testReplaceAttribute() throws Exception
   {
      IoSession session = createStrictMock(IoSession.class);

      replay(session);

      MessagingIOSessionDataStructureFactory factory = new MessagingIOSessionDataStructureFactory();
      IoSessionAttributeMap map = factory.getAttributeMap(null);

      try
      {
         map.replaceAttribute(session, null, randomString(), randomString());
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }

      assertFalse(map.replaceAttribute(session, randomString(), randomString(), randomString()));
   }

   public void testContainsAttribute() throws Exception
   {
      IoSession session = createStrictMock(IoSession.class);

      replay(session);

      MessagingIOSessionDataStructureFactory factory = new MessagingIOSessionDataStructureFactory();
      IoSessionAttributeMap map = factory.getAttributeMap(null);

      try
      {
         map.containsAttribute(session, null);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }

      String key = randomString();
      Object value = randomString();

      assertFalse(map.containsAttribute(session, key));
      map.setAttribute(session, key, value);
      assertTrue(map.containsAttribute(session, key));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
