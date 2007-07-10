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
package org.jboss.test.messaging.core.message;

import org.jboss.messaging.core.impl.message.MessageSupport;
import org.jboss.test.messaging.MessagingTestCase;


/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2202 $</tt>
 *
 * $Id: RoutableSupportTestBase.java 2202 2007-02-08 10:50:26Z timfox $
 */
public class MessageSupportTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MessageSupport ms;

   // Constructors --------------------------------------------------

   public MessageSupportTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testNullAsHeaderValue() throws Exception
   {
      ms.putHeader("someHeader", null);
      assertTrue(ms.containsHeader("someHeader"));
      assertNull(ms.getHeader("someHeader"));
      assertNull(ms.removeHeader("someHeader"));
      assertFalse(ms.containsHeader("someHeader"));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
