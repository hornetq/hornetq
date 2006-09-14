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

import java.util.Set;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.RoutableSupport;
import org.jboss.messaging.core.plugin.SimpleMessageReference;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.test.messaging.core.message.base.RoutableSupportTestBase;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleMessageReferenceTest extends RoutableSupportTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SimpleMessageReferenceTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testHeader()
   {
      SimpleMessageReference ref = (SimpleMessageReference)rs;
      Set headerNames = ref.getHeaderNames();
      assertTrue(headerNames.contains("headerName01"));
      assertEquals("headerValue01", ref.getHeader("headerName01"));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      Message m = CoreMessageFactory.createCoreMessage(0);
      m.putHeader("headerName01", "headerValue01");

      MessageStore ms = new SimpleMessageStore();

      rs = (RoutableSupport)ms.reference(m);

      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      rs = null;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
