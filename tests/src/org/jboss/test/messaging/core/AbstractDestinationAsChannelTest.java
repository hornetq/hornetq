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
package org.jboss.test.messaging.core;



/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AbstractDestinationAsChannelTest extends TransactionalChannelSupportTest
{
//   // Attributes ----------------------------------------------------
//
//   protected AbstractDestination abstractDestination;
//
//   // Constructors --------------------------------------------------
//
   public AbstractDestinationAsChannelTest(String name)
   {
      super(name);
   }

//   public void setUp() throws Exception
//   {
//      abstractDestination = (AbstractDestination)channel;
//
//      super.setUp();
//   }
//
//   public void tearDown() throws Exception
//   {
//      abstractDestination = null;
//      super.tearDown();
//   }
//
//   public void testDeliveryAttemptTriggeredByAddingReceiver()
//   {
//      if (abstractDestination == null) { return; }
//
//      assertTrue(abstractDestination.setSynchronous(false));
//
//      assertTrue(abstractDestination.handle(new RoutableSupport("routableID1", false)));
//      assertTrue(abstractDestination.handle(new RoutableSupport("routableID2", false)));
//      assertTrue(abstractDestination.handle(new RoutableSupport("routableID3", false)));
//
//      assertEquals(3, abstractDestination.getUndelivered().size());
//
//      assertFalse(abstractDestination.deliver());
//      assertEquals(3, abstractDestination.getUndelivered().size());
//
//      // this should trigger asynchronous delivery attempt
//      abstractDestination.add(receiverOne);
//
//      assertFalse(abstractDestination.hasMessages());
//
//      assertEquals(3, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID1"));
//      assertTrue(receiverOne.contains("routableID2"));
//      assertTrue(receiverOne.contains("routableID3"));
//   }

}