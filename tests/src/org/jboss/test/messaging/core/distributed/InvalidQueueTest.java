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
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.MessagingTestCase;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class InvalidQueueTest extends MessagingTestCase
{

   // Constructors --------------------------------------------------

   public InvalidQueueTest(String name)
   {
      super(name);
   }

//   // Protected -----------------------------------------------------
//
//   // Public --------------------------------------------------------
//
//   public void testNoRpcServer() throws Exception
//   {
//      JChannel channel = new JChannel();
//      RpcDispatcher dispatcher = new RpcDispatcher(channel, null, null, null);
//
//      try
//      {
//         new Queue(dispatcher, "doesntmatter");
//         fail("Should have thrown IllegalStateException");
//      }
//      catch(IllegalStateException e)
//      {
//         // OK
//      }
//   }
//
//   public void testConnectWithTheJChannelNotConnected() throws Exception
//   {
//      JChannel jChannel = new JChannel();
//      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, new RpcServer());
//      assertFalse(jChannel.isConnected());
//      Queue peerOne = new Queue(dispatcher, "doesntmatter");
//
//      try
//      {
//         peerOne.start();
//         fail("Should have thrown DistributedException");
//      }
//      catch(DistributedException e)
//      {
//         // Ok
//      }
//   }

   public void testNoop()
   {
   }
   

}
