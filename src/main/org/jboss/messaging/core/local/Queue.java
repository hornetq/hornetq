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
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.plugin.contract.TransactionLog;
import org.jboss.messaging.core.ChannelSupport;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.jms.server.plugin.contract.MessageStore;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Queue extends ChannelSupport implements CoreDestination
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public Queue(String name, MessageStore ms)
   {
      this(name, ms, null);
   }

   public Queue(String name, MessageStore ms, TransactionLog tl)
   {
      super(name, ms, tl, false);
      router = new PointToPointRouter();
   }

   // CoreDestination implementation --------------------------------
   
   public String getName()
   {
      return (String)channelID;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreQueue[" + getChannelID() + "]";
   }
   
   public void load() throws Exception
   {
      state.load();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
