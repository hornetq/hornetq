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

package org.jboss.messaging.core.channelfactory.impl;

import org.jboss.messaging.core.channelfactory.ChannelFactory;
import org.jgroups.Channel;
import org.jgroups.JChannelFactory;

/**
 * A ChannelFactory that will use the MBean ChannelFactory interface
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 3465 $</tt>
 * $Id: MultiplexerChannelFactory.java 3465 2007-12-10 17:32:22Z ataylor $
 */
public class MultiplexerChannelFactory implements ChannelFactory
{

   // Constants ------------------------------------------------------------------------------------


   // Attributes -----------------------------------------------------------------------------------
   JChannelFactory jChannelFactory;
   String dataStack;
   String controlStack;
   String uniqueID;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public MultiplexerChannelFactory(JChannelFactory jChannelFactory,
                                    String uniqueID,
                                    String controlStack,
                                    String dataStack)
   {
      this.jChannelFactory = jChannelFactory;
      this.uniqueID = uniqueID;
      this.dataStack = dataStack;
      this.controlStack = controlStack;
   }

   // Public ---------------------------------------------------------------------------------------


   public String getDataStack()
   {
      return dataStack;
   }

   public void setDataStack(String dataStack)
   {
      this.dataStack = dataStack;
   }

   public String getControlStack()
   {
      return controlStack;
   }

   public void setControlStack(String controlStack)
   {
      this.controlStack = controlStack;
   }

   public String getUniqueID()
   {
      return uniqueID;
   }

   public void setUniqueID(String uniqueID)
   {
      this.uniqueID = uniqueID;
   }

   public Channel createControlChannel() throws Exception
   {
      return jChannelFactory.createMultiplexerChannel(controlStack, uniqueID + "-CTRL", Boolean.TRUE, uniqueID);
   }

   public Channel createDataChannel() throws Exception
   {
      return jChannelFactory.createMultiplexerChannel(dataStack, uniqueID + "-DATA", Boolean.TRUE, uniqueID);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private  -------------------------------------------------------------------------------------

   // Inner classes  -------------------------------------------------------------------------------

}
