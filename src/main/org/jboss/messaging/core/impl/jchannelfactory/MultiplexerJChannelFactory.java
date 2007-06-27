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

package org.jboss.messaging.core.impl.jchannelfactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.messaging.core.contract.JChannelFactory;
import org.jgroups.JChannel;

/**
 * A JChannelFactory that will use the MBean JChannelFactory interface
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class MultiplexerJChannelFactory implements JChannelFactory
{

   // Constants ------------------------------------------------------------------------------------

   private static final String[] MUX_SIGNATURE = new String[]{"java.lang.String",
      "java.lang.String", "boolean", "java.lang.String"};

   // Attributes -----------------------------------------------------------------------------------
   MBeanServer server;
   ObjectName channelFactory;
   String asyncStack;
   String syncStack;
   String uniqueID;
   private static final String MUX_OPERATION = "createMultiplexerChannel";

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public MultiplexerJChannelFactory(MBeanServer server,
                                    ObjectName channelFactory,
                                    String uniqueID,
                                    String syncStack,
                                    String asyncStack)
   {
      this.server = server;
      this.channelFactory = channelFactory;
      this.uniqueID = uniqueID;
      this.asyncStack = asyncStack;
      this.syncStack = syncStack;
   }

   // Public ---------------------------------------------------------------------------------------

   public MBeanServer getServer()
   {
      return server;
   }

   public void setServer(MBeanServer server)
   {
      this.server = server;
   }

   public ObjectName getChannelFactory()
   {
      return channelFactory;
   }

   public void setChannelFactory(ObjectName channelFactory)
   {
      this.channelFactory = channelFactory;
   }

   public String getAsyncStack()
   {
      return asyncStack;
   }

   public void setAsyncStack(String asyncStack)
   {
      this.asyncStack = asyncStack;
   }

   public String getSyncStack()
   {
      return syncStack;
   }

   public void setSyncStack(String syncStack)
   {
      this.syncStack = syncStack;
   }

   public String getUniqueID()
   {
      return uniqueID;
   }

   public void setUniqueID(String uniqueID)
   {
      this.uniqueID = uniqueID;
   }

   public JChannel createControlChannel() throws Exception
   {
      return (JChannel) server.invoke(this.channelFactory, MUX_OPERATION,
         new Object[]{syncStack, uniqueID, Boolean.TRUE, uniqueID}, MUX_SIGNATURE);
   }

   public JChannel createDataChannel() throws Exception
   {
      return (JChannel) server.invoke(this.channelFactory, MUX_OPERATION,
         new Object[]{asyncStack, uniqueID, Boolean.TRUE, uniqueID}, MUX_SIGNATURE);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private  -------------------------------------------------------------------------------------

   // Inner classes  -------------------------------------------------------------------------------

}
