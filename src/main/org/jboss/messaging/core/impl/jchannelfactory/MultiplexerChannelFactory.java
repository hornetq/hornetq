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

import org.jboss.messaging.core.contract.ChannelFactory;
import org.jgroups.Channel;

/**
 * A ChannelFactory that will use the MBean ChannelFactory interface
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class MultiplexerChannelFactory implements ChannelFactory
{

   // Constants ------------------------------------------------------------------------------------

   private static final String[] MUX_SIGNATURE = new String[]{"java.lang.String",
      "java.lang.String", "boolean", "java.lang.String"};

   // Attributes -----------------------------------------------------------------------------------
   MBeanServer server;
   ObjectName channelFactory;
   String dataStack;
   String controlStack;
   String uniqueID;
   private static final String MUX_OPERATION = "createMultiplexerChannel";

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public MultiplexerChannelFactory(MBeanServer server,
                                    ObjectName channelFactory,
                                    String uniqueID,
                                    String controlStack,
                                    String dataStack)
   {
      this.server = server;
      this.channelFactory = channelFactory;
      this.uniqueID = uniqueID;
      this.dataStack = dataStack;
      this.controlStack = controlStack;
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
      return (Channel) server.invoke(this.channelFactory, MUX_OPERATION,
         new Object[]{controlStack, uniqueID + "-CTRL", Boolean.TRUE, uniqueID}, MUX_SIGNATURE);
   }

   //Note that for the data channel we don't receive state immediately after connecting so third param
   //must be false http://jira.jboss.com/jira/browse/JBMESSAGING-1120
   public Channel createDataChannel() throws Exception
   {
      return (Channel) server.invoke(this.channelFactory, MUX_OPERATION,
         new Object[]{dataStack, uniqueID + "-DATA", Boolean.FALSE, uniqueID}, MUX_SIGNATURE);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private  -------------------------------------------------------------------------------------

   // Inner classes  -------------------------------------------------------------------------------

}
