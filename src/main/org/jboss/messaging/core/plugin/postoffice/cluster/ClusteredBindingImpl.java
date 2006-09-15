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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.plugin.postoffice.BindingImpl;

/**
 * 
 * A ClusteredBindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredBindingImpl extends BindingImpl implements ClusteredBinding
{
   private double consumptionRate;
   
   private int messageCount;
   
   public ClusteredBindingImpl()
   {
   }

   public ClusteredBindingImpl(String nodeId, String queueName, String condition, Filter filter, long channelId, boolean durable)
   {
      super(nodeId, queueName, condition, filter, channelId, durable);
   }

   public double getConsumptionRate()
   {
      return consumptionRate;
   }

   public int getMessageCount()
   {
      return messageCount;
   }

   public void setConsumptionRate(double consumptionRate)
   {
      this.consumptionRate = consumptionRate;
   }

   public void setMessageCount(int messageCount)
   {
      this.messageCount = messageCount;
   }

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);
      
      consumptionRate = in.readDouble();
      
      messageCount = in.readInt();
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      out.writeDouble(consumptionRate);
      
      out.writeInt(messageCount);
   }
}
