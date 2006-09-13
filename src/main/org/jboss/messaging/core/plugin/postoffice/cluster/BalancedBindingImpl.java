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

import org.jboss.messaging.core.plugin.postoffice.BindingImpl;

/**
 * A BalancedBindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BalancedBindingImpl extends BindingImpl implements BalancedBinding
{
   private double consumptionRate;
   
   private int messageCount;
   
   public BalancedBindingImpl()
   {
   }

   public BalancedBindingImpl(String nodeId, String queueName, String condition, String selector, long channelId, boolean durable)
   {
      super(nodeId, queueName, condition, selector, channelId, durable);
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

 

}
