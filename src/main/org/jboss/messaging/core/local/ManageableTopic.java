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

import java.util.List;

import javax.jms.InvalidSelectorException;

import org.jboss.messaging.core.ManageableCoreDestination;

/**
 * Manageable interface for a topic.
 *
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public interface ManageableTopic extends ManageableCoreDestination
{
   /**
    * Get all subscription count.
    * @return number of registered subscriptions
    */
   int subscriptionCount();
   
   /**
    * Get subscription count.
    * @param durable If true, return durable subscription count.
    *                If false, return non-durable subscription count.
    * @return either durable or non-durable subscription count.
    */
   int subscriptionCount(boolean durable);
   
   /**
    * Get all subscription list.
    * @return List of String[] {ChannelID, ClientID, SubscriptionName}. Never null. 
    */
   List getSubscriptions();
   
   /**
    * Get durable/non-durable subscription list.
    * @param durable If true, return durable subscription list.
    *                If false, return non-durable subscription list.
    * @return List of String[] {ChannelID, ClientID, SubscriptionName}. Never null. 
    */
   List getSubscriptions(boolean durable);
   
   /**
    * Get list of messages that is in certain subscription and meet the condition of selector.
    * @param channelID String array element 0 that you get from getSubscriptions().
    * @param clientID String array element 1 that you get from getSubscriptions().
    * @param subName String array element 2 that you get from getSubscriptions().
    * @param selector Filter expression
    * @return List of javax.jms.Message. Never null.
    * @throws InvalidSelectorException
    */
   List getMessages(long channelID, String clientID, String subName, String selector) throws InvalidSelectorException;
   
   // TODO adding more manageable operations
}
