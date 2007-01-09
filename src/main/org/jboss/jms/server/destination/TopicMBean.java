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
package org.jboss.jms.server.destination;

import java.util.List;

/**
 * A TopicMBean
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public interface TopicMBean
{
   //JMX attributes
    
   int getAllMessageCount() throws Exception;
      
   int getDurableMessageCount() throws Exception;
      
   int getNonDurableMessageCount() throws Exception;
   
   int getAllSubscriptionsCount() throws Exception;

   int getDurableSubscriptionsCount() throws Exception;
   
   int getNonDurableSubscriptionsCount() throws Exception;
   
   // JMX operations
   
   void removeAllMessages() throws Exception;
      
   List listAllSubscriptions() throws Exception;
   
   List listDurableSubscriptions() throws Exception;
   
   List listNonDurableSubscriptions() throws Exception;
   
   String listAllSubscriptionsAsHTML() throws Exception;
   
   String listDurableSubscriptionsAsHTML() throws Exception;
   
   String listNonDurableSubscriptionsAsHTML() throws Exception;
   
   List listAllMessages(String subscriptionId) throws Exception;
   
   List listAllMessages(String subscriptionId, String selector) throws Exception;
   
   List listDurableMessages(String subscriptionId) throws Exception;
   
   List listDurableMessages(String subscriptionId, String selector) throws Exception;
   
   List listNonDurableMessages(String subscriptionId) throws Exception;
   
   List listNonDurableMessages(String subscriptionId, String selector) throws Exception;
   
   List getMessageCounters() throws Exception;      
}
