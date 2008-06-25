/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.bridge;

import javax.transaction.TransactionManager;

import org.jboss.messaging.core.server.MessagingComponent;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface Bridge extends MessagingComponent
{
   void pause() throws Exception;

   void resume() throws Exception;

   DestinationFactory getSourceDestinationFactory();

   void setSourceDestinationFactory(DestinationFactory dest);

   DestinationFactory getTargetDestinationFactory();

   void setTargetDestinationFactory(DestinationFactory dest);

   String getSourceUsername();

   void setSourceUsername(String name);

   String getSourcePassword();

   void setSourcePassword(String pwd);

   String getTargetUsername();

   void setTargetUsername(String name);

   String getTargetPassword();

   void setTargetPassword(String pwd);

   String getSelector();

   void setSelector(String selector);

   long getFailureRetryInterval();

   void setFailureRetryInterval(long interval);

   int getMaxRetries();

   void setMaxRetries(int retries);

   QualityOfServiceMode getQualityOfServiceMode();

   void setQualityOfServiceMode(QualityOfServiceMode mode);

   int getMaxBatchSize();

   void setMaxBatchSize(int size);

   long getMaxBatchTime();

   void setMaxBatchTime(long time);

   String getSubscriptionName();

   void setSubscriptionName(String subname);

   String getClientID();

   void setClientID(String clientID);

   boolean isAddMessageIDInHeader();

   void setAddMessageIDInHeader(boolean value);

   boolean isPaused();

   boolean isFailed();

   void setSourceConnectionFactoryFactory(ConnectionFactoryFactory cff);

   void setTargetConnectionFactoryFactory(ConnectionFactoryFactory cff);

   void setTransactionManager(TransactionManager tm);

}