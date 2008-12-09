/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.client;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * A ClientSessionFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionFactory
{
   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks) throws MessagingException;

   ClientSession createSession(String username,
                               String password,
                               boolean xa,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               int ackBatchSize) throws MessagingException;

   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge) throws MessagingException;

   void setConsumerWindowSize(int size);

   int getConsumerWindowSize();

   void setSendWindowSize(int size);

   int getSendWindowSize();

   void setConsumerMaxRate(int rate);

   int getConsumerMaxRate();

   void setProducerMaxRate(int rate);

   int getProducerMaxRate();

   int getMinLargeMessageSize();

   void setMinLargeMessageSize(int minLargeMessageSize);

   boolean isBlockOnPersistentSend();

   void setBlockOnPersistentSend(boolean blocking);

   boolean isBlockOnNonPersistentSend();

   void setBlockOnNonPersistentSend(boolean blocking);

   boolean isBlockOnAcknowledge();

   void setBlockOnAcknowledge(boolean blocking);

   boolean isAutoGroup();

   void setAutoGroup(boolean autoGroup);

   int getAckBatchSize();

   void setAckBatchSize(int ackBatchSize);

   boolean isPreAcknowledge();

   void setPreAcknowledge(boolean preAcknowledge);

   long getPingPeriod();

   long getCallTimeout();

   int getMaxConnections();

   // TransportConfiguration getTransportConfiguration();
   //   
   // TransportConfiguration getBackupTransportConfiguration();

   void close();
}
