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
package org.jboss.messaging.tests.integration.queue;

import org.jboss.messaging.core.persistence.impl.nullpm.NullStorageManager;
import org.jboss.messaging.core.server.ServerMessage;

import java.util.List;
import java.util.ArrayList;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class FakeStorageManager extends NullStorageManager
{
   List<Long> messageIds = new ArrayList<Long>();

   List<Long> ackIds = new ArrayList<Long>();

   public void storeMessage(ServerMessage message) throws Exception
   {
      messageIds.add(message.getMessageID());
   }

   public void storeMessageTransactional(long txID, ServerMessage message) throws Exception
   {
      messageIds.add(message.getMessageID());
   }

   public void deleteMessageTransactional(long txID, long queueID, long messageID) throws Exception
   {
      messageIds.remove(messageID);
   }

   public void deleteMessage(long messageID) throws Exception
   {
      messageIds.remove(messageID);
   }

   public void storeAcknowledge(long queueID, long messageID) throws Exception
   {
      ackIds.add(messageID);
   }

   public void storeAcknowledgeTransactional(long txID, long queueID, long messageiD) throws Exception
   {
      ackIds.add(messageiD);
   }
}
