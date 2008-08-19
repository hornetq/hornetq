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

package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.CommandManager;

/**
 *
 * A RemotingConnection
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public interface RemotingConnection
{
   Object getID();

   Packet sendBlocking(long targetID, long executorID, Packet packet, CommandManager cm);

   void sendOneWay(long targetID, long executorID, Packet packet);

   // TODO this method is only used in tests so should be removed
   Packet sendBlocking(Packet packet, CommandManager cm) throws MessagingException;

   void sendOneWay(Packet packet);

   void addFailureListener(FailureListener listener);

   boolean removeFailureListener(FailureListener listener);

   PacketDispatcher getPacketDispatcher();

   Location getLocation();

   MessagingBuffer createBuffer(int size);

   void fail(MessagingException me);

   void destroy();
}
