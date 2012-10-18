/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.api.core;

import static org.hornetq.api.core.HornetQExceptionType.QUEUE_DOES_NOT_EXIST;

/**
 * An operation failed because a queue does not exist on the server.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 4/30/12
 */
public final class HornetQNonExistentQueueException extends HornetQException
{
   private static final long serialVersionUID = -8199298881947523607L;

   public HornetQNonExistentQueueException()
   {
      super(QUEUE_DOES_NOT_EXIST);
   }

   public HornetQNonExistentQueueException(String msg)
   {
      super(QUEUE_DOES_NOT_EXIST, msg);
   }
}
