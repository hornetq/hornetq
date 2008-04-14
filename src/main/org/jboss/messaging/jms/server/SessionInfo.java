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
package org.jboss.messaging.jms.server;

import java.io.Serializable;

/**
 * Information regarding to a session
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SessionInfo implements Serializable
{
   private final long id;
   
   private final long connectionID;

   public SessionInfo(final long id, final long connectionID)
   {
      this.id = id;
      this.connectionID = connectionID;
   }

   public long getId()
   {
      return id;
   }

   public long getConnectionID()
   {
      return connectionID;
   }

   public String toString()
   {
      return id + "@" + connectionID + "\n";
   }
}
