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

package org.jboss.messaging.jms.server;

import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.Serializable;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConnectionInfo implements Serializable
{
	private static final long serialVersionUID = -2525719954021417273L;
	
	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("HH:mm:ss, EEE, MMM d, yyyy");

   public enum status{ STARTED, STOPPED }

   private final long id;
   private final String user;
   private final String address;
   private final boolean started;
   private final long created;

   public ConnectionInfo(final long id, final String user, final String address,
   		                final boolean started, final long created)
   {
      this.id = id;
      this.user = user;
      this.address = address;
      this.started = started;
      this.created = created;
   }

   public long getId()
   {
      return id;
   }

   public String getUser()
   {
      return user;
   }

   public String getAddress()
   {
      return address;
   }

   public status getStatus()
   {
      return started? status.STARTED:status.STOPPED;
   }

    public String getTimeCreated()
   {

      Calendar calendar = Calendar.getInstance();
      calendar.setTime(new Date(created));
      return SIMPLE_DATE_FORMAT.format(calendar.getTime());
   }

   public String getAliveTime()
   {
      StringBuilder builder = new StringBuilder();
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(new Date(System.currentTimeMillis() - created));
      builder.append(calendar.get(Calendar.DAY_OF_YEAR) - 1).append(" days ").append(calendar.get(Calendar.HOUR_OF_DAY)).
              append(" hours ").append(calendar.get(Calendar.MINUTE)).append(" minutes ").append(calendar.get(Calendar.SECOND)).
              append(" seconds.");
      return builder.toString();
   }

   public String toString()
   {
      return  id + (user!=null?"(" + user + ")":"") + "@" + address + " started at " + getTimeCreated() + "," + "uptime " + getAliveTime() + "\n"; 
   }
}
