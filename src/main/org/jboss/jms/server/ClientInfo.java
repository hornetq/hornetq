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
package org.jboss.jms.server;

import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ClientInfo
{
   private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("HH:mm:ss, EEE, MMM d, yyyy");

   enum status{ STARTED, STOPPED }

   private String user;
   private String address;
   private boolean started;
   private long created;

   public ClientInfo(String user, String address, boolean started, long created)
   {
      this.user = user;
      this.address = address;
      this.started = started;
      this.created = created;
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
}
