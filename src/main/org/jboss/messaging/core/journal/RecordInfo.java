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

package org.jboss.messaging.core.journal;

/**
 * 
 * A RecordInfo
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class RecordInfo
{
   public RecordInfo(final long id, final byte userRecordType, final byte[] data, final boolean isUpdate)
   {
      this.id = id;

      this.userRecordType = userRecordType;

      this.data = data;

      this.isUpdate = isUpdate;
   }

   public final long id;

   public final byte userRecordType;

   public final byte[] data;

   public boolean isUpdate;

   public byte getUserRecordType()
   {
      return userRecordType;
   }

   @Override
   public int hashCode()
   {
      return (int)(id >>> 32 ^ id);
   }

   @Override
   public boolean equals(final Object other)
   {
      RecordInfo r = (RecordInfo)other;

      return r.id == id;
   }
   
   public String toString()
   {
      return ("RecordInfo (id=" + id + ", userRecordType = " + userRecordType + ", data.length = " + data.length + ", isUpdate = " + this.isUpdate);
   }

}
