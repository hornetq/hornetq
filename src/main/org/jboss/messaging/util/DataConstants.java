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

package org.jboss.messaging.util;

/**
 * 
 * A DataConstants
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class DataConstants
{
   public static final int SIZE_INT = 4;   
   public static final int SIZE_BOOLEAN = 1;   
   public static final int SIZE_LONG = 8;   
   public static final int SIZE_BYTE = 1;
   public static final int SIZE_SHORT = 2;
   public static final int SIZE_DOUBLE = 8;
   public static final int SIZE_FLOAT = 4;
   public static final int SIZE_CHAR = 2;
 
   public static final byte TRUE = 1;
   public static final byte FALSE = 0;
      
   public static final byte NULL = 0;   
   public static final byte NOT_NULL = 1;
   
   public static final byte BOOLEAN = 2;	
   public static final byte BYTE = 3;
   public static final byte BYTES = 4;
   public static final byte SHORT = 5;
   public static final byte INT = 6;
   public static final byte LONG = 7;
   public static final byte FLOAT = 8;
   public static final byte DOUBLE = 9;
   public static final byte STRING = 10;
   public static final byte CHAR = 11;      
}
