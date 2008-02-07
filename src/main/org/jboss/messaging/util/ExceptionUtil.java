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
package org.jboss.messaging.util;


/**
 * A ExceptionUtil

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ExceptionUtil
{
   private static final Logger log = Logger.getLogger(ExceptionUtil.class);
    
   
   /*
    * This method is used to log any Throwables occurring in the execution of JMX methods before
    * propagating them back to the caller.
    * If we don't log them then we have no record of them occurring on the server.
    */
   public static Exception handleJMXInvocation(Throwable t, String msg)
   {
      log.error(msg, t);
      
      if (t instanceof RuntimeException)
      {
         throw (RuntimeException)t;
      }
      else if (t instanceof Error)
      {
         throw (Error)t;
      }
      else if (t instanceof Exception)
      {
         //Some other non RuntimeException
         return (Exception)t;
      }
      else
      {
         //Some other subclass of Throwable
         throw new RuntimeException(msg);
      }         
   }
}
