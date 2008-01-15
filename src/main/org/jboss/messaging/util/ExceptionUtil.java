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

import java.util.UUID;

import javax.jms.JMSException;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.logging.Logger;

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
    * The strategy for what how we propagate Throwables from the server to the client
    * in the event of one occuring on the server in a client initiated invocation is as
    * follows:
    * 1) We always log the Throwable irrespective of it's type - this is so system administrators
    * have a record of what has happened - typically they may configure log4j to send an email
    * or create some other alert when this happens
    * 2) If the exception is a JMSException that was created and thrown in the messaging code, then
    * this is propagated back to the client
    * 4) Any other Errors or Exceptions (e.g. SQLException) are rethrown as a JMSException
    * - we do not want to propagate the original exception back to the client since the client may not
    * have the correct jars to receive the exception - (e.g. if it was an exception thrown from a postgressql driver)
    * Also there maybe security reasons we do not want to expose the specific original exception to the client
    */
   public static JMSException handleJMSInvocation(Throwable t, String msg)
   {
      //We create a GUID and log it and send it in the client exception.
      //This allows what is received at the client to be correlated if necessary
      //to what is logged in the server logs
      String id = UUID.randomUUID().toString();
      
      //First we log the Throwable
      log.error(msg + " [" + id + "]", t);
      
      if (t instanceof JMSException)
      {         
         return (JMSException)t;
      }
      else
      {
         JMSException e = new MessagingJMSException("A failure has occurred during processing of the request. " + 
                                                    "Please consult the server logs for more details. " + msg + " [" + id + "]");    
         return e;
      }    
   }   
   
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
