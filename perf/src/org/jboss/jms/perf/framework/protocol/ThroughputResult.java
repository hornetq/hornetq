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
package org.jboss.jms.perf.framework.protocol;

import org.jboss.jms.perf.framework.remoting.ResultSupport;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ThroughputResult extends ResultSupport
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -6238059261642836113L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private long time;
   private long messages;

   // Constructors --------------------------------------------------

   public ThroughputResult(long time, long messages)
   {
      this.time = time;
      this.messages = messages;
   }

   // Public --------------------------------------------------------

   public long getTime()
   {
      return time;
   }

   public long getMessages()
   {
      return messages;
   }

   public double getThroughput()
   {
      return 1000 * (double)messages / time;
   }

   public String toString()
   {
      if (request == null)
      {
         return "INCOMPLETE THROUGHPUT RESULT";
      }

      boolean isSender = request instanceof SendJob;
      boolean isReceiver = request instanceof ReceiveJob;
      boolean isDrain = request instanceof DrainJob;

      StringBuffer sb = new StringBuffer();

      sb.append(request.toString());
      sb.append(isSender ? " sent " : (isReceiver ? " received " : " drained "));
      sb.append(getMessages()).append(" messages");

      if (!isDrain)
      {
         Job job = (Job)request;
         double t = getThroughput();
         t = ((double)Math.round(t * 100))/100;
         sb.append(" in ").append(getTime()).append(" ms with a target rate ").
            append(job.getRate()).append(" messages/sec, real rate ").
            append(t).append(" messages/sec");
      }

      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
