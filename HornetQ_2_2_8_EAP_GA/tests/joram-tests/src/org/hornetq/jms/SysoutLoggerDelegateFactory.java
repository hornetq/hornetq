/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms;

import org.hornetq.spi.core.logging.LogDelegate;
import org.hornetq.spi.core.logging.LogDelegateFactory;

/**
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class SysoutLoggerDelegateFactory implements LogDelegateFactory
{
   public LogDelegate createDelegate(final Class<?> clazz)
   {
      return new SysoutLoggerDelegate();
   }

   public class SysoutLoggerDelegate implements LogDelegate
   {
      public void debug(final Object message, final Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void debug(final Object message)
      {
         System.out.println(message);
      }

      public void error(final Object message, final Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void error(final Object message)
      {
         System.out.println(message);
      }

      public void fatal(final Object message, final Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void fatal(final Object message)
      {
         System.out.println(message);
      }

      public void info(final Object message, final Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void info(final Object message)
      {
         System.out.println(message);
      }

      public boolean isDebugEnabled()
      {
         return true;
      }

      public boolean isInfoEnabled()
      {
         return true;
      }

      public boolean isTraceEnabled()
      {
         return true;
      }

      public void trace(final Object message, final Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void trace(final Object message)
      {
         System.out.println(message);
      }

      public void warn(final Object message, final Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void warn(final Object message)
      {
         System.out.println(message);
      }

   }
}
