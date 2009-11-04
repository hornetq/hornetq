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

import org.hornetq.core.logging.LogDelegate;
import org.hornetq.core.logging.LogDelegateFactory;

/**
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class SysoutLoggerDelegateFactory implements LogDelegateFactory
{
   public LogDelegate createDelegate(Class<?> clazz)
   {
      return new SysoutLoggerDelegate();
   }      
   
   public class SysoutLoggerDelegate implements LogDelegate
   {
      public void debug(Object message, Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void debug(Object message)
      { 
         System.out.println(message);
      }

      public void error(Object message, Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void error(Object message)
      { 
         System.out.println(message);
      }

      public void fatal(Object message, Throwable t)
      { 
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void fatal(Object message)
      {
         System.out.println(message);
      }

      public void info(Object message, Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void info(Object message)
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

      public void trace(Object message, Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void trace(Object message)
      {
         System.out.println(message);
      }

      public void warn(Object message, Throwable t)
      {
         System.out.println(message);
         t.printStackTrace(System.out);
      }

      public void warn(Object message)
      { 
         System.out.println(message);
      }
      
   }
}


