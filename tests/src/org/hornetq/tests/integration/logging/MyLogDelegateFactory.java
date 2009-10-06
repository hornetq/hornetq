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

package org.hornetq.tests.integration.logging;

import org.hornetq.core.logging.LogDelegate;
import org.hornetq.core.logging.LogDelegateFactory;

/**
 * A MyLogDelegateFactory
 *
 * @author Tim Fox
 *
 *
 */
public class MyLogDelegateFactory implements LogDelegateFactory
{
   public LogDelegate createDelegate(Class<?> clazz)
   {
      return new MyLogDelegate();
   }      
   
   public class MyLogDelegate implements LogDelegate
   {
      public void debug(Object message, Throwable t)
      {
      }

      public void debug(Object message)
      { 
      }

      public void error(Object message, Throwable t)
      {
      }

      public void error(Object message)
      { 
      }

      public void fatal(Object message, Throwable t)
      { 
      }

      public void fatal(Object message)
      {
      }

      public void info(Object message, Throwable t)
      {
      }

      public void info(Object message)
      {
      }

      public boolean isDebugEnabled()
      {
         return false;
      }

      public boolean isInfoEnabled()
      {
         return false;
      }

      public boolean isTraceEnabled()
      {
         return false;
      }

      public void trace(Object message, Throwable t)
      {
      }

      public void trace(Object message)
      {
      }

      public void warn(Object message, Throwable t)
      {
      }

      public void warn(Object message)
      { 
      }
      
   }
}


