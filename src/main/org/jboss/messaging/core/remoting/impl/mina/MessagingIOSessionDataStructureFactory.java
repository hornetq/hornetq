/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.IoSessionAttributeMap;
import org.apache.mina.common.IoSessionDataStructureFactory;
import org.apache.mina.common.WriteRequest;
import org.apache.mina.common.WriteRequestQueue;

/**
 * 
 * A MessagingIOSessionDataStructureFactory
 * 
 * @author The Apache MINA Project (dev@mina.apache.org)
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MessagingIOSessionDataStructureFactory implements IoSessionDataStructureFactory
{

   public IoSessionAttributeMap getAttributeMap(IoSession session)
         throws Exception
   {
      return new ConcurrentIoSessionAttributeMap();
   }

   public WriteRequestQueue getWriteRequestQueue(IoSession session)
         throws Exception
   {
      return new ConcurrentWriteRequestQueue();
   }
   
   
   private static class ConcurrentIoSessionAttributeMap implements IoSessionAttributeMap {

      private final ConcurrentMap<Object, Object> attributes = new ConcurrentHashMap<Object, Object>(4);

      public Object getAttribute(IoSession session, Object key, Object defaultValue) {
          if (key == null) {
              throw new NullPointerException("key");
          }

          Object answer = attributes.get(key);
          if (answer == null) {
              return defaultValue;
          } else {
              return answer;
          }
      }

      public Object setAttribute(IoSession session, Object key, Object value) {
          if (key == null) {
              throw new NullPointerException("key");
          }

          if (value == null) {
              return attributes.remove(key);
          } else {
              return attributes.put(key, value);
          }
      }

      public Object setAttributeIfAbsent(IoSession session, Object key, Object value) {
          if (key == null) {
              throw new NullPointerException("key");
          }

          if (value == null) {
              return null;
          }
          
          return attributes.putIfAbsent(key, value);
      }

      public Object removeAttribute(IoSession session, Object key) {
          if (key == null) {
              throw new NullPointerException("key");
          }

          return attributes.remove(key);
      }

      public boolean removeAttribute(IoSession session, Object key, Object value) {
          if (key == null) {
              throw new NullPointerException("key");
          }

          if (value == null) {
              return false;
          }
          
          return attributes.remove(key, value);
      }

      public boolean replaceAttribute(IoSession session, Object key, Object oldValue, Object newValue) {
         return attributes.replace(key, oldValue, newValue);
      }

      public boolean containsAttribute(IoSession session, Object key) {
          return attributes.containsKey(key);
      }

      public Set<Object> getAttributeKeys(IoSession session) {
          return new HashSet<Object>(attributes.keySet());          
      }

      public void dispose(IoSession session) throws Exception {
      }
  }
   
   
//   private static class DefaultWriteRequestQueue implements WriteRequestQueue
//   {
//      private final Queue<WriteRequest> q = new CircularQueue<WriteRequest>(16);
//      
//      public void dispose(IoSession session) {
//      }
//      
//      public void clear(IoSession session) {
//          q.clear();
//      }
//
//      public synchronized boolean isEmpty(IoSession session) {
//          return q.isEmpty();
//      }
//
//      public synchronized void offer(IoSession session, WriteRequest writeRequest) {
//          q.offer(writeRequest);
//      }
//
//      public synchronized WriteRequest poll(IoSession session) {
//          return q.poll();
//      }
//      
//      @Override
//      public String toString() {
//          return q.toString();
//      }
//  }
   
   private static class ConcurrentWriteRequestQueue implements WriteRequestQueue
   {
      private final Queue<WriteRequest> q = new ConcurrentLinkedQueue<WriteRequest>();
      
      public void dispose(IoSession session) {
      }
      
      public void clear(IoSession session) {
          q.clear();
      }

      public synchronized boolean isEmpty(IoSession session) {
          return q.isEmpty();
      }

      public synchronized void offer(IoSession session, WriteRequest writeRequest) {
          q.offer(writeRequest);
      }

      public synchronized WriteRequest poll(IoSession session) {
          return q.poll();
      }
      
      @Override
      public String toString() {
          return q.toString();
      }
  }

}
