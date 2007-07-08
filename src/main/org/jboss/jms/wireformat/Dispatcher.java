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
package org.jboss.jms.wireformat;

import java.util.Map;

import org.jboss.jms.server.endpoint.advised.AdvisedSupport;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A NewJMSDispatcher
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class Dispatcher
{
   private static final Logger log = Logger.getLogger(Dispatcher.class);
   
   public static Dispatcher instance = new Dispatcher();
   
   private Map targets;
   
   private Dispatcher()
   {
      targets = new ConcurrentReaderHashMap();
   }
   
   public Object getTarget(String id)
   {
      return targets.get(id);
   }
      
   public void registerTarget(String id, AdvisedSupport obj)
   {
      targets.put(id, obj);
   }
      
   public boolean unregisterTarget(String id, Object endpoint)
   {
      // Note that we pass the object id in, this is as a sanity check to ensure the object we are
      // deregistering is the correct one since there have been bugs related to deregistering the
      // wrong object. This can happen if an earlier test opens a connection then the test ends
      // without closing the connection, then on the server side the serverpeer is restarted which
      // resets the object counter, so a different object is registered under the id of the old
      // object. Remoting then times out the old connection and dereigsters the new object which is
      // registered under the same id.
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-812
          
      AdvisedSupport advised = (AdvisedSupport)(targets.get(id));
      
      if (advised == null)
      {
         // This can happen due to http://jira.jboss.com/jira/browse/JBMESSAGING-812
         log.warn("Cannot find object with id " + id + " to register");
         return false;
      }
           
      if (advised.getEndpoint() != endpoint)
      {
         log.warn("The object you are trying to deregister is not the same as the one you registered!");
         return false;
      }
      else
      {      
         return targets.remove(id) != null;
      }
   }
     
}
