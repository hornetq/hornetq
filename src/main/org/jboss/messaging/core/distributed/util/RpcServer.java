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
package org.jboss.messaging.core.distributed.util;

import org.jboss.logging.Logger;
import org.jgroups.Address;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * An RpcServer is installed as "server object" with a RpcDispatcher instance.
 * <p>
 * The RpcServer allows dynamic registration of other server objects, under different "categories".
 * For each category, the RpcServer allows registration of one or more equivalent server delegate
 * objects, on which methods will be serially invoked when a remote method call is received from the
 * group.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RpcServer
{

   private static final Logger log = Logger.getLogger(RpcServer.class);

   // Attributes ----------------------------------------------------

   protected Map servers;

   // Constructors --------------------------------------------------

   public RpcServer()
   {
      servers = new HashMap();
   }

   // Public --------------------------------------------------------
   
   /**
    * Generic detyped interface to be accessed remotely.
    *
    * @param category - the category of equivalent server delegates to invoke the method on.
    * @param methodName - the name of the method to be invoked by reflection on the registered
    *        serverObjects.
    * @param args - the method's arguments.
    * @param signature - the method's signature. The array contains fully qualified class names.
    *
    * @return a Collection containing SubordinateServerResponse instances. It always returns a
    *         Collection, an empty collection in the case that no servers were found, never null.
    *
    * @exception ClassNotFoundException if a fully qualified name specified in the method's
    *            signature cannot be used to load a valid class.
    *
    *
    * @exception NoSuchMethodException if no method with the specified name or with the specified
    *            signature can be identified by reflection on the server object.
    * @exception IllegalAccessException
    * @exception IllegalArgumentException
    * @exception InvocationTargetException if the underlying method throws an exception.
    */
   public Collection invoke(Serializable category, String methodName,
                            Object[] args, String[] signature)
         throws Exception
   {

      Class[] parameterTypes = new Class[signature.length];
      for (int i = 0; i < signature.length; i++)
      {
         parameterTypes[i] = Class.forName(signature[i]);
      }

      synchronized(servers)
      {
         Object o = servers.get(category);
         if (o == null)
         {
            return Collections.EMPTY_SET;
         }
         Set equivalents = null;
         if (o instanceof Set)
         {
            equivalents = (Set)o;
         }
         else
         {
            // deal with unique servers in a consistent manner
            equivalents = new HashSet();
            equivalents.add(o);
         }
         Collection results = new ArrayList();
         for(Iterator i = equivalents.iterator(); i.hasNext(); )
         {
            ServerFacade subordinate = (ServerFacade)i.next();
            Serializable id = subordinate.getID();
            try
            {
               Method method = subordinate.getClass().getMethod(methodName, parameterTypes);
               Object result = method.invoke(subordinate, args);
               results.add(new SubordinateServerResponse(id, result));

               if (log.isTraceEnabled())
               {
                  log.trace("RpcServer invocation of " + method.getName() +
                            "() on category=" + category + ", delegate=" + id + " successful");
               }
            }
            catch(Throwable t)
            {
               log.debug("RpcServer invocation on category =" + category + ", delegate =" + id + "failed", t);
               results.add(new SubordinateServerResponse(id, t));
            }
         }
         return results;
      }
   }

   /**
    * Registers a server delegate object with the RpcServer, under the specified category.
    *
    * <p> Note that more than one "equivalent" server objects can be registered under the same
    * category. However, no equal (as per the "equal()" method semantics) are allowed to be
    * registered under the same category. If an unique server delegate was already registered under
    * the category, the call fails (returns false).
    *
    * @param category - the server category.
    * @param subordinate - the server delegate object to be registered.
    * @return true if the server was sucessfully registered or false if an "equal" object was
    *         already registered under the specified server ID. The method returns false if an
    *         unique server delegate was already registered under the category.
    *
    * @exception NullPointerException if trying to register a null instance.
    *
    */
   public boolean register(Serializable category, ServerFacade subordinate)
   {
      if (subordinate == null)
      {
         throw new NullPointerException("null server delegate");
      }

      boolean result = false;
      synchronized(servers)
      {
         Object o = servers.get(category);
         if (o == null || o instanceof Set)
         {
            Set equivalents = (Set)o;
            if (equivalents == null)
            {
               equivalents = new HashSet();
               servers.put(category, equivalents);
            }
            result = equivalents.add(subordinate);
         }
         log.debug("register " + category + " -> " + subordinate.getID() + "[" +
                   subordinate.getClass().getName() + "]: " + result);
         return result;
      }
   }

   /**
    * Registers a <i>unique</i> server delegate object with the RpcServer, under the specified
    * category.
    *
    * <p>If a server delegate is already registered under this category, the call should fail
    * (return false).
    *
    * @param category - the server category.
    * @param subordinate - the server delegate object to be registered.
    * @return true if the server was sucessfully registered or false if a server delegate was
    *         already registered under the category.
    *
    * @exception NullPointerException if trying to register a null instance.
    */
   public boolean registerUnique(Serializable category, ServerFacade subordinate)
   {
      if (subordinate == null)
      {
         throw new NullPointerException("null server delegate");
      }

      boolean result = false;
      synchronized(servers)
      {
         Object o = servers.get(category);
         if (o == null || (o instanceof Set) && (((Set)o)).isEmpty())
         {
            servers.put(category, subordinate);
            result = true;
         }
         log.debug("registerUnique " + category + " -> " + subordinate.getID() + "[" +
                   subordinate.getClass().getName() + "]: " + result);
         return result;
      }
   }

   /**
    * @return true if the server delegate was sucessfully unregistered or false if the specified
    *         server delegate was not found under the specified category.
    */
   public boolean unregister(Serializable category, ServerFacade subordinate)
   {
      synchronized(servers)
      {
         Object o = servers.get(category);
         if (o == null)
         {
            return false;
         }
         if (o instanceof Set)
         {
            Set equivalents = (Set)o;
            return equivalents.remove(subordinate);
         }
         else
         {
            // unique subordinate
            if (o != subordinate)
            {
               return false;
            }
            servers.remove(category);
            return true;
         }
      }
   }

   /**
    * @param category - the server category.
    * @return a Set of equivalent server subordinates, or an empty Set if no servers were registered
    *         under that ID. If the server delegate was registered as unique, an one-element
    *         Set is returned.
    */
   public Set get(Serializable category)
   {
      Object o;
      Set result;
      synchronized(servers)
      {
         o = servers.get(category);
      }
      if (o == null)
      {
         result = Collections.EMPTY_SET;
      }
      else if (o instanceof Set)
      {
         result = (Set)o;
      }
      else
      {
         result = new HashSet();
         result.add(o);
      }
      return result;
   }


   public String toString()
   {
      return servers.toString();
   }


   /**
    * Helper method that returns a human readable label for a server delegate, to be used in logs.
    */
   public static String subordinateToString(Serializable category, Serializable subordinateID,
                                            Address address)
   {
      StringBuffer sb = new StringBuffer();
      sb.append(category).append(":");
      sb.append(subordinateID).append(":");
      sb.append(address);
      
      return sb.toString();
   }

}
