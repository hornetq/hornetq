/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.util;

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
 * The RpcServer allows dynamic registration of other server objects, under "categories". For each
 * category, the RpcServer allows registration of one or more equivalent sub-server objects, on
 * which methods will be serially invoked when a remote method call is received from the group.
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
    * @param category - the category of equivalent-subservers to invoke the method on.
    * @param methodName - the name of the method to be invoked by reflection on the registered
    *        serverObjects.
    * @param args - the method's arguments.
    * @param signature - the method's signature. The array contains fully qualified class names.
    *
    * @return a Collection containing SubServerResponse instances. It always returns a Collection,
    *         an empty collection in the case that no servers were found, never null.
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
         Set equivalents = (Set)servers.get(category);
         if (equivalents == null)
         {
            return Collections.EMPTY_SET;
         }
         Collection results = new ArrayList();
         for(Iterator i = equivalents.iterator(); i.hasNext(); )
         {
            SubServer subServer = (SubServer)i.next();
            Serializable id = subServer.getID();
            try
            {
               Method method = subServer.getClass().getMethod(methodName, parameterTypes);
               Object result = method.invoke(subServer, args);
               results.add(new SubServerResponse(id, result));
            }
            catch(Throwable t)
            {
               log.debug("server object invocation failed", t);
               results.add(new SubServerResponse(id, t));
            }
         }
         return results;
      }
   }

   /**
    * Registers a sub-server object with the RpcServer, under the specified category.
    *
    * <p> Note that more than one "equivalent" server objects can be registered under the same
    * category. However, no equal (as per the "equal()" method semantics) are allowed to be
    * registered under the same category.
    *
    * @param category - the server category.
    * @param subServerObject - the sub-server object to be registered.
    * @return true if the server was sucessfully registered or false if an "equal" object was
    *         already registered under the specified server ID.
    *
    * @exception NullPointerException if trying to register a null instance.
    *
    */
   public boolean register(Serializable category, SubServer subServerObject)
   {
      if (subServerObject == null)
      {
         throw new NullPointerException("null sub-server object");
      }

      boolean result = false;

      synchronized(servers)
      {
         Set equivalents = (Set)servers.get(category);
         if (equivalents == null)
         {
            equivalents = new HashSet();
            servers.put(category, equivalents);
         }

         result = equivalents.add(subServerObject);
         log.debug("register " + category + " -> " + subServerObject.getID() + "[" +
                   subServerObject.getClass().getName() + "]: " + result);
         return result;
      }
   }

   /**
    * @param category - the server category.
    * @return a Set of equivalent SubServers, or an empty Set if no servers were registered under
    *         that ID.
    */
   public Set get(Serializable category)
   {
      synchronized(servers)
      {
         Set equivalents = (Set)servers.get(category);
         if (equivalents == null)
         {
            return Collections.EMPTY_SET;
         }
         return equivalents;
      }
   }

   public String toString()
   {
      return servers.toString();
   }


   /**
    * Helper method that returns a human readable label for a sub-server, to be used in logs.
    */
   public static String subServerToString(Address address, Serializable category,
                                          Serializable subServerID)
   {
      StringBuffer sb = new StringBuffer();
      sb.append(address);
      sb.append('.');
      sb.append(category);
      if (subServerID != null)
      {
         sb.append('.');
         sb.append(subServerID);
      }
      return sb.toString();
   }

}
