/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.util;

import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * An RpcServer is installed as "server object" with a RpcDispatcher instance. The RpcServer
 * allows dynamic registration of other server objects.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RpcServer
{
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
    * @param methodName - the name of the method to be invoked by reflection on the registered
    *        serverObjects.
    * @param args - the method's arguments
    * @param signature - the method's signature. The array contains fully qualified class names.
    *
    * @return the result of the invocation.
    * *
    * @exception NoSuchServerException if no server with the specified ID was registered with the
    *            rpcServer.
    * @exception ClassNotFoundException if a fully qualified name specified in the method's
    *            signature cannot be used to load a valid class.
    * @exception NoSuchMethodException if no method with the specified name or with the specified
    *            signature can be identified by reflection on the server object.
    * @exception IllegalAccessException
    * @exception IllegalArgumentException
    * @exception InvocationTargetException if the underlying method throws an exception. 
    */ 
   public Object invoke(Serializable serverID, String methodName, Object[] args, String[] signature)
         throws Exception
   {
      Object serverObject = null;
      synchronized(servers)
      {
         serverObject = servers.get(serverID);
         if (serverObject == null)
         {
            throw new NoSuchServerException();
         }

         Class[] parameterTypes = new Class[signature.length];
         for (int i = 0; i < signature.length; i++)
         {
            parameterTypes[i] = Class.forName(signature[i]);
         }

         Method method = serverObject.getClass().getMethod(methodName, parameterTypes);

         return method.invoke(serverObject, args);

      }
   }

   public void register(Serializable serverID, Object serverObject)
   {
      synchronized(servers)
      {
         servers.put(serverID, serverObject);
      }
   }

}
