package org.hornetq.jms.server.embedded;

import org.hornetq.core.registry.MapBindingRegistry;
import org.hornetq.core.server.embedded.EmbeddedHornetQ;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.spi.BindingRegistry;

/**
 * Simple bootstrap class that parses hornetq config files (server and jms and security) and starts
 * a HornetQServer instance and populates it with configured JMS endpoints.
 * <p/>
 * JMS Endpoints are registered with a simple MapBindingRegistry.  If you want to use a different registry
 * you must set the registry property of this clas
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class EmbeddedJMS extends EmbeddedHornetQ
{
   protected JMSServerManagerImpl serverManager;
   protected BindingRegistry registry;
   protected String jmsConfigResourcePath;

   /**
    * Classpath resource where JMS config file is.  Defaults to 'hornetq-jms.xml'
    *
    * @param jmsConfigResourcePath
    */
   public void setJmsConfigResourcePath(String jmsConfigResourcePath)
   {
      this.jmsConfigResourcePath = jmsConfigResourcePath;
   }

   public BindingRegistry getRegistry()
   {
      return registry;
   }

   public void setRegistry(BindingRegistry registry)
   {
      this.registry = registry;
   }

   /**
    * Lookup in the registry for registered object, i.e. a ConnectionFactory.  This is a convenience method.
    *
    * @param name
    * @return
    */
   public Object lookup(String name)
   {
      return serverManager.getRegistry().lookup(name);
   }

   public void start() throws Exception
   {
      super.initStart();
      if (jmsConfigResourcePath == null) serverManager = new JMSServerManagerImpl(hornetQServer);
      else serverManager = new JMSServerManagerImpl(hornetQServer, jmsConfigResourcePath);
      if (registry == null)
      {
         registry = new MapBindingRegistry();
      }
      serverManager.setRegistry(registry);
      serverManager.start();
   }

   public void stop() throws Exception
   {
      serverManager.stop();
   }

}
