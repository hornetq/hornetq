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
package org.jboss.jms.server;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Set;
import java.util.Map;

import javax.jms.XAConnectionFactory;
import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.aop.AspectXmlLoader;
import org.jboss.jms.server.connectionfactory.ConnectionFactoryJNDIMapper;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.jms.server.plugin.contract.ThreadPool;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.jms.server.endpoint.ServerConsumerEndpoint;
import org.jboss.jms.tx.JMSRecoverable;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.Util;
import org.jboss.mx.loading.UnifiedClassLoader3;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.remoting.serialization.SerializationStreamFactory;
import org.jboss.system.ServiceCreator;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;
import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeer extends ServiceMBeanSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerPeer.class);

   public static final String RECOVERABLE_CTX_NAME = "jms-recoverables";

   // The "subsystem" label this ServerPeer uses to register its ServerInvocationHandler with the
   // Remoting connector
   public static final String REMOTING_JMS_SUBSYSTEM = "JMS";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String serverPeerID;
   protected ObjectName connectorName;
   protected InvokerLocator locator;
   protected byte[] clientAOPConfig;
   private Version version;

   protected String defaultQueueJNDIContext;
   protected String defaultTopicJNDIContext;

   protected boolean started;

   protected int objectIDSequence = Integer.MIN_VALUE + 1;

   protected long remotingConnectionLeasePeriod;

   // wired components

   protected DestinationJNDIMapper destinationJNDIMapper;
   protected SecurityMetadataStore securityStore;
   protected ConnectionFactoryJNDIMapper connFactoryJNDIMapper;
   protected TransactionRepository txRepository;
   protected ConnectionManager connectionManager;
   protected IdManager messageIdManager;

   // plugins

   protected ObjectName threadPoolObjectName;
   protected ThreadPool threadPoolDelegate;
   protected ObjectName persistenceManagerObjectName;
   protected PersistenceManager persistenceManagerDelegate;
   protected ObjectName messageStoreObjectName;
   protected MessageStore messageStoreDelegate;
   protected ObjectName channelMapperObjectName;
   protected ChannelMapper channelMapper;

   protected JMSServerInvocationHandler handler;

   // We keep a map of consumers to prevent us to recurse through the attached session in order to
   // find the ServerConsumerDelegate so we can acknowledge the message. Originally, this map was
   // maintained per-connection, but with the http://jira.jboss.org/jira/browse/JBMESSAGING-211 bug
   // we shared it among connections, so a transaction submitted on the "wrong" connection can
   // still succeed. For more details, see the JIRA issue.

   private Map consumers;

   // Constructors --------------------------------------------------

   public ServerPeer(String serverPeerID,
                     String defaultQueueJNDIContext,
                     String defaultTopicJNDIContext) throws Exception
   {
      this.serverPeerID = serverPeerID;
      this.defaultQueueJNDIContext = defaultQueueJNDIContext;
      this.defaultTopicJNDIContext = defaultTopicJNDIContext;

      // the default value to use, unless the JMX attribute is modified
      connectorName = new ObjectName("jboss.remoting:service=Connector,transport=socket");

      securityStore = new SecurityMetadataStore();
      txRepository = new TransactionRepository();

      destinationJNDIMapper = new DestinationJNDIMapper(this);
      connFactoryJNDIMapper = new ConnectionFactoryJNDIMapper(this);
      connectionManager = new SimpleConnectionManager();

      consumers = new ConcurrentReaderHashMap();

      version = Version.instance();

      started = false;
   }

   // ServiceMBeanSupport overrides ---------------------------------

   public synchronized void startService() throws Exception
   {
      if (started)
      {
         return;
      }

      log.debug(this + " starting");

      loadClientAOPConfig();

      loadServerAOPConfig();

      MBeanServer mbeanServer = getServer();

      // Acquire references to plugins. Each plug-in will be accessed directly via a reference
      // circumventing the MBeanServer. However, they are installed as services to take advantage
      // of their automatically-creating management interface.

      threadPoolDelegate =
         (ThreadPool)mbeanServer.getAttribute(threadPoolObjectName, "Instance");

      persistenceManagerDelegate =
         (PersistenceManager)mbeanServer.getAttribute(persistenceManagerObjectName, "Instance");

      // TODO: is should be possible to share this with other peers
      messageStoreDelegate =
         (MessageStore)mbeanServer.getAttribute(messageStoreObjectName, "Instance");

      channelMapper = (ChannelMapper)mbeanServer.
         getAttribute(channelMapperObjectName, "Instance");

      // TODO: Shouldn't this go into ChannelMapper's dependencies? Since it's a plug in,
      //       we shouldn't inject their dependencies externally, but they should take care
      ///      of them themselves.
      channelMapper.setPersistenceManager(persistenceManagerDelegate);

      // start the rest of the internal components

      destinationJNDIMapper.start();
      securityStore.start();
      connFactoryJNDIMapper.start();
      txRepository.start(persistenceManagerDelegate);
      txRepository.loadPreparedTransactions();

      //TODO Make block size configurable
      messageIdManager = new IdManager("MESSAGE_ID", 8192, persistenceManagerDelegate);

      initializeRemoting(mbeanServer);

      createRecoverable();

      started = true;

      log.info("JMS " + this + " started");
   }

   public synchronized void stopService() throws Exception
   {
      if (!started)
      {
         return;
      }

      log.debug(this + " stopping");

      started = false;

      removeRecoverable();

      // remove the JMS subsystem invocation handler
      getServer().invoke(connectorName, "removeInvocationHandler",
                         new Object[] { REMOTING_JMS_SUBSYSTEM },
                         new String[] {"java.lang.String"});

      // stop the internal components
      txRepository.stop();
      txRepository = null;
      securityStore.stop();
      securityStore = null;
      connFactoryJNDIMapper.stop();
      connFactoryJNDIMapper = null;
      destinationJNDIMapper.stop();
      destinationJNDIMapper = null;

      unloadServerAOPConfig();

      // TODO unloadClientAOPConfig();

      log.info("JMS " + this + " stopped");
   }

   // JMX Attributes ------------------------------------------------

   public ObjectName getThreadPool()
   {
      return threadPoolObjectName;
   }

   public void setThreadPool(ObjectName on)
   {
      threadPoolObjectName = on;
   }

   public ObjectName getPersistenceManager()
   {
      return persistenceManagerObjectName;
   }

   public void setPersistenceManager(ObjectName on)
   {
      persistenceManagerObjectName = on;
   }

   public ObjectName getMessageStore()
   {
      return messageStoreObjectName;
   }

   public void setMessageStore(ObjectName on)
   {
      messageStoreObjectName = on;
   }

   public ObjectName getChannelMapper()
   {
      return channelMapperObjectName;
   }

   public void setChannelMapper(ObjectName on)
   {
      channelMapperObjectName = on;
   }

   public Object getInstance()
   {
      return this;
   }

   public String getJMSVersion()
   {
      return version.getJMSVersion();
   }

   public int getJMSMajorVersion()
   {
      return version.getJMSMajorVersion();
   }

   public int getJMSMinorVersion()
   {
      return version.getJMSMinorVersion();
   }

   public String getJMSProviderName()
   {
      return version.getJMSProviderName();
   }

   public String getProviderVersion()
   {
      return version.getProviderVersion();
   }

   public int getProviderMajorVersion()
   {
      return version.getProviderMajorVersion();
   }

   public int getProviderMinorVersion()
   {
      return version.getProviderMinorVersion();
   }

   public String getServerPeerID()
   {
      return serverPeerID;
   }

   public String getLocatorURI()
   {
      if (locator == null)
      {
         return null;
      }
      return locator.getLocatorURI();
   }

   public String getDefaultQueueJNDIContext()
   {
      return defaultQueueJNDIContext;
   }

   public String getDefaultTopicJNDIContext()
   {
      return defaultTopicJNDIContext;
   }

   public ObjectName getConnector()
   {
      return connectorName;
   }

   public void setConnector(ObjectName on)
   {
      connectorName = on;
   }

   public void setSecurityDomain(String securityDomain)
   {
      securityStore.setSecurityDomain(securityDomain);
   }

   public String getSecurityDomain()
   {
      return securityStore.getSecurityDomain();
   }

   public void setDefaultSecurityConfig(Element conf) throws Exception
   {
      securityStore.setDefaultSecurityConfig(conf);
   }

   public Element getDefaultSecurityConfig()
   {
      return securityStore.getDefaultSecurityConfig();
   }

   public IdManager getMessageIdManager()
   {
      return messageIdManager;
   }

   public long getRemotingConnectionLeasePeriod()
   {
      return remotingConnectionLeasePeriod;
   }

   public void setRemotingConnectionLeasePeriod(long remotingConnectionLeasePeriod) throws Exception
   {
      long oldValue = this.remotingConnectionLeasePeriod;
      this.remotingConnectionLeasePeriod = remotingConnectionLeasePeriod;
      if (oldValue != remotingConnectionLeasePeriod)
      {
         // update the Connector's value
         MBeanServer mbeanServer = getServer();
         mbeanServer.
            setAttribute(connectorName, new Attribute("LeasePeriod",
                                                      new Long(remotingConnectionLeasePeriod)));

      }
   }

   // JMX Operations ------------------------------------------------

   public String createQueue(String name, String jndiName) throws Exception
   {
      return createDestinationDefault(true, name, jndiName);
   }

   public String createQueue(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      return createDestination(true, name, jndiName, fullSize, pageSize, downCacheSize);
   }

   public boolean destroyQueue(String name) throws Exception
   {
      return destroyDestination(true, name);
   }

   public String createTopic(String name, String jndiName) throws Exception
   {
      return createDestinationDefault(false, name, jndiName);
   }

   public String createTopic(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      return createDestination(false, name, jndiName, fullSize, pageSize, downCacheSize);
   }

   public boolean destroyTopic(String name) throws Exception
   {
      return destroyDestination(false, name);
   }

   public Set getDestinations() throws Exception
   {
      return destinationJNDIMapper.getDestinations();
   }

   // Public --------------------------------------------------------

   public boolean isDeployed(boolean isQueue, String name)
   {
      return destinationJNDIMapper.isDeployed(isQueue, name);
   }

   public ChannelMapper getChannelMapperDelegate()
   {
      return channelMapper;
   }

   public TransactionRepository getTxRepository()
   {
      return txRepository;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public byte[] getClientAOPConfig()
   {
      return clientAOPConfig;
   }

   public Version getVersion()
   {
      return version;
   }

   // access to hard-wired server extensions

   public SecurityManager getSecurityManager()
   {
      return securityStore;
   }

   public DestinationManager getDestinationManager()
   {
      return destinationJNDIMapper;
   }

   public ConnectionFactoryManager getConnectionFactoryManager()
   {
      return connFactoryJNDIMapper;
   }

   public ConnectionManager getConnectionManager()
   {
      return connectionManager;
   }

   // access to plugin references

   public ThreadPool getThreadPoolDelegate()
   {
      return threadPoolDelegate;
   }

   public PersistenceManager getPersistenceManagerDelegate()
   {
      return persistenceManagerDelegate;
   }

   public MessageStore getMessageStoreDelegate()
   {
      return messageStoreDelegate;
   }

   public synchronized int getNextObjectID()
   {
      return objectIDSequence++;
   }

   public ServerConsumerEndpoint putConsumerEndpoint(int consumerID, ServerConsumerEndpoint c)
   {
      log.debug(this + " caching consumer " + consumerID);
      return (ServerConsumerEndpoint)consumers.put(new Integer(consumerID), c);
   }

   public ServerConsumerEndpoint getConsumerEndpoint(int consumerID)
   {
      return (ServerConsumerEndpoint)consumers.get(new Integer(consumerID));
   }

   public ServerConsumerEndpoint removeConsumerEndpoint(Integer consumerID)
   {
      log.debug(this + " removing consumer " + consumerID + " from the cache");
      return (ServerConsumerEndpoint)consumers.remove(consumerID);
   }

   public String toString()
   {
      return "ServerPeer [" + getServerPeerID() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Place a Recoverable instance in the JNDI tree. This can be used by a transaction manager in
    * order to obtain an XAResource so it can perform XA recovery.
    */
   private void createRecoverable() throws Exception
   {
      InitialContext ic = new InitialContext();

      int connFactoryID = connFactoryJNDIMapper.registerConnectionFactory(null, null);

      XAConnectionFactory xaConnFactory =
         (XAConnectionFactory)connFactoryJNDIMapper.getConnectionFactory(connFactoryID);

      JMSRecoverable recoverable = new JMSRecoverable(serverPeerID, xaConnFactory);

      Context recCtx = null;
      try
      {
         recCtx = (Context)ic.lookup(RECOVERABLE_CTX_NAME);
      }
      catch (NamingException e)
      {
         //Ignore
      }

      if (recCtx == null)
      {
         recCtx = ic.createSubcontext(RECOVERABLE_CTX_NAME);
      }

      recCtx.rebind(this.serverPeerID, recoverable);
   }

   private void removeRecoverable() throws Exception
   {
      InitialContext ic = new InitialContext();

      Context recCtx = null;
      try
      {
         recCtx = (Context)ic.lookup(RECOVERABLE_CTX_NAME);
         recCtx.unbind(serverPeerID);
      }
      catch (NamingException e)
      {
         //Ignore
      }
   }

   private void initializeRemoting(MBeanServer mbeanServer) throws Exception
   {
      //We explicitly associate the datatype "jms" with the java SerializationManager
      //This is vital for performance reasons.
      SerializationStreamFactory.setManagerClassName(
         "jms", "org.jboss.remoting.serialization.impl.jboss.JBossSerializationManager");

      JMSWireFormat wf = new JMSWireFormat();

      MarshalFactory.addMarshaller("jms", wf, wf);
      

      // first remove the invocation handler specified in the config

      // TODO: Why removing this? Isn't ServerPeer.stopService() supposed to remove it?
      //       If it doesn't, fix ServerPeer.stopService(), don't add unnecessary behavior here.

      mbeanServer.invoke(connectorName, "removeInvocationHandler",
                         new Object[] { REMOTING_JMS_SUBSYSTEM },
                         new String[] { "java.lang.String" });

      // add the JMS subsystem invocation handler

      handler = new JMSServerInvocationHandler();

      mbeanServer.invoke(connectorName, "addInvocationHandler",
                         new Object[] { REMOTING_JMS_SUBSYSTEM, handler},
                         new String[] { "java.lang.String",
                                        "org.jboss.remoting.ServerInvocationHandler"});

      
      String s = (String)mbeanServer.getAttribute(connectorName, "InvokerLocator");
            
      //To turn off remoting leasing need to ensure that the connection listener hasn't been set
      //OR set the lease period to zero
      //Client side pinging is DISABLED by default so even if set connection listener
      //on the server side the client won't ping unless add InvokerLocator.CLIENT_LEASE is set      
      if (remotingConnectionLeasePeriod != -1)
      {
         mbeanServer.
            setAttribute(connectorName, new Attribute("LeasePeriod",
                  new Long(remotingConnectionLeasePeriod)));
         
         //install the connection listener that listens for failed connections
         
         mbeanServer.invoke(connectorName, "addConnectionListener",
               new Object[] {connectionManager},
               new String[] {"org.jboss.remoting.ConnectionListener"});
         
         //Enable client side pinging
         s += "&" + InvokerLocator.CLIENT_LEASE + "=true";
      }
      else
      {
         log.info("LeasePeriod == -1, not activating connection checking");
      }            
      
      locator = new InvokerLocator(s);

      //FIXME Note - There seems to be a bug (feature?) in JBoss Remoting which if you specify
      //a socket timeout on the invoker locator URI then it always makes a remote call
      //even when in the same JVM

      log.debug("LocatorURI: " + getLocatorURI());
   }

   private void loadServerAOPConfig() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("aop-messaging-server.xml");
      AspectXmlLoader.deployXML(url);
   }

   private void unloadServerAOPConfig() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("aop-messaging-server.xml");
      AspectXmlLoader.undeployXML(url);
   }

   private void loadClientAOPConfig() throws Exception
   {
      // Note the file is called aop-messaging-client.xml NOT messaging-client-aop.xml. This is
      // because the JBoss will automatically deploy any files ending with aop.xml; we do not want
      // this to happen for the client config

      URL url = this.getClass().getClassLoader().getResource("aop-messaging-client.xml");
      InputStream is = null;
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try
      {
         is = url.openStream();
         int b;
         while ((b = is.read()) != -1)
         {
            os.write(b);
         }
         os.flush();
         clientAOPConfig = os.toByteArray();
      }
      finally
      {
         if (is != null)
         {
            is.close();
         }
         if (os != null)
         {
            os.close();
         }
      }
   }

   private String createDestinationDefault(boolean isQueue, String name, String jndiName)
      throws Exception
   {
      //
      // TODO - THIS IS A TEMPORARY IMPLEMENTATION; WILL BE REPLACED WITH INTEGRATION-CONSISTENT ONE
      // TODO - if I find a way not using UnifiedClassLoader3 directly, then get rid of
      //        <path refid="jboss.jmx.classpath"/> from jms/build.xml dependentmodule.classpath
      //

      //FIXME - Yes this is super-ugly - there must be an easier way of doing it
      //also in LocalTestServer is doing the same thing in a slightly different way
      //this should be combined

      String destType = isQueue ? "Queue" : "Topic";
      String className = "org.jboss.jms.server.destination." + destType;
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      String destinationMBeanConfig =
         "<mbean code=\"" + className + "\" " +
         "       name=\"" + ons + "\" " +
         "       xmbean-dd=\"xmdesc/" + destType + "-xmbean.xml\">\n" +
         "    <constructor>" +
         "        <arg type=\"boolean\" value=\"true\"/>" +
         "    </constructor>" +
         "</mbean>";

      return createDestinationInternal(destinationMBeanConfig, on, jndiName, false, -1, -1, -1);
   }

   private String createDestinationInternal(String destinationMBeanConfig, ObjectName on,
                                            String jndiName, boolean params, int fullSize,
                                            int pageSize, int downCacheSize) throws Exception
   {
      MBeanServer mbeanServer = getServer();

      Element element = Util.stringToElement(destinationMBeanConfig);

      ServiceCreator sc = new ServiceCreator(mbeanServer);

      ClassLoader cl = this.getClass().getClassLoader();
      ObjectName loaderObjectName = null;
      if (cl instanceof UnifiedClassLoader3)
      {
         loaderObjectName = ((UnifiedClassLoader3)cl).getObjectName();
      }

      sc.install(on, loaderObjectName, element);

      // inject dependencies
      mbeanServer.setAttribute(on, new Attribute("ServerPeer", getServiceName()));
      mbeanServer.setAttribute(on, new Attribute("JNDIName", jndiName));
      if (params)
      {
         mbeanServer.setAttribute(on, new Attribute("FullSize", new Integer(fullSize)));
         mbeanServer.setAttribute(on, new Attribute("PageSize", new Integer(pageSize)));
         mbeanServer.setAttribute(on, new Attribute("DownCacheSize", new Integer(downCacheSize)));
      }
      mbeanServer.invoke(on, "create", new Object[0], new String[0]);
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);

      return (String)mbeanServer.getAttribute(on, "JNDIName");

      //
      // end of TODO
      //
   }

   private String createDestination(boolean isQueue, String name, String jndiName, int fullSize,
                                    int pageSize, int downCacheSize) throws Exception
   {
      //
      // TODO - THIS IS A TEMPORARY IMPLEMENTATION; WILL BE REPLACED WITH INTEGRATION-CONSISTENT ONE
      // TODO - if I find a way not using UnifiedClassLoader3 directly, then get rid of
      //        <path refid="jboss.jmx.classpath"/> from jms/build.xml dependentmodule.classpath
      //

      String destType = isQueue ? "Queue" : "Topic";
      String className = "org.jboss.jms.server.destination." + destType;
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      String destinationMBeanConfig =
         "<mbean code=\"" + className + "\" " +
         "       name=\"" + ons + "\" " +
         "       xmbean-dd=\"xmdesc/" + destType + "-xmbean.xml\">\n" +
         "    <constructor>" +
         "        <arg type=\"boolean\" value=\"true\"/>" +
         "    </constructor>" +
         "    <attribute name=\"FullSize\">" + fullSize + "</attribute>" +
         "    <attribute name=\"PageSize\">" + pageSize + "</attribute>" +
         "    <attribute name=\"DownCacheSize\">" + downCacheSize + "</attribute>" +
         "</mbean>";

      return createDestinationInternal(destinationMBeanConfig, on, jndiName, true, fullSize,
                                       pageSize, downCacheSize);
   }

   private boolean destroyDestination(boolean isQueue, String name) throws Exception
   {
      String destType = isQueue ? "Queue" : "Topic";
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      MBeanServer mbeanServer = getServer();

      // we can only destroy destinations that have been created programatically
      Boolean b = (Boolean)mbeanServer.getAttribute(on, "CreatedProgrammatically");
      if (!b.booleanValue())
      {
         log.warn("Cannot destroy a destination that has not been created programatically");
         return false;
      }
      mbeanServer.invoke(on, "stop", new Object[0], new String[0]);
      mbeanServer.invoke(on, "destroy", new Object[0], new String[0]);
      mbeanServer.unregisterMBean(on);
      return true;
   }

   // Inner classes -------------------------------------------------

}
