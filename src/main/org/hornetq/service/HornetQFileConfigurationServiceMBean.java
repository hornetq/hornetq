package org.hornetq.service;

import org.hornetq.core.config.Configuration;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 21, 2009
 */
public interface HornetQFileConfigurationServiceMBean
{
   void create() throws Exception;

   void start() throws Exception;

   void stop() throws Exception;

   void setConfigurationUrl(String configurationUrl);

   Configuration getConfiguration();
}
