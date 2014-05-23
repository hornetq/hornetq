package org.hornetq.tests.util;

import org.hornetq.api.core.client.ServerLocator;

/**
 * A callback interface which allows properties to set on a ServerLocator before it is used.
 * See {@link org.hornetq.tests.integration.cluster.distribution.ClusterTestBase#setupSessionFactory(int, boolean, org.hornetq.tests.util.ServerLocatorSettingsCallback)}
 */
public interface ServerLocatorSettingsCallback
{
   void set(ServerLocator locator);
}
