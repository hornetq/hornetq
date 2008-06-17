package org.jboss.messaging.core.settings;

/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface HierarchicalRepository<T>
{
   /**
    * Add a new match to the repository
    * @param match The regex to use to match against
    * @param value the value to hold agains the match
    */
    void addMatch(String match, T value);

   /**
    * return the value held against the nearest match
    * @param match the match to look for
    * @return the value
    */
   T getMatch(String match);

   /**
    * set the default value to fallback to if none found
    * @param defaultValue the value
    */
   void setDefault(T defaultValue);

   /**
    * remove a match from the repository
    * @param match the match to remove
    */
   void removeMatch(String match);


   /**
    * register a listener to listen for changes in the repository
    * @param listener
    */
   void registerListener(HierarchicalRepositoryChangeListener listener);

   /**
    * unregister a listener
    * @param listener
    */
   void unRegisterListener(HierarchicalRepositoryChangeListener listener);
}
