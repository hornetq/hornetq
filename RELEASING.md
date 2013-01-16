# A check list of things to be done before a release. #

Things to do before issuing a new MAJOR release:

* check if all new Configuration parameters are documented;

* use your IDE to regerate equals/hashCode for ConfigurationImpl (this
  is just much safer than trying to inspect the code).

* check if all public API classes have a proper Javadoc.


## Tagging a relese in Git ##

We must avoid having multiple commits with the same final release version in the POMs. To achieve that, the commit changing the pom versions to the final release version, should be merged together with a second commit changing to version in all pom's to ``X.Y.Z-SNAPSHOT``.

Assuming current version is ``X.Y.Z-SNAPSHOT``

0. Update the release notes.
1. Prepare a single commit changing all version tags in all pom's.
3. Either use ``git revert`` to create a new commit reverting the commit with the version changes. Or change again all versions to ``R.S.T-SNAPSHOT``.
4. push both commits with version changes together, including them in the same _pull-request_.
