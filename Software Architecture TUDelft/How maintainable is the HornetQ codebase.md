# How maintainable is the HornetQ codebase? 

## Intro + Motivation

* maintainability - how easy it is to make changes to the project
* very important quality attribute to study for any system, because all are subject to change 
* HornetQ's focus is on performance, which in practice can lead to maintainabilty degradation 
* inspired by: [Mozilla maintainability analysis](http://almossawi.com/firefox/prose/) (primary, followed the same approach) and [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA)
* discussed this idea with Clebert Suconic, received encouragement and interest


## Maintainability and source code metrics

* [ISO 9126](http://en.wikipedia.org/wiki/ISO/IEC_9126): six quality attributes of software systems, among them is maintainability
* a number of source code metrics have been found to be correlated with this quality attribute
* each metric that we used in the analysis is explained below

### LOC

* number of executable lines, so we exclude comments or empty lines
* a "baseline" measure of software quality
* a bigger project is more complicated to maintain - _less is more_ :-)
* we take into consideration only source files, not tests or other artefacts

### Cyclomatic Complexity

* developed by Thomas McCabe in 1976
* measures the number of linearly independent paths in a software system

### First-Order Density

* measures the number of _direct dependencies_ between files
* calculated by: 
 
  * first build an adjacency matrix (also called _dependency matrix_), using the dependencies from the source code files sorted by their hierarchical directory structure.  
  * this matrix is populated with values in the following way: if a file in a particular row depends on a file on a particular column, we set this element to 1.  
  * the density of such a matrix is the _first-order density_.

### Propagation cost

* measures direct as well as indirect dependecies within a code base
* in practical terms it gives a measure of how many files will be impacted when changing a randomly chosen file [1][2].
* in order to transform the _dependency matrix_ defined above to the _visibility matrix_ (which captures also indirect dependencies) is achieved by matrix multiplication by raising the _dependency matrix_ to successive powers until we reach it's transitive closure
* a matrix raised to the power of two would show the indirect dependencies that have the path length of two, so calls from A to C, where A calls B and B calls C. 
* summing these matrices gives the _visibility matrix_
* the density of the _visibility matrix_ is named as _propagation cost_

### Core Size

* several studies [3][4] have shown that a smaller core size leads to a smaller number of defects
* depending on their indirect fan-in and fan-out values, files can be categoried as: 
  ![coredescription](https://f.cloud.github.com/assets/2643634/662971/7e033a30-d762-11e2-8288-ca7574ad9974.png)
* _Core Size_ is therefore the metric that gives an overview of the number of highly interconnected files : _core files_ 
the fan-in and fan-out values are taken from the _visibility matrix_, hence we have "indirect fan-in and fan-out".
* by inspecting the distribution of indirect fan-in and fan-out of files, we see that the paths are not smooth, but both present a discontinuity point, example for 2.0.0: 

![fan-in hornetq-2 0 0 beta5-src](https://f.cloud.github.com/assets/2643634/673959/ac24a8de-d8c7-11e2-89be-476dc5449e2f.png)

* in the above graphic, we have on Ox axis the files sorted according to their indirect fan-in, and Oy the indirect fan-in values

* this point is used as treshold when determining whether a file is of type _core_ and it amounts to the metric that counts them, _core size_.


##Method 

### Overview

![recipe](https://f.cloud.github.com/assets/2643634/663160/3711f630-d766-11e2-9abd-5e2292af1657.png)

Tooling used to carry out the approach: 

* Matlab + respective scripts
* Python scripts
* [Google Code Pro Analytix](https://developers.google.com/java-dev-tools/codepro/doc/)
* [Java NCSS](http://www.kclee.de/clemens/java/javancss/)

### Subject releases
For this maintainability analysis we have considered the following 8 stable releases' source code available to [download](https://www.jboss.org/hornetq/downloads), as we wanted to see possible trends in evolution.  In the third column, we assign each release with an ID so that values from the plots we will present can be traced back to their respective release identification:

| Release        | Date           |ID           | 
| ------------- |:-------------:| :-------------:|
| 2.0.0.Beta5     | 2009/08/24 | 1 | 
| 2.0.0.CR1      | 2009/12/06     | 2    |
| 2.0.0.CR2 |2009/12/14     | 3    |
| 2.1.0.CR1 |2010/05/28     | 4     | 
| 2.1.2.Final |2010/08/17     | 5     | 
| 2.2.2.Final |2011/03/30     | 6     | 
| 2.2.5.Final |2011/06/17     | 7     | 
| 2.2.14.Final |2012/04/19   | 8   | 

Statistical values for the set of 8 releases:

|         | Mean           |Median           | Stdev           | Min           | Max           | 
| ------------- |:-------------:| :-------------:|:-------------:|:-------------:|:-------------:|
| First Order Density     | 0.0054 | 0.0065 | 0.0032 | 2.75E-04 | 0.0093 | 
| Propagation Cost      | 0.0414     | 0.0472    |0.0311    |0.0014    |0.0991    |
| Core Size |9.25     | 0    |14.06    |0    |35    |
| Lines of Code |87960    | 86706     | 11.883    |71775    |105859    |

### LOC and Cyclomatic Complexity

* LOC was acquired for each project using Google Code Pro Analytix
* used [Java NCSS](http://www.kclee.de/clemens/java/javancss/) to get complexity reports
* Cyclomatic complexity is a power-law, then aggregate operations such as average do not provide useful information as described in [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA)
* therefore created special Matlab script that follows the approach described in [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA) and created resulting pie charts that describe the projects from this perspective

### Dependency metrics

* Google Code Pro Analytix is used to generate [dependency data](https://developers.google.com/java-dev-tools/codepro/doc/features/dependencies/dependencies) for each individual projects
* The generated xml file contains _references_ relationships between types (Java classes)
* This xml file is then processed by a Python script which takes these input files and outputs files where on each row we have :"A->B", where A and B are unique numerical identifiers for each file
    * this script makes use of another script, responsible for assigning each file in the project an unique numerical value
* these input files are then used by MATLAB scripts to: 
  * build a first-order dependency matrix
  * calculate the transitive closure using Floyd algorithm for graphs
  * raise it to multiple powers until its transitive closure is reached resulting in a visibility matrix
  * generate the desired measures - first-order density, propagation cost, core size

* Example of a _first-order dependency matrix_ for version 2.2.14-Final of HornetQ:
  ![first-order dependency matrix hornetq-2 2 14 final-src](https://f.cloud.github.com/assets/2643634/680018/94e5e1d0-d983-11e2-9459-8ac3b39e900c.png)
* And here is the _visibility matrix_ for the same release: 
 ![visibility matrix hornetq-2 2 14 final-src](https://f.cloud.github.com/assets/2643634/680029/d6e93d52-d983-11e2-8ce1-1918fad3d84e.png)
* as we can see, when we consider also indirect dependencies, new patterns of dependencies can be witnessed which make the lines in the matrix thicker

##Findings



###LOC

![loc](https://f.cloud.github.com/assets/2643634/680035/f76bca0e-d983-11e2-9431-12f86f0d6bb1.png)

* LOC grows at a mean rate of 5.76% (a reasonable rate)
* Biggest growth happens between 2.1.2 (5) and 2.2.2 (6)
* At no point do we see substantial drops which would be indicative of refactoring effort.

###Cyclomatic Complexity

* according to the procedure described in [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA), all of the projects under observation would receive a rating of ++, as they meet the requirements dictated by this quality category with regards to cyclomatic complexity. 
* a histogram for release 2.2.14 to show that we have power-law (oX axis - cyclomatic complexity and oY axis - number of files):

![2-2-14-finalhistogram](https://f.cloud.github.com/assets/2643634/680050/1ecd801a-d984-11e2-8cd6-21e068db89af.png)

* a pie chart to show the final result of applying the [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA) with respect to cyclomatic complexity for the same release (2.2.14): 

![2-2-14-final](https://f.cloud.github.com/assets/2643634/680055/300e1d80-d984-11e2-9191-b54918882e79.png)

###First-order Density

![first-order-density](https://f.cloud.github.com/assets/2643634/680059/3f6d6ac4-d984-11e2-9b51-2cb5ac0f1d71.png)

* The trend is fluctuating
* Ideally it should decrease
* In first version (2.0.0 Beta5), if we randomly change one file it implies changes on average in other 4.5 files which are direct dependencies
* Strange drop in the fourth version

###Propagation Cost

![propagation-cost](https://f.cloud.github.com/assets/2643634/680060/4a8f4c9c-d984-11e2-9c7c-601017deb819.png)


* The trend is fluctuating
* Ideally it should decrease
* In last version (2.2.14), if we randomly change one file it implies changes on average in other ~70 files with direct/indirect dependencies
* We can see the strange drop in version 4 again

###Core size

![core-size](https://f.cloud.github.com/assets/2643634/680065/60e40208-d984-11e2-84fe-44f4d90a7d34.png)

* The trend is fluctuating
* Ideally it should decrease
* High uplift between versions 2.0.0 - CR2 (3) and 2.1.0 - CR1 (4)
* High uplift between versions 2.2.2 (6) and 2.2.5 (7)


###Preliminary conclusions


* High uplift in measurements in most recent versions, especially the last - not good news
* Version 4 (2.1.2) needs further investigations because of the first-order density, propagation cost and core size low measurement values
* Future work can include similar approach but on individual modules and comparation with other messaging systems (ActiveMQ)
* The results of these measurements can be used as starting point for focused discussions with the HornetQ team about possible explanations to the data.


##Guide to all the deliverables for this study

* a lot of files/scripts were created, therefore we organised in the following way:
* there are two main folders, [Python](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Python) and [Matlab](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab)
  * each of these folders contains 3 sub-folders: input, output and scripts
  * in each _scripts_ folder we have the scripts that received as input files from the _input_ folder and generated as output files that are in the _output_ folder
* in the [Python](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Python) folder:
  * [input](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Python/input) - the xml files given by Google Code Pro Analytix with dependencies
  * [scripts](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Python/scripts) - the scripts used to translate from dependencies between files to numerical pairs according to their hierarchical position in the project on the file system
  * [output](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Python/output) - the resulting files as numerical pairs
* in the [Matlab](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab) folder:
  * [input](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab/input) contains:
      * the resulting files outputted by the Python scripts as described above (copied them here)
      * the resulting files after running Java NCSS tool on the projects to get Cyclomatic Complexity for all methods per project
  * [scripts](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab/scripts) contains:
      * the "Main" entry point for the Matlab scripts, and various metric related scripts
      * Cyclomatic Complexity script that follows the [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA) procedure
  * [output](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab/output) contains the following folders:
    * [aggregate](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab/output/aggregate) - which contains plots of the metrics over all the projects to observe trends
    * [complexity](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab/output/complexity) - plots for the complexity section, includes:
      * histograms for all releases to see the power law + pie charts for all releases for assigning a rating
    * [per-project](https://github.com/delftswa/ReportingRepo/tree/HornetQ-final/HornetQ/artefacts/Matlab/output/per-project) - for each project, we have 5 plots:
      * Dependency Matrix
      * Visibility Matrix
      * Fan-in
      * Fan-out
      * File Types (Core, or other types)


# References
[1] MacCormack, A., J. Rusnak, and C. Baldwin. "Exploring the Structure of Complex Software Designs: An Empirical Study of Open Source and Proprietary Code." Institute for Operations Research and the Management Sciences (INFORMS) 52, no. 7, 2006.

[2] Eppinger, S. D. and T. R. Browning. Design Structure Matrix Methods and Applications. MIT Press, 2012.

[3] MacCormack, A., C. Baldwin, and J. Rusnak. "The Architecture of Complex Systems: Do Core-periphery Structures Dominate?" MIT Research Paper no. 4770-10, Harvard Business School Finance Working Paper no. 1539115, 2010.

[4] Almossawi, A., "An Empirical Study of Defects and Reopened Defects in GNOME." Unpublished, 2012.




