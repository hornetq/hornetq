##Code Duplication

Code Duplication can also be seen as a negative impact to maintainability.  If you have one bug in a fragment of code, but if that code fragment is duplicated, the bug has to be removed in all places until it can be called "resolved". 

In order to get an overview of how much duplicated code does HornetQ have, we have used the tool called [DuDe](http://www.inf.usi.ch/phd/wettel/dude.html).  We determine a clone when we have duplicated blocks over 6 lines. 

According to the [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA), HornetQ version 2.2.14 would receive a rating of -, because the code duplication is 12%, so it fits to the 10%-20% category.


![image](https://f.cloud.github.com/assets/2643634/685891/9decc2f8-da46-11e2-845c-0d3f803e60a2.png)
