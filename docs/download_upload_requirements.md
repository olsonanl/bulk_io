# Requirements for set downloads and uploads

A user should be able to search for, upload, and download batch sets of data. It isnÕt clear if this needs to be restricted to the GUI, or that if a command line interface is acceptable. In either case, there will be a front end and back end component.  For our purposes here, we will be describing the back end components and the key interface(s) with the front end. 

* We need to understand and document the interface between the front end and backend pieces in the architecture, and demonstrate the backend components through operational front end pieces. *

If a user does want to download data, then presumably that user has some means to locally operate on that data, at which point we loose visibility into what is being done with the data.

* We should not attempt to track what a user does with data once it is downloaded. We should however, know that the data was downloaded. *

In order for a user to be able to download a set of data, the user needs a means by which to identify the data to be downloaded. This brings into question the linkage between search and batch downloads (and uploads).

* The batch download should have a uses relationship to search; however search should not have any relationship with download. *


References from JIRA

1. I would like to know if there is a way to download a table matching gene
names (e.g. kb|g.3083.peg.1563) to their genome coordinates and gene
aliases.  KBASE-2922
2. Can you tell me how to download this functional categories from kbase? If it is not available by direct downloading, can I get some mapping files to map the gene annotation of each gene to corresponding class or categories?  KBASE-3105
3. Is there somewhere I can download a batch of metabolic models? KBASE-3116
4. I would like to be able to download the gene table that has the annotations. Also, it looks that the annotations are limited to just one level of the SEED subsystems. It is often very useful to have the full hierarchy for each gene."  KBASE-1438






