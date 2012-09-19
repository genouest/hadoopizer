Hadoopizer Help
===============

Overview
--------

Hadoopizer is a generic tool for the parallelisation of bioinformatics analysis in the cloud using the MapReduce paradigm.
The source code is publicly available at http://github.com/genouest/hadoopizer

Installation
------------

Download the latest version of Hadoopizer from the official website (http://github.com/genouest/hadoopizer/downloads).

Creating a job
--------------

### Writing an xml config file



### Launching the job


Using paired end data
----------------------

Deploying software
------------------

If you want to use a software that is not available on the compute nodes, you can automatically deploy it while launching your Hadoopizer job.
To do so, first prepare an archive containing the binaries  you would like to deploy. You can organize the content as you want.
Then, when launching your Hadoopizer job, add the following option:

> -b /path/to/your/binary/archive.tar.gz

The archive will then be extracted in a directory named 'binaries' in the work directory of each node. To use it, simply adapt your xml file as follow:

> &lt;?xml version="1.0" encoding="utf-8"?&gt;
> &lt;job&gt;
>     &lt;command&gt;
>         binaries/your_binary -some ${options}
>     &lt;/command&gt;
> [...]
> &lt;/job&gt;
