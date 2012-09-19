Hadoopizer Help
===============

Overview
--------

Hadoopizer is a generic tool for the parallelisation of bioinformatics analysis in the cloud using the MapReduce paradigm.
The source code is publicly available at http://github.com/genouest/hadoopizer

Installation
------------

Download the latest version of Hadoopizer from the official website (http://github.com/genouest/hadoopizer/downloads).
Copy the hadoopizer.jar file somewhere on the master node of hadoop cluster.
Hadoopizer has the same dependencies as Hadoop, so it is usable directly on a machine where hadoop is correctly installed.

Creating a job
--------------

### Writing an xml config file

The first thing to do when you want to create a hadoopizer job is to create a xml file describing the command you would like to run.
Take a look at the following example:

    <?xml version="1.0" encoding="utf-8"?>
    <job>
        <command>
            mapper -query ${q} -db ${db} -out ${res}
        </command>
        
        <input id="query" split="true">
            <url split="fastq">/local/foo/bar/myfile.fastq</url>
        </input>
    
        <input id="db">
            <url>/local/foo/bar/mydb.fasta</url>
        </input>
        
        <outputs>
            <url>/local/foo/bar/output/</url>
            <output id="res" reducer="sam" />
        </outputs>
    </job>

In this example, we want to launch a mapping command. There are 2 input files: 'query' and 'db'.
The 'query' file will be splitted and is in the fastq format.
The 'db' input will be automatically copied on each compute node.
The result of the command line is in the sam format and will be placed in the /local/foo/bar/output/ directory.

### Launching the job

Once you have written your xml file, launching the job is simple. Just login to your hadoop cluster master node and issue the following command:

    hadoop jar hadoopizer.jar -c your_config_file.xml -w hdfs://your_hdfs_master_node/a_temp_folder

The last option specifies a directory on your HDFS filesystem where hadoopizer will write some temporary data.
It must be a non existing directory, and it is safe to delete it once the job is finished.


Advanced usage
--------------

### Supported protocols for data transfers

Hadoopizer is able to read/write data from/to several kind of filesystems.
The supported protocols are so far:

    local
    HDFS

S3, http or ftp could be implemented soon.

In the configuration file, just write the urls according to the data location:

    <input id="db">
        <url>/local/foo/bar/mydb.fasta</url>
    </input>
    
    <input id="db2">
        <url>hdfs://your_hdfs_master_node/foo/bar/mydb.fasta</url>
    </input>

This works for data output too.

### Multiple input data

In some cases, it is needed to split input data coming from different files.
One frequent use case in bioinformatics is when you want to analyse paired end sequences. In this case, you start from 2 files that need to be read synchronously: each line from the file 1 needs data from corresponding line in file 2.
This is possible in Hadoopizer by specifying multiple url in the input element:

    <input id="query" split="true">
        <url split="fastq">/local/foo/bar/myfile1.fastq</url>
        <url split="fastq">/local/foo/bar/myfile2.fastq</url>
    </input>

### Multiple output data

It is possible to specify several output files for your command line. To do this, simply write one output element for each output file:

    <?xml version="1.0" encoding="utf-8"?>
    <job>
        <command>
            mapper -query ${q} -db ${db} -out ${res}; wc -l ${res} > ${count}
        </command>
        [...]
        <outputs>
            <url>/local/foo/bar/output/</url>
            <output id="res" reducer="sam" />
            <output id="count" reducer="text" />
        </outputs>
    </job>

Both output files will be placed in the output directory (/local/foo/bar/output/).

### Sequence files

If you want to reuse some output data as the input of another job, you can improve performances by writing the output in a Hadoop-specific binary format that offers better i/o performances.
To do this, add the sequence option in your xml file:

    <outputs>
        <url>/local/foo/bar/output/</url>
        <output id="res" reducer="sam" sequence="true" />
        <output id="count" reducer="text" />
    </outputs>

In this example, only the sam file will be written in this binary format.
For better performances, you can also write this file directly to HDFS:

    <outputs>
        <url>hdfs://your_hdfs_master_node/foo/bar/output/</url>
        <output id="res" reducer="sam" sequence="true" />
        <output id="count" reducer="text" />
    </outputs>

### Reusing multiple input data

### Compression

### Hadoop options

It is possible to add some Hadoop options directly within the config file. See the example below.

    <?xml version="1.0" encoding="utf-8"?>
    <job>
        [...]
        <hadoop>
            <config key="mapred.child.java.opts">-Xmx1024m</config>
        </hadoop>
    </job>

This way you can easily adapt your Hadoop cluster settings (size of data chunks, number of reduce tasks, ...) to the kind of analysis you are performing. 

### Input path autocomplete mode

Sometimes you may need to write in a command line a path referring to multiple files with the same prefix, but different extensions.
This situation happens for example with the database parameter in blast command lines:
You can write the following option:

    -db /local/foo/bar/mydb

In this example, /local/foo/bar/mydb refers to several files in /local/foo/bar/: mydb.pal, mydb.ppi, mydb.pin, ...

Using the 'autocomplete' attribute, we tell hadoopizer to consider all the files begining with the 'mydb' prefix:

    <?xml version="1.0" encoding="utf-8"?>
    <job>
        <command>
            blast -query ${q} -db ${db} -out ${res}
        </command>
        [...]
        <input id="db">
            <url autocomplete="true">/local/foo/bar/mydb</url>
        </input>
        [...]
    </job>

### Deploying software

If you want to use a software that is not available on the compute nodes, you can automatically deploy it while launching your Hadoopizer job.
To do so, first prepare an archive containing the binaries  you would like to deploy. You can organize the content as you want.
Then, when launching your Hadoopizer job, add the following option:

    -b /path/to/your/binary/archive.tar.gz

The archive will then be extracted in a directory named 'binaries' in the work directory of each node. To use it, simply adapt your xml file as follow:

    <?xml version="1.0" encoding="utf-8"?>
    <job>
        <command>
            binaries/your_binary -some ${options}
        </command>
    [...]
    </job>
