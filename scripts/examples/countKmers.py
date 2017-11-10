
# coding: utf-8

# # Reading and Counting Kmers with ADAM
# 
# This tutorial introduces users to using ADAM in Jupyter notebook.

# In[ ]:

# import ADAMContext
from bdgenomics.adam.adamContext import ADAMContext


# In[ ]:

# Create ADAM Context
ac = ADAMContext(sc)


# In[ ]:

# Set file name. SCRIPT_DIR is set in adam-notebook
alignmentFile = os.environ['SCRIPT_DIR'] + "/adam-core/src/test/resources/NA12878.sam"


# In[ ]:

reads = ac.loadAlignments(alignmentFile)


# In[ ]:

# sort reads by position
sorted = reads.sortReadsByReferencePosition()


# Count the number of reads in our file:

# In[ ]:

# count the number of reads
count = sorted.toDF().rdd.count()
print(count)


# Count the number of distinct kmers of length 6 in the alignments:

# In[ ]:

# count kmers of length 6
kmers = sorted.countKmers(6)


# In[ ]:

# show kmer results
kmers.show()


# In[ ]:



