#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from bdgenomics.adam.adamContext import ADAMContext
from bdgenomics.adam.test import SparkTestCase


class ADAMContextTest(SparkTestCase):


    def test_load_alignments(self):
        
        testFile = self.resourceFile("small.sam")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadAlignments(testFile)

        self.assertEqual(reads.toDataFrame().count(), 20)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 20)


    def test_load_gtf(self):

        testFile = self.resourceFile("Homo_sapiens.GRCh37.75.trun20.gtf")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDataFrame().count(), 15)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 15)


    def test_load_bed(self):

        testFile = self.resourceFile("gencode.v7.annotation.trunc10.bed")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDataFrame().count(), 10)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 10)


    def test_load_narrowPeak(self):

        testFile = self.resourceFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDataFrame().count(), 10)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 10)


    def test_load_interval_list(self):


        testFile = self.resourceFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDataFrame().count(), 369)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 369)


    def test_load_genotypes(self):

        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadGenotypes(testFile)

        self.assertEqual(reads.toDataFrame().count(), 18)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 18)
        

    def test_load_variants(self):

        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadVariants(testFile)

        self.assertEqual(reads.toDataFrame().count(), 6)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 6)


    def test_load_sequence(self):


        testFile = self.resourceFile("HLA_DQB1_05_01_01_02.fa")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadSequence(testFile)

        self.assertEqual(reads.toDataFrame().count(), 1)
        self.assertEqual(reads._jvmRdd.jrdd().count(), 1)
