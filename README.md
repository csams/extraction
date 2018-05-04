# Archive Data Aggregator

This project provides a few primitives for extracting files from archives using
insights-core and aggregating them together with similar data from previously
processed archives.

Requires insights-core 3 and python 3, preferrably 3.6+

~~~
from extraction import *

ext = ExtractionContext(accountnumber="1234",
                        case_number="9876",
                        createddate="2018-05-04 00:00:00",
                        attachment_uuid="12345")

acc = Accumulator("/tmp/my_data", small_max=20*MB, large_max=2*GB)
acc.process(ext, "/path/to/sosreport.tar.gz")
~~~

If files already exist in the directory, the Accumulator picks up where the
previous one left off.

To combine multiple accumulators, do the following:
~~~
total = Accumulator("/tmp/aggregated_data", small_max=500*MB, large_max=8*GB)

for acc in [acc1, acc2, acc3, acc4]:
    total += acc
~~~
