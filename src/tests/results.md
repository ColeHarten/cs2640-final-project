# Performance Results

## Blocking

hartenc@muxnode:~/cs2640-final-project$ ./build/perf_block --ops 10000 --concurrency 16 --threads 8
blockingmux sequential_write         ops=10000    conc=16   sec=14.764    ops/s=677.3        MiB/s=2.6        avg_us=23578.6    p50=12038.0   p95=60827.7   p99=124878.9  max=7730227.6
blockingmux sequential_read          ops=10000    conc=16   sec=0.056     ops/s=179235.6     MiB/s=700.1      avg_us=88.2       p50=60.8      p95=209.6     p99=309.9     max=720.1    
blockingmux random_read              ops=10000    conc=16   sec=0.054     ops/s=185185.0     MiB/s=723.4      avg_us=85.4       p50=56.9      p95=204.8     p99=302.6     max=588.2    
blockingmux random_write             ops=10000    conc=16   sec=49.887    ops/s=200.5        MiB/s=0.8        avg_us=79756.2    p50=31966.9   p95=186818.1  p99=766773.2  max=19249610.4
blockingmux mixed_80r_20w            ops=10000    conc=16   sec=10.875    ops/s=919.5        MiB/s=3.6        avg_us=17396.6    p50=10437.2   p95=51454.6   p99=148680.7  max=692201.5 
blockingmux fanout_read_multitier    ops=10000    conc=16   sec=0.059     ops/s=170192.8     MiB/s=664.8      avg_us=92.9       p50=75.5      p95=195.9     p99=274.1     max=591.4    
blockingmux foreground_rw_with_bg_promotion ops=10000    conc=16   sec=10.844    ops/s=922.2        MiB/s=3.6        avg_us=17334.8    p50=10351.7   p95=46806.4   p99=169663.4  max=831433.3 

## Non-Blocking

hartenc@muxnode:~/cs2640-final-project$ ./build/perf_async --ops 10000 --concurrency 16 --threads 8
asyncmux    sequential_write             ops=10000    conc=16   sec=9.105     ops/s=1098.3       MiB/s=4.3        avg_us=14546.8    p50=3915.2    p95=14841.3   p99=27057.9   max=9099395.2
asyncmux    sequential_read              ops=10000    conc=16   sec=0.060     ops/s=165657.3     MiB/s=647.1      avg_us=96.1       p50=44.8      p95=65.4      p99=77.3      max=60151.6  
asyncmux    random_read                  ops=10000    conc=16   sec=0.067     ops/s=150091.2     MiB/s=586.3      avg_us=106.1      p50=60.4      p95=67.8      p99=78.4      max=66434.3  
asyncmux    random_write                 ops=10000    conc=16   sec=40.505    ops/s=246.9        MiB/s=1.0        avg_us=64752.6    p50=12569.2   p95=21282.4   p99=76936.7   max=40480404.4
asyncmux    mixed_80r_20w                ops=10000    conc=16   sec=8.330     ops/s=1200.4       MiB/s=4.7        avg_us=13289.9    p50=5611.7    p95=13735.9   p99=26687.9   max=8314775.1
asyncmux    fanout_read_multitier        ops=10000    conc=16   sec=0.044     ops/s=227182.4     MiB/s=887.4      avg_us=70.0       p50=29.3      p95=55.1      p99=73.1      max=43854.7  
asyncmux    foreground_rw_multitier      ops=10000    conc=16   sec=1.961     ops/s=5098.2       MiB/s=19.9       avg_us=3132.7     p50=1133.7    p95=3573.8    p99=4410.1    max=1960247.6