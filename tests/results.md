# Performance Results

## Blocking

hartenc@muxnode:~/cs2640-final-project$  ./build/perf_block --ops 10000 --concurrency 16 --threads 8
blockingmux sequential_write         ops=10000    conc=16   sec=14.299    ops/s=699.3        MiB/s=2.7        avg_us=22839.3    p50=10923.3   p95=60835.9   p99=189063.6  max=2225744.8
blockingmux sequential_read          ops=10000    conc=16   sec=0.056     ops/s=177091.0     MiB/s=691.8      avg_us=89.2       p50=60.1      p95=192.1     p99=289.5     max=4283.3   
blockingmux random_read              ops=10000    conc=16   sec=0.065     ops/s=154636.7     MiB/s=604.0      avg_us=102.5      p50=70.0      p95=243.2     p99=358.0     max=637.0    
blockingmux random_write             ops=10000    conc=16   sec=50.638    ops/s=197.5        MiB/s=0.8        avg_us=80960.3    p50=36379.4   p95=182639.8  p99=836231.3  max=11174550.8
blockingmux mixed_80r_20w            ops=10000    conc=16   sec=11.038    ops/s=906.0        MiB/s=3.5        avg_us=17650.2    p50=10420.2   p95=44055.7   p99=181919.3  max=819673.7 
blockingmux fanout_read_multitier    ops=10000    conc=16   sec=0.061     ops/s=162929.9     MiB/s=636.4      avg_us=97.0       p50=79.6      p95=204.6     p99=318.0     max=508.2    
blockingmux foreground_rw_with_bg_promotion ops=10000    conc=16   sec=10.675    ops/s=936.7        MiB/s=3.7        avg_us=17061.4    p50=10519.8   p95=46073.2   p99=150925.3  max=764161.5 

## Non-Blocking

hartenc@muxnode:~/cs2640-final-project$ ./build/perf_async --ops 10000 --concurrency 16 --threads 8
asyncmux    sequential_write             ops=10000    conc=16   sec=9.105     ops/s=1098.3       MiB/s=4.3        avg_us=14546.8    p50=3915.2    p95=14841.3   p99=27057.9   max=9099395.2
asyncmux    sequential_read              ops=10000    conc=16   sec=0.060     ops/s=165657.3     MiB/s=647.1      avg_us=96.1       p50=44.8      p95=65.4      p99=77.3      max=60151.6  
asyncmux    random_read                  ops=10000    conc=16   sec=0.067     ops/s=150091.2     MiB/s=586.3      avg_us=106.1      p50=60.4      p95=67.8      p99=78.4      max=66434.3  
asyncmux    random_write                 ops=10000    conc=16   sec=40.505    ops/s=246.9        MiB/s=1.0        avg_us=64752.6    p50=12569.2   p95=21282.4   p99=76936.7   max=40480404.4
asyncmux    mixed_80r_20w                ops=10000    conc=16   sec=8.330     ops/s=1200.4       MiB/s=4.7        avg_us=13289.9    p50=5611.7    p95=13735.9   p99=26687.9   max=8314775.1
asyncmux    fanout_read_multitier        ops=10000    conc=16   sec=0.044     ops/s=227182.4     MiB/s=887.4      avg_us=70.0       p50=29.3      p95=55.1      p99=73.1      max=43854.7  
asyncmux    foreground_rw_multitier      ops=10000    conc=16   sec=1.961     ops/s=5098.2       MiB/s=19.9       avg_us=3132.7     p50=1133.7    p95=3573.8    p99=4410.1    max=1960247.6