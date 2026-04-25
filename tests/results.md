# Performance Results

hartenc@muxnode:~/cs2640-final-project$  ./build/perf_block --ops 10000 --concurrency 16 --threads 8
blockingmux sequential_write         ops=10000    conc=16   sec=14.299    ops/s=699.3        MiB/s=2.7        avg_us=22839.3    p50=10923.3   p95=60835.9   p99=189063.6  max=2225744.8
blockingmux sequential_read          ops=10000    conc=16   sec=0.056     ops/s=177091.0     MiB/s=691.8      avg_us=89.2       p50=60.1      p95=192.1     p99=289.5     max=4283.3   
blockingmux random_read              ops=10000    conc=16   sec=0.065     ops/s=154636.7     MiB/s=604.0      avg_us=102.5      p50=70.0      p95=243.2     p99=358.0     max=637.0    
blockingmux random_write             ops=10000    conc=16   sec=50.638    ops/s=197.5        MiB/s=0.8        avg_us=80960.3    p50=36379.4   p95=182639.8  p99=836231.3  max=11174550.8
blockingmux mixed_80r_20w            ops=10000    conc=16   sec=11.038    ops/s=906.0        MiB/s=3.5        avg_us=17650.2    p50=10420.2   p95=44055.7   p99=181919.3  max=819673.7 
blockingmux fanout_read_multitier    ops=10000    conc=16   sec=0.061     ops/s=162929.9     MiB/s=636.4      avg_us=97.0       p50=79.6      p95=204.6     p99=318.0     max=508.2    
blockingmux foreground_rw_with_bg_promotion ops=10000    conc=16   sec=10.675    ops/s=936.7        MiB/s=3.7        avg_us=17061.4    p50=10519.8   p95=46073.2   p99=150925.3  max=764161.5 