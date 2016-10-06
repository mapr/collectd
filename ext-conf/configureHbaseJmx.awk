$0 ~ jmx_uncomment_start   { if (jmx_end_found == 0) {
                                 jmx_start_found=1
                                 print "#Enable JMX for MaprMonitoring"
                             }
                           }
$0 ~ jmx_uncomment_end     { if (jmx_start_found == 1) {
                                  jmx_end_found=1
                             }
                           }
                           { if ( jmx_start_found == 1 ) {
                                sub("^#", "")
                                if (jmx_end_found == 1) {
                                     jmx_start_found=0
                                     jmx_end_found=0
                                     jmx_enabled=1
                                }
                             }
                             print
                           }
END                        { if (jmx_enabled == 0 ) {
                               exit(1)
                             }
                           }
