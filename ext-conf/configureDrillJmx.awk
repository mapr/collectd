$0 ~ jmx_uncomment         { if (jmx_end_found == 0) {
                                jmx_start_found=1
                                print "#Enable JMX for MaprMonitoring"
                                sub("^#", "")
                             }
                           }
                           { 
                             print
                           }
