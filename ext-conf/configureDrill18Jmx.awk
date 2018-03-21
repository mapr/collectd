$0 ~ jmx_insert_after      { if (jmx_start_found == 0) {
                                jmx_start_found=1
                                print
                                print "#Enable JMX for MaprMonitoring"
                                print "DRILL_JMX_OPTS=\"-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=" securejmx " -Dcom.sun.management.jmxremote.password.file=" mapr_home "/conf/jmxremote.password -Dcom.sun.management.jmxremote.access.file=" mapr_home "/conf/jmxremote.access -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=" drillport "\""
                                next
                             }
                           }
                           {  
                              if (jmx_start_found == 1 && drill_opts_subst == 0) {
			        if (match ($0,/^export DRILL_JAVA_OPTS=\"\${DRILL_JAVA_OPTS}/)) {
                                  sub(/export DRILL_JAVA_OPTS=\"\${DRILL_JAVA_OPTS}/,"export DRILL_JAVA_OPTS=\"${DRILL_JAVA_OPTS} ${DRILL_JMX_OPTS}")
                                  drill_opts_subst=1
			        }
                             }
                             print
                           }
END                        {
                              if (drill_opts_subst == 0) {
                                exit(1)
                              }
                           }
