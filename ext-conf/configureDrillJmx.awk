$0 ~ jmx_insert_after         { if (jmx_end_found == 0) {
                                jmx_start_found=1
                                print
                                print "#Enable JMX for MaprMonitoring"
                                print "DRILL_JMX_OPTS=\"-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=6090\""
                                next
                             }
                           }
                           { 
                             if (jmx_start_found == 1 && drill_opts_subst == 0) {
			       if (match ($0,/^export DRILLBIT_JAVA_OPTS=\"\$DRILLBIT_JAVA_OPTS/)) {
                                    sub(/export DRILLBIT_JAVA_OPTS=\"\$DRILLBIT_JAVA_OPTS/,"export DRILLBIT_JAVA_OPTS=\"$DRILLBIT_JAVA_OPTS $DRILL_JMX_OPTS")
                                    drill_opts_subst=1
                                } else {
				  drill_jmx_opts_added=0
				}
                             }
		             if (drill_jmx_opts_added == 0) {
			       print "export DRILLBIT_JAVA_OPTS=\"$DRILLBIT_JAVA_OPTS $DRILL_JMX_OPTS"
                             }		
                             print
                           }
