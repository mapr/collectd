$1 ~ jmx_ins_after      { if (jmx_options_processed == 0) {
                                 print
                                 print jmx_insert
                                 jmx_options_processed=1
                                 next
                             }
                           }
$0 ~ jmx_opts_pattern { if (jmx_rm_processed == 0) {
                                  jmx_rm_found=1
                             }
                           }
/YARN_OPTS/                { if ( jmx_rm_found == 1 && jmx_rm_processed == 0) {
                                app_str=" " yarn_opts  "\""
                                sub("\"$", app_str)
                                jmx_rm_processed=1
                             }
                           }
                           {print }
