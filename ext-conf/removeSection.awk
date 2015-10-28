BEGIN          { remove=0}
$0 ~ tag       { remove=1; print $0; next }
$0 ~ tag"_END" { remove=0}
               { if (remove!=1) {
                    print $0
                 }
               }
