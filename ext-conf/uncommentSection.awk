BEGIN           { uncomment=0 }
$0 ~ tag":"     { uncomment=1 ; print $0 ; next}
$0 ~ tag"_END:" { uncomment=0 }
                { if (uncomment==1) {
                     sub("^#"," ")
                  }
                  print $0
                }
