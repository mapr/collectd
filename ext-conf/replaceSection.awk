BEGIN           { replace=0 }
$0 ~ tag":"     { replace=1 ; print $0 ; next}
$0 ~ tag"_END:" { replace=0 }
                { if (replace==1) {
                     while ( getline l < newSectionContentFile > 0 )
                        print l
                  }
                  print $0
                }
