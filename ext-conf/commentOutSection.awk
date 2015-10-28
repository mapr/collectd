BEGIN           { commentOut=0}
$0 ~ tag        { commentOut=1; print $0 ; next}
$0 ~ tag"_END"  { commentOut=0}
                { if (commentOut==1) {
                     sub("^","#")
                  }
                  print $0
                }
