BEGIN                               { uncomment=0; found_end=0 }
$0 ~ "^### \\*\\*\\*\\* "tag":"     { uncomment=1 }
$0 ~ "^### \\*\\*\\*\\* "tag"_END:" { found_end=1 }
                                    { if (uncomment==1) {
                                         sub("^#","")
                                      }
                                      print $0
                                      if (found_end==1) {
                                         uncomment=0
                                      }
                                    }
