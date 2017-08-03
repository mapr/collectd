BEGIN                                 { commentOut=0; found_end}
$0 ~ "^## \\*\\*\\*\\* "tag":"        { commentOut=1; }
$0 ~ "^## \\*\\*\\*\\* "tag"_END"     { found_end=1}
                                      { if (commentOut==1) {
                                           sub("^","#")
                                        }
                                        print $0
                                        if (found_end==1) {
                                           commentOut=0
                                        }
                                      }
