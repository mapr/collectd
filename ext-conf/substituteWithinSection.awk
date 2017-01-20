BEGIN                               { inSection=0 }
$0 ~ "## \\*\\*\\*\\* "tag":"       { inSection=1 }
$0 ~ "## \\*\\*\\*\\* "tag"_END:"   { inSection=0 }
$0 ~ findPattern                    { if (inSection==1) {
                                         sub(findPattern,replacePattern)
                                      }
                                    }
                                    { print $0 }
