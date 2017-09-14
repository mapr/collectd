BEGIN               { addJmxUserInfo=0 }
$0 ~ tag            { addJmxUserInfo=1
                      print $0
                      next 
                    }
$0 ~ "<Connection>" { if (addJmxUserInfo == 1) {
                        print $0
                        print "      user \"mapr\""
                        print "      password \""password"\""
                        next
                      }
                    }
$0 ~ tag"_END"      { addJmxUserInfo=0
                      print $0
                    }
                    { print $0 }
