/<Plugin / && $2 ~ plugin { inPlugin=1; }
/Host/                { if (inPlugin==1) { 
                           print "               Host \"" hostname "\""
                           # next is needed so the print below doesn't print
                           # the old Host information
                           next}
                      } 
/Port/                { if (inPlugin==1) { 
                           print "               Port \"" port "\""
                           inPlugin=0; 
                           # next is needed so the print below doesn't print
                           # the old Port information
                           next}
                      }
                      { print }
