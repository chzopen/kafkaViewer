# -*- coding: gbk -*-  

import os
import platform;
import subprocess
import signal
import sys


_mainClass = "per.chzopen.kafkaViewer.AppGUI";


def isWindows():
    return platform.system().lower().find("windows")>-1


def makeCmd():
    
    _sep = ":";
    
    if isWindows()>-1:
        _sep = ";"
    
    _cp = "target/classes" + _sep;
    
    for _pa_dir, _p2, filenames in os.walk("target/dependency"):
        for _filename in filenames:
            if _filename.lower().endswith(".jar"):
                _cp += os.path.join(_pa_dir, _filename) + _sep;
    
    _cmd = 'java -cp "%s" %s' % (_cp, _mainClass)
    return _cmd

        
def doStartup():
    _process = subprocess.Popen(makeCmd())
    

if __name__=="__main__":
    
    doStartup();
    
    
        
    
    

















