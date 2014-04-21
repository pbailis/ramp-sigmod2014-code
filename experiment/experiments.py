
defaultsettings = { "serversList" : [(5,5)],
                    "txnlen" : [4],
                    "threads" : [1000],
                    "numseconds" : 60,
                    "configs" : [ "READ_COMMITTED",
                                  "READ_ATOMIC_STAMP", 
                                  "EIGER",
                                  "READ_ATOMIC_LIST",
                                  "READ_ATOMIC_BLOOM",
                                  "LWLR",
                                  "LWSR",
                                  "LWNR" ],
                    "readprop" : [0.95],
                    "iterations" : range(0,3),
                    "numkeys" : [1000000],
                    "valuesize" : [1],
                    "keydistribution" : "zipfian",
                    "bootstrap_time_ms" : 10000,
                    "launch_in_bg" : False,
                    "drop_commit_pcts" : [0],
                    "check_commit_delays" : [-1],
                 }

def chg_param(d, param, value):
    d[param] = value
    return d

experiments = { "debug" :
                chg_param(chg_param(chg_param(chg_param(chg_param(defaultsettings.copy(),
                                                                  "valuesize", [1000]), "configs", ["LWNR"]),
                                              "iterations", [1]), "keydistribution", "zipfian"),
                          "readprop", [.95]),
                
                "maxthru" : chg_param(chg_param(chg_param(chg_param(chg_param(defaultsettings.copy(),
                                                                              "threads",
                                                                              [2000]),
                                                                    "configs",
                                                                    ["READ_ATOMIC_LIST"]),
                                                          "iterations",
                                                          [1]),
                                                "keydistribution",
                                                "zipfian"),
                                      "readprop",
                                      [.95]),
                
                
                "threads" : chg_param(defaultsettings.copy(),
                                      "threads",
                                      [2250, 2500, 2750, 3000]),#[1500, 1, 125, 250, 500, 750, 1000, 1250]),
                
                "commit_pct" : chg_param(chg_param(chg_param(chg_param(chg_param(chg_param(chg_param(defaultsettings.copy(),
                                                                                                     "drop_commit_pcts",
                                                                                                     [0, .01, .25, .5, .75, 1]),#.5, .1, 1]),
                                                                                           "check_commit_delays",
                                                                                           [1000]),
                                                                                 "configs",
                                                                                 ["READ_ATOMIC_LIST"]),
                                                                       "serversList",
                                                                       [(5,5)]),
                                                             "threads",
                                                             [1000],
                                                             ),
                                                   "readprop",
                                                   [0, .25, 0.5, .75, .95]),#[0, .95]),
                                         "keydistribution",
                                         "zipfian"),
                
                "scaleout" : chg_param(chg_param(chg_param(chg_param(chg_param(chg_param(defaultsettings.copy(),
                                                                                         "serversList",
                                                                                         [(10, 10)]),
                                                                               "configs",
                                                                               ["READ_COMMITTED",
                                                                                "READ_ATOMIC_LIST",
                                                                                "READ_ATOMIC_STAMP",
                                                                                "READ_ATOMIC_BLOOM"]),
                                                                     "keydistribution",
                                                                     "uniform"),
                                                           "threads",
                                                           [2000]),
                                                 "bootstrap_time_ms",
                                                 30000),
                                       "launch_in_bg",
                                       True),
                
                "rprop" :  chg_param(defaultsettings.copy(),
                                     "readprop",
                                     [1, .75, .5, .25, 0]),
                
                "numkeys" :  chg_param(defaultsettings.copy(),
                                       "numkeys",
                                       [10, 100, 1000, 10000, 100000, 10000000]),
                
                "txnlen" :  chg_param(defaultsettings.copy(),
                                      "txnlen",
                                      [2, 8, 16, 32, 64, 128]),
                
                "valuesize" :  chg_param(defaultsettings.copy(),
                                         "valuesize",
                                         [1, 100, 1000, 10000, 100000]),
                
                "valuesize-cleanup" :  chg_param(chg_param(chg_param(defaultsettings.copy(),
                                                                     "valuesize",
                                                                     [100000]),
                                                           "configs",
                                                           ["READ_ATOMIC_STAMP",
                                                            "LWLR",
                                                            "LWNR"]),
                                                 "iterations",
                                                 range(0,2)),
                
                "valuesize-cleanup-2" :  chg_param(chg_param(chg_param(defaultsettings.copy(),
                                                                       "valuesize",
                                                                       [100000]),
                                                             "configs", [ "READ_COMMITTED",
                                                                          
                                                                          "EIGER",
                                                                          "LWSR" ]),
                                                   "iterations",
                                                   range(0,1)),
                
                
                
                "stress" : {
                    "serversList" : [(5,5)],
                    "txnlen" : [4],
                    "threads" : [50],
                    "numseconds" : 90,
                    "configs" : [ "READ_COMMITTED" ],
                    "readprop" : [0.5],
                    "iterations" : range(0,1),
                    "valuesize" : [1],
                    "numkeys": [1000],
                }
}
        
