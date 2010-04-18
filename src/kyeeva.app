{application, kyeeva,
    [{description, "Kyeeva"},
     {vsn, ""},
     {modules, [kyeeva_app, chord_server, sg_server]},
     {registered, [chord_server, sg_server]},
     {applications, [kernel, stdlib]},
     {mod, {kyeeva_app, []}},
     {env, [{max_r, 3},
            {max_t, 60},
            {initnode, nil}]}
    ]}.
