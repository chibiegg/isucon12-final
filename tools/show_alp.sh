sudo cat /var/log/nginx/access.log | alp ltsv -m '^/user/[^/]+/home$,^/user/[^/]+/reward$,^/user/[^/]+/card$,^/user/[^/]+/item$,^/user/[^/]+/card/addexp/[^/]+$,^/user/[^/]+/present/index/[^/]+$,^/user/[^/]+/present/receive$,^/user/[^/]+/gacha/index$,^/user/[^/]+/gacha/draw/[^/]+/10$,^/admin/master$,^/admin/user/[^/]+/ban$,^/admin/user/[^/]+$,^/login$,^/user$' --sort sum -r