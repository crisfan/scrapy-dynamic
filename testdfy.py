import pickle
import redis
con = redis.Redis()
# a = con.zrange('zzh:2:moe:queue', 0, -1)
# con.zremrangebyrank('',0,-1)
# print a

# con.zadd('score','kk',1,'ll',9,'sadd',4,'ssss',3)

con.zremrangebyrank('score',0,-1)
# print pickle.loads("(dp1\nVbody\np2\nV{\\u000a    \"url\": \"http://www.moe.edu.cn/jyb_xxgk/moe_1777/moe_1779/201612/t20161226_293049.html\"\\u000a}\np3\nsV_encoding\np4\nVutf-8\np5\nsVcookies\np6\n(dp7\nsVjob_id\np8\nV1\nsVheaders\np9\n(dp10\nVContent-Type\np11\n(lp12\nVapplication/json\np13\nassVurl\np14\nVhttp://0.0.0.0:8050/render.html\np15\nsVdont_filter\np16\nI00\nsVexpires\np17\nI0\nsS'ts'\np18\nF1517053581.9506719\nsVmaxdepth\np19\nI0\nsVpriority\np20\nI100\nsVcallback\np21\nVparse_inform_detail\np22\nsVmeta\np23\n(dp24\nVajax_crawlable\np25\nI01\nsVsecond_page_css\np26\n(dp27\nVbody\np28\nVtrue\np29\nsVtitle\np30\nVh1\np31\nssVdownload_timeout\np32\nF10\nsVitem\np33\n(dp34\nVsecond_page_url\np35\nVhttp://www.moe.edu.cn/jyb_xxgk/moe_1777/moe_1779/201612/t20161226_293049.html\np36\nsVtime\np37\nV2016-12-07\np38\nsVtitle\np39\nV\\u98df\\u54c1\\u836f\\u54c1\\u76d1\\u7ba1\\u603b\\u5c40 \\u6559\\u80b2\\u90e8\\u5173\\u4e8e\\u8fdb\\u4e00\\u6b65\\u52a0\\u5f3a\\u4e2d\\u5c0f\\u5b66\\u6821\\u548c\\u5e7c\\u513f\\u56ed\\u98df\\u54c1\\u5b89\\u5168\\u76d1\\u7763\\u7ba1\\u7406\\u5de5\\u4f5c\\u7684\\u901a\\u77e5\np40\nssV_splash_processed\np41\nI01\nsVsplash\np42\n(dp43\nVendpoint\np44\nVrender.html\np45\nsVhttp_status_from_error_code\np46\nI01\nsVslot_policy\np47\nVper_domain\np48\nsVargs\np49\n(dp50\nVurl\np51\nVhttp://www.moe.edu.cn/jyb_xxgk/moe_1777/moe_1779/201612/t20161226_293049.html\np52\nssVdont_send_headers\np53\nI01\nsVmagic_response\np54\nI01\nssVdownload_slot\np55\nVwww.moe.edu.cn\np56\nssVspider_type\np57\nVzzh\np58\nsV_class\np59\nVscrapy_splash.request.SplashRequest\np60\nsVmethod\np61\nVPOST\np62\nsVerrback\np63\nVparse_inform_index\np64\ns.")
