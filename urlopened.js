var settings = require(__dirname + '/etc/settings.json')
var de = require('devent').createDEvent('sender');
var queue = require('queuer');
var mysql = require("mysql");
var tool = require('./lib/tool').tool;
var db = require('./lib/db').db;
var mcli = mysql.createClient(settings.mysql.weibo);
var redis = require('redis');
var rcli = redis.createClient(settings.redis.port, settings.redis.host);
var async = require("async");
db.init(settings);

var rq = queue.getQueue('http://'+settings.queue.host+':'+settings.queue.port+'/queue', settings.queue.repost);

var weiboAccounts;
db.loadAccounts (function (err, accounts) {
    weiboAccounts = accounts;
});

var run = function(ev){
    if(!ev || !ev.meta || !ev.meta.match(/^\d+$/)){
        return;
    }
    var microBlogId = ev.meta;
    var start = function(callback){
        callback(null, microBlogId);
    }
    var funcs = [start, clickCounter, getMicroBlog, getSentRecord, setRepostTask];    
    async.waterfall(funcs, function(err, result){
        console.log([err, result]);
    });
}

var getSentRecord = function (blog, callback) {
    var sql = "SELECT * FROM sent_micro_blog WHERE micro_blog_id = ?";
    sql = mcli.format(sql, [blog.micro_blog_id]);
    mcli.query(sql, function (err, result) {
        if (err || result.length == 0) {
            callback({msg:'not found the sent record'});
        } else {
            blog.sentRecord = result;
            callback(null, blog);
        }
    });
}

var setRepostTask = function(result, callback){
    var microBlogId = result.micro_blog_id;
    if(result.stock_code == 'a_stock'
         || result.in_time < (Date.now() / 1000) - 21600) {
        callback({msg:microBlogId + ':timeout'});
        return;
    }
    if(result.content_type 
        && (result.content_type == 'bulllist' || result.content_type == 'bulletin')){
        callback({msg:microBlogId + " is bulletin"});
        return;
    }

    if(result.content && result.content.match(/【.*(公告|:|：|【).*】/)){
        callback({msg:microBlogId + "'s title is invalid"});
        return;
    }

    var title = '';
    var m = result.content.match(/^【(.+?)】/);
    if(m){
        title = m[1];
    }    
    
    insertTask(microBlogId, 'a_stock', title, function(err, info){
        if(err){
            if(err.number != 1062){
                callback({msg:microBlogId + ':insert repost_task error'});
            }else{
                callback({msg:microBlogId + ':repeat repost'});
            }
        }else{
            //循环发送列表，找到发送被点击的微博的账号
            //再找到A股雷达和发送账号同一平台的账号
            async.forEach(result.sentRecord, function (sent, cb) {
                var account, sentAccount = weiboAccounts.ids[sent.account_id];
                if(!sentAccount || !(account = weiboAccounts.stocks.a_stock[sentAccount.provider])) {
                    cb();
                    return;
                }
                var task = "mysql://172.16.39.117:3306/weibo?repost_task#" + info.insertId + "_" + account.id;
                rq.enqueue(task);
            }, function () {
                callback(null, task);
            });
        }
    });
}
    
de.on("open-url", run);

var clickCounter = function(microBlogId, callback){
    var redisKey = 'microblog_click_counter_' + microBlogId;
    rcli.get(redisKey, function(err, count){
        if(!count || count == 0){
            rcli.setex(redisKey, 3600, 1);
            callback({msg:microBlogId+':not clicked in 1 hour'});
        }else{
            callback(null, microBlogId);
        }
    })
}

var getMicroBlog = function(id, callback){
    var sql = "SELECT * FROM article_subject WHERE micro_blog_id = ? AND send_type = 'post'";
    mcli.query(sql, [id], function(err, subject){
        if(err || subject.length == 0){
            callback({msg : id + ':can not get the article_subject'}, subject);
            return;
        }

        var sql = "SELECT * FROM micro_blog WHERE id = ?";
        mcli.query(sql, [id], function(err1, blog){
            if(!err1 && blog.length > 0){
                subject[0].content_type = blog[0].content_type;
                subject[0].content = blog[0].content;
            }
            callback(err, subject[0]);
        });
    });
}

var insertTask = function(microBlogId, stockCode, title, callback){
    var sql = "INSERT INTO repost_task(micro_blog_id, stock_code, in_time, content) VALUES(?, ?, ?, ?)";
    mcli.query(sql, [microBlogId, stockCode, tool.timestamp(), title], callback);
}
console.log("urlopen listen start at " + tool.getDateString());
//run({meta:'145515'});
