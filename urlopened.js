var settings = require(__dirname + '/etc/settings.json')
var de = require('devent').createDEvent('sender');
var queue = require('queuer');
var mysql = require("mysql");
var tool = require('./lib/tool').tool;
var mcli = mysql.createClient(settings.mysql.weibo);

var rq = queue.getQueue('http://'+settings.queue.host+':'+settings.queue.port+'/queue', settings.queue.repost);

var run = function(ev){
    if(!ev || !ev.meta || !ev.meta.match(/^\d+$/)){
        return;
    }
    var microBlogId = ev.meta;

    getMicroBlog(microBlogId, function(err, result){
        if(err || result.length > 0){
            return;
        }
        insertTask(microBlogId, 'a_stock', function(err, result){
            if(err){
                if(err.number != 1062){
                    console.log(err);
                }
            }else{
                var task = "mysql://172.16.39.117:3306/weibo?repost_task#" + result.insertId;
                rq.enqueue(task);
            }
        });
    });
    
}
de.on("open-url", run);

var getMicroBlog = function(id, callback){
    var sql = "SELECT * FROM article_subject WHERE micro_blog_id = ? AND stock_code = 'a_stock'";
    mcli.query(sql, [id], callback);
}

var insertTask = function(microBlogId, stockCode, callback){
    var sql = "INSERT INTO repost_task(micro_blog_id, stock_code, in_time) VALUES(?, ?, ?)";
    mcli.query(sql, [microBlogId, stockCode, tool.timestamp()], callback);
}
console.log("urlopen listen start at " + tool.getDateString());
//run({meta:'1150376'});