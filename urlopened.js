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
        if(err || result.length == 0 || result[0].stock_code == 'a_stock'
             || result[0].in_time < (Date.now() / 1000) - 10800) {
            console.log(microBlogId + " time out");
            return;
        }

        if(result[0].content_type 
            && (result[0].content_type == 'bulllist' || result[0].content_type == 'bulletin')){

            console.log(microBlogId + " is bulletin");
            return;
        }

        if(result[0].content && result[0].content.match(/【.*(公告|:|：|【).*】/)){
            console.log(microBlogId + "'s title is invalid");
            return;
        }

        var title = '';
        var m = result[0].content.match(/^【(.+?)】/);
        if(m){
            title = m[1];
        }    
        
        insertTask(microBlogId, 'a_stock', title, function(err, result){
            if(err){
                if(err.number != 1062){
                    console.log(err);
                }
            }else{
                var task = "mysql://172.16.39.117:3306/weibo?repost_task#" + result.insertId;
                console.log(task);
                rq.enqueue(task);
            }
        });
    });
    
}
de.on("open-url", run);

var getMicroBlog = function(id, callback){
    var sql = "SELECT * FROM article_subject WHERE micro_blog_id = ? AND send_type = 'post'";
    mcli.query(sql, [id], function(err, subject){
        if(err || subject.length == 0){
            callback(err, subject);
            return;
        }

        var sql = "SELECT * FROM micro_blog WHERE id = ?";
        mcli.query(sql, [id], function(err1, blog){
            if(!err1 && blog.length > 0){
                subject[0].content_type = blog[0].content_type;
                subject[0].content = blog[0].content;
            }
            callback(err, subject);
        });
    });
}

var insertTask = function(microBlogId, stockCode, title, callback){
    var sql = "INSERT INTO repost_task(micro_blog_id, stock_code, in_time, content) VALUES(?, ?, ?, ?)";
    mcli.query(sql, [microBlogId, stockCode, tool.timestamp(), title], callback);
}
console.log("urlopen listen start at " + tool.getDateString());
//run({meta:'1486788'});
